use crate::provider::ConnectedProvider;
use crate::{BackfillWork, BlockBatch, DbRequest, chunk_range, events, metrics, transaction};
use eyre::Result;
use log::{debug, error, info};
use std::collections::HashMap;
use std::ops::Range;
use tokio::sync::mpsc;

pub fn extract_backfill_events(
    event: &events::Event,
    backfill_tx: &mpsc::UnboundedSender<BackfillWork>,
    validator_filter: &std::collections::HashSet<u64>,
) {
    if let events::Event::System(system_event) = event
        && let events::SystemEvent::ValidatorRewarded(vre) = system_event
        && validator_filter.contains(&vre.validator_id)
    {
        let req = crate::BlockTipsFetchRequest {
            block_number: event.block_meta().block_number,
            validator_id: vre.validator_id,
        };
        let _ = backfill_tx.send(BackfillWork::BlockTipsFetch(req));
    }

    if let events::Event::Staking(staking_event) = event
        && validator_filter.contains(&staking_event.val_id())
    {
        let req = crate::TransactionFetchRequest {
            transaction_hash: staking_event.tx_hash().to_string(),
            block_number: event.block_meta().block_number,
            event_type: staking_event.event_type(),
        };
        let _ = backfill_tx.send(BackfillWork::TransactionFetch(req));
    }
}

pub fn process_historical_logs(
    mut logs: Vec<alloy::rpc::types::Log>,
    tx: mpsc::UnboundedSender<DbRequest>,
    backfill_tx: &mpsc::UnboundedSender<BackfillWork>,
    validator_filter: &std::collections::HashSet<u64>,
) -> Result<()> {
    logs.sort_by_key(|l| (l.block_number, l.transaction_index, l.log_index));

    let mut blocks_map: HashMap<u64, (events::BlockMeta, Vec<events::Event>)> = HashMap::new();

    for log in logs {
        if let Some(event) = events::extract_event(&log)? {
            let block_num = event.block_meta().block_number;
            blocks_map
                .entry(block_num)
                .or_insert_with(|| (event.block_meta().clone(), Vec::new()))
                .1
                .push(event.clone());

            extract_backfill_events(&event, backfill_tx, validator_filter);
        }
    }

    let mut block_metas_and_events: Vec<(u64, events::BlockMeta, Vec<events::Event>)> = blocks_map
        .into_iter()
        .map(|(num, (meta, events))| (num, meta, events))
        .collect();
    block_metas_and_events.sort_by_key(|(num, _, _)| *num);

    let mut batch = BlockBatch::new();
    for (_, meta, events) in block_metas_and_events {
        batch.add_block_meta(meta);
        for event in events {
            batch.add_event(event);
        }
    }

    if !batch.block_meta.is_empty() {
        tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch)))
            .expect("Channel closed");
    }

    Ok(())
}

pub async fn process_block_gap(
    client: &ConnectedProvider,
    range: Range<u64>,
    log_tx: &mpsc::UnboundedSender<DbRequest>,
    tx_fetch_tx: &mpsc::UnboundedSender<BackfillWork>,
    validator_filter: &std::collections::HashSet<u64>,
    chunk_size: u64,
    metrics_tx: &mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let chunks = chunk_range(range.clone(), chunk_size);
    if chunks.len() > 1 {
        info!(
            "Backfilling large range: {:?} ({} blocks) in {} chunks",
            range,
            range.end - range.start,
            chunks.len()
        );
    }

    for chunk_range in chunks.iter() {
        debug!("Backfilling chunk: blocks {:?}", chunk_range);
        let blocks_processed = chunk_range.end - chunk_range.start;

        let res = client.historical_logs(chunk_range).await.and_then(|logs| {
            if logs.is_empty() {
                Err(eyre::eyre!(
                    "RPC returned no logs for non-empty historical block range {chunk_range:?}"
                ))
            } else {
                process_historical_logs(logs, log_tx.clone(), tx_fetch_tx, validator_filter)
            }
        });

        let metric = match &res {
            Ok(()) => {
                debug!("Successfully backfilled {chunk_range:?}");
                metrics::Metric::BackfilledBlocks(blocks_processed)
            }
            Err(e) => {
                error!("Failed to backfill {chunk_range:?}: {e:?}");
                let _ = metrics_tx.send(metrics::Metric::FailedToBackfill(blocks_processed));
                return Err(eyre::eyre!(
                    "Failed to backfill {chunk_range:?} in range {range:?}: {e:?}"
                ));
            }
        };
        let _ = metrics_tx.send(metric);
    }
    info!(
        "Finished backfilling range: {range:?} ({} blocks)",
        range.end - range.start
    );

    Ok(())
}

pub async fn process_transaction_fetch(
    client: &ConnectedProvider,
    request: &crate::TransactionFetchRequest,
    db_tx: &mpsc::UnboundedSender<DbRequest>,
    metrics_tx: &mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    match client.get_transaction(&request.transaction_hash).await {
        Ok(Some(tx)) => {
            let access_list = transaction::extract_access_list(&tx);
            let tx_data = transaction::EventTxData {
                transaction_hash: request.transaction_hash.clone(),
                block_number: request.block_number,
                event_type: request.event_type,
                access_list,
            };
            let _ = db_tx.send(DbRequest::InsertTransactions(vec![tx_data]));
            Ok(())
        }
        Ok(None) => {
            error!("Transaction not found: {}", request.transaction_hash);
            let _ = metrics_tx.send(metrics::Metric::TransactionFetchFailed(1));
            Err(eyre::eyre!(
                "Transaction not found: {}",
                request.transaction_hash
            ))
        }
        Err(e) => {
            error!(
                "Failed to fetch transaction {}: {:?}",
                request.transaction_hash, e
            );
            let _ = metrics_tx.send(metrics::Metric::TransactionFetchFailed(1));
            Err(e)
        }
    }
}

pub async fn process_block_tips_fetch(
    client: &ConnectedProvider,
    request: &crate::BlockTipsFetchRequest,
    db_tx: &mpsc::UnboundedSender<DbRequest>,
    metrics_tx: &mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let block = match client.get_full_block(request.block_number).await {
        Ok(block) => block,
        Err(e) => {
            let _ = metrics_tx.send(metrics::Metric::BlockTipsFetchFailed(1));
            return Err(e);
        }
    };
    let total_priority_fees = match transaction::calculate_block_tips(&block) {
        Ok(tips) => tips,
        Err(e) => {
            error!(
                "Failed to calculate tips for block {}: {}",
                request.block_number, e
            );
            let _ = metrics_tx.send(metrics::Metric::BlockTipsFetchFailed(1));
            return Err(e);
        }
    };

    info!(
        "Block {}: priority fees = {:.4} MON",
        request.block_number,
        total_priority_fees
            .to_string()
            .parse::<f64>()
            .expect("2**256 (<1e78) is < 1e308")
            / 1.0e18
    );

    let _ = db_tx.send(DbRequest::InsertBlockTip((
        request.block_number,
        request.validator_id,
        total_priority_fees,
    )));

    let _ = metrics_tx.send(metrics::Metric::BackfilledBlockTips(1));
    Ok(())
}

pub async fn process_delegator_snapshot_fetch(
    client: &ConnectedProvider,
    request: &crate::DelegatorSnapshotFetchRequest,
    db_tx: &mpsc::UnboundedSender<DbRequest>,
    metrics_tx: &mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    info!(
        "Fetching delegator snapshot for validator {} at epoch {} (block {})",
        request.validator_id, request.epoch, request.block_number
    );

    let snapshots = match client
        .get_delegator_snapshot(request.block_number, request.validator_id)
        .await
    {
        Ok(snapshots) => snapshots,
        Err(e) => {
            error!(
                "Failed to fetch delegator snapshot for validator {} at epoch {}: {}",
                request.validator_id, request.epoch, e
            );
            let _ = metrics_tx.send(metrics::Metric::DelegatorSnapshotFetchFailed(1));
            return Err(e);
        }
    };

    info!(
        "Fetched {} delegators for validator {} at epoch {}",
        snapshots.len(),
        request.validator_id,
        request.epoch
    );

    let _ = db_tx.send(DbRequest::InsertDelegatorSnapshots {
        validator_id: request.validator_id,
        epoch: request.epoch,
        block_number: request.block_number,
        snapshots,
    });

    let _ = metrics_tx.send(metrics::Metric::BackfilledDelegatorSnapshots(1));
    Ok(())
}
