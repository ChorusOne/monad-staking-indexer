use crate::provider::ReconnectProvider;
use crate::processing::{process_block_gap, process_transaction_fetch, process_block_tips_fetch, extract_backfill_events};
use crate::{BackfillWork, BlockBatch, DbRequest, db, events, metrics, BlockGapsResponse, TransactionFetchRequest, BlockTipsFetchRequest};
use crate::events::u256_to_bigdecimal;
use eyre::Result;
use log::{error, info};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use futures_util::stream::StreamExt;

pub async fn periodic_backfill_check_task(
    interval_secs: u64,
    db_tx: mpsc::UnboundedSender<DbRequest>,
    backfill_tx: mpsc::UnboundedSender<BackfillWork>,
) -> Result<()> {
    let mut interval = interval(Duration::from_secs(interval_secs));
    interval.tick().await;
    loop {
        info!("Running periodic backfill check...");

        let (block_gaps_tx, block_gaps_rx) = tokio::sync::oneshot::channel();
        let (transaction_gaps_tx, transaction_gaps_rx) = tokio::sync::oneshot::channel();
        let (block_tips_gaps_tx, block_tips_gaps_rx) = tokio::sync::oneshot::channel();

        let _ = db_tx.send(DbRequest::GetBlockGaps(block_gaps_tx));
        let _ = db_tx.send(DbRequest::GetTransactionGaps(transaction_gaps_tx));
        let _ = db_tx.send(DbRequest::GetBlockTipsGaps(block_tips_gaps_tx));

        if let Ok(Some(response)) = block_gaps_rx.await {
            for range in response.gaps {
                info!("Queueing block gap for backfill: {:?}", range);
                let _ = backfill_tx.send(BackfillWork::BlockGap(range));
            }
        }

        if let Ok(requests) = transaction_gaps_rx.await {
            for request in requests {
                let _ = backfill_tx.send(BackfillWork::TransactionFetch(request));
            }
        }

        if let Ok(requests) = block_tips_gaps_rx.await {
            for request in requests {
                let _ = backfill_tx.send(BackfillWork::BlockTipsFetch(request));
            }
        }

        interval.tick().await;
    }
}

pub async fn process_backfill_task(
    mut reconnect_provider: ReconnectProvider,
    mut backfill_rx: mpsc::UnboundedReceiver<BackfillWork>,
    backfill_tx: mpsc::UnboundedSender<BackfillWork>,
    db_tx: mpsc::UnboundedSender<DbRequest>,
    chunk_size: u64,
    validator_filter: std::collections::HashSet<u64>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let mut client = reconnect_provider.connect(&metrics_tx).await;

    while let Some(work) = backfill_rx.recv().await {
        let result = match &work {
            BackfillWork::BlockGap(range) => {
                process_block_gap(
                    &client,
                    range.clone(),
                    &db_tx,
                    &backfill_tx,
                    &validator_filter,
                    chunk_size,
                    &metrics_tx,
                )
                .await
            }
            BackfillWork::TransactionFetch(request) => {
                process_transaction_fetch(&client, request, &db_tx, &metrics_tx).await
            }
            BackfillWork::BlockTipsFetch(request) => {
                process_block_tips_fetch(&client, request, &db_tx, &metrics_tx).await
            }
        };

        match result {
            Ok(()) => continue,
            Err(e) => {
                error!("Backfill work failed: {e:?}, reconnecting...");
                client = reconnect_provider.connect(&metrics_tx).await;
            }
        }
    }

    Ok(())
}

pub async fn process_live_blocks_task(
    mut reconnect_provider: ReconnectProvider,
    mut start_block: Option<u64>,
    tx: mpsc::UnboundedSender<DbRequest>,
    backfill_tx: mpsc::UnboundedSender<BackfillWork>,
    batch_size: usize,
    validator_filter: std::collections::HashSet<u64>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let mut current_block_buffer: Vec<events::Event> = Vec::new();
    let mut current_block_meta: Option<events::BlockMeta> = None;
    let mut batch = BlockBatch::new();
    let mut block_count = 0;

    info!("Starting live event stream from block {:?}", start_block);

    loop {
        let client = reconnect_provider.connect(&metrics_tx).await;

        let event_stream = match client.stream_events().await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to start event stream: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        tokio::pin!(event_stream);

        info!("Connected to event stream");

        while let Some(log) = event_stream.next().await {
            match events::extract_event(&log) {
                Ok(Some(event)) => {
                    let event_block_num = event.block_meta().block_number;

                    if let Some(start) = start_block {
                        if event_block_num > start {
                            backfill_tx
                                .send(BackfillWork::BlockGap(start..event_block_num))
                                .unwrap();
                        }
                        start_block = None;
                    }

                    if let Some(ref meta) = current_block_meta
                        && meta.block_number != event_block_num
                    {
                        batch.add_block_meta(meta.clone());
                        for evt in current_block_buffer.drain(..) {
                            batch.add_event(evt);
                        }
                        block_count += 1;
                    }
                    extract_backfill_events(&event, &backfill_tx, &validator_filter);

                    current_block_meta = Some(event.block_meta().clone());
                    current_block_buffer.push(event);

                    if block_count >= batch_size {
                        let _ = tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch)));
                        batch = BlockBatch::new();
                        block_count = 0;
                    }
                }
                Ok(None) => (),
                Err(e) => {
                    error!("Error extracting event: {}", e);
                }
            }
        }

        error!("Event stream closed (timeout or error), reconnecting...");
        let _ = metrics_tx.send(metrics::Metric::RpcTimeout);
    }
}

pub async fn process_db_requests_task(
    pool: PgPool,
    mut rx: mpsc::UnboundedReceiver<DbRequest>,
    validator_ids: Vec<u64>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
    db_operation_timeout_secs: u64,
) -> Result<()> {
    let timeout = tokio::time::Duration::from_secs(db_operation_timeout_secs);
    while let Some(req) = rx.recv().await {
        match req {
            DbRequest::GetBlockGaps(response_tx) => {
                let checkpoint = match db::repository::get_block_sync_checkpoint(&pool).await {
                    Ok(cp) => cp,
                    Err(e) => {
                        error!("Failed to get block sync checkpoint: {}", e);
                        let _ = response_tx.send(None);
                        continue;
                    }
                };

                let max_block = match db::repository::get_max_block_number(&pool).await {
                    Ok(Some(max)) => max,
                    Ok(None) => {
                        info!("No blocks in database yet");
                        let _ = response_tx.send(None);
                        continue;
                    }
                    Err(e) => {
                        error!("Failed to get max block number: {}", e);
                        let _ = response_tx.send(None);
                        continue;
                    }
                };

                match db::repository::get_block_gaps(&pool, checkpoint).await {
                    Ok(gaps) => {
                        if gaps.is_empty() {
                            info!("No block gaps detected");
                            if max_block > checkpoint {
                                match db::repository::update_block_sync_checkpoint(&pool, max_block)
                                    .await
                                {
                                    Ok(_) => {
                                        info!("Updated block sync checkpoint to {}", max_block)
                                    }
                                    Err(e) => {
                                        error!("Failed to update block sync checkpoint: {}", e)
                                    }
                                }
                            }
                        } else {
                            info!("Detected {} block gap(s)", gaps.len());
                        }
                        let _ = response_tx.send(Some(BlockGapsResponse {
                            gaps,
                            checkpoint,
                            max_block,
                        }));
                    }
                    Err(e) => {
                        error!("Failed to check for gaps: {}", e);
                        let _ = response_tx.send(None);
                    }
                };
            }
            DbRequest::GetTransactionGaps(response_tx) => {
                match db::repository::get_missing_transaction_hashes(&pool, &validator_ids).await {
                    Ok(missing_hashes) => {
                        if missing_hashes.is_empty() {
                            info!("No transaction gaps detected");
                        } else {
                            info!("Detected {} missing transactions", missing_hashes.len());
                            let _ = metrics_tx.send(metrics::Metric::TransactionGapsFound(
                                missing_hashes.len() as u64,
                            ));
                        }
                        let requests: Vec<TransactionFetchRequest> = missing_hashes
                            .into_iter()
                            .map(|(transaction_hash, block_number, event_type)| {
                                TransactionFetchRequest {
                                    transaction_hash,
                                    block_number,
                                    event_type,
                                }
                            })
                            .collect();
                        let _ = response_tx.send(requests);
                    }
                    Err(e) => {
                        error!("Failed to check for transaction gaps: {}", e);
                        let _ = response_tx.send(Vec::new());
                    }
                }
            }
            DbRequest::GetBlockTipsGaps(response_tx) => {
                match db::repository::get_missing_block_tips(&pool, &validator_ids).await {
                    Ok(missing_blocks) => {
                        if missing_blocks.is_empty() {
                            info!("No block tips gaps detected");
                        } else {
                            info!("Detected {} missing block tips", missing_blocks.len());
                        }
                        let requests: Vec<BlockTipsFetchRequest> = missing_blocks
                            .into_iter()
                            .map(|block_number| BlockTipsFetchRequest { block_number })
                            .collect();
                        let _ = response_tx.send(requests);
                    }
                    Err(e) => {
                        error!("Failed to check for block tips gaps: {}", e);
                        let _ = response_tx.send(Vec::new());
                    }
                }
            }
            DbRequest::InsertCompleteBlocks(blocks) => {
                info!("Inserting {} blocks", blocks.block_meta.len(),);

                match db::insert_blocks(&pool, &blocks, timeout).await {
                    Ok(event_counts) => {
                        let total_inserted: u64 =
                            event_counts.values().map(|(inserted, _)| inserted).sum();
                        info!("Successfully inserted {} events", total_inserted);
                        let _ = metrics_tx.send(metrics::Metric::InsertedEvents(event_counts));
                    }
                    Err(db::repository::DbError::Sqlx(sqlx::Error::PoolTimedOut)) => {
                        error!("Insert operation timed out");
                        let _ = metrics_tx.send(metrics::Metric::InsertTimeout);
                    }
                    Err(e) => {
                        error!("Failed to insert blocks: {:?}", e);
                        let _ = metrics_tx.send(metrics::Metric::FailedToInsert);
                    }
                }
            }
            DbRequest::InsertTransactions(tx_data) => {
                info!("Inserting {} transactions", tx_data.len());
                match db::insert_transactions(&pool, &tx_data).await {
                    Ok(inserted) => {
                        info!("Successfully inserted {} transactions", inserted);
                        let _ = metrics_tx.send(metrics::Metric::TransactionsFetched(inserted));
                    }
                    Err(e) => {
                        error!("Failed to insert transactions: {:?}", e);
                    }
                }
            }
            DbRequest::InsertBlockTip((block_height, tip)) => {
                info!("Inserting block tip at block {block_height}");
                match db::set_block_tip(&pool, block_height, u256_to_bigdecimal(tip)).await {
                    Ok(()) => {
                        info!("Successfully inserted block tip");
                    }
                    Err(e) => {
                        error!("Failed to insert block tip: {:?}", e);
                    }
                }
            }
        }
    }
    Ok(())
}
