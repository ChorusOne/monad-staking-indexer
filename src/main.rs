use env_logger::TimestampPrecision;
use monad_staking_indexer::provider::ReconnectProvider;
use monad_staking_indexer::{
    BackfillWork, BlockBatch, DbRequest, chunk_range, config::Config, db, events, metrics,
    process_db_requests, transaction,
};

use std::collections::HashMap;
use std::ops::Range;

use eyre::Result;
use futures_util::stream::StreamExt;
use log::{debug, error, info};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load().expect("Failed to load configuration");

    env_logger::builder()
        .filter_level(config.parse_log_level())
        .format_timestamp(Some(TimestampPrecision::Millis))
        .format_target(false)
        .init();

    info!("Config is {config:#?}");

    info!("Connecting to database...");
    let database_url = config
        .connection_string()
        .await
        .expect("Failed to build database connection string");
    let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();
    let pool = db::create_pool(&database_url, metrics_tx.clone()).await?;
    info!("Database connected");

    info!("Getting current indexing state...");
    let max_block_on_startup = db::repository::get_max_block_number(&pool).await?;
    info!("Max block at startup {max_block_on_startup:?}");

    info!("Creating ReconnectProviders...");
    let live_reconnect_provider =
        ReconnectProvider::new(config.rpc_urls.clone(), config.watchdog_timeout_secs);

    let backfill_reconnect_provider =
        ReconnectProvider::new(config.rpc_urls.clone(), config.watchdog_timeout_secs);

    let validator_filter: std::collections::HashSet<u64> = config
        .fetch_tx_meta_for_validators
        .iter()
        .copied()
        .collect();

    let (backfill_tx, backfill_rx) = mpsc::unbounded_channel();

    let (db_tx, db_rx) = mpsc::unbounded_channel();
    let (metrics_request_tx, metrics_request_rx) = mpsc::unbounded_channel();

    let tasks = vec![
        tokio::spawn(metrics::process_metrics(metrics_rx, metrics_request_rx)),
        tokio::spawn(metrics::run_metrics_server(
            metrics_request_tx,
            config.metrics_bind_addr().clone(),
        )),
        tokio::spawn(process_db_requests(
            pool.clone(),
            db_rx,
            backfill_tx.clone(),
            config.fetch_tx_meta_for_validators.clone(),
            metrics_tx.clone(),
            config.db_operation_timeout_secs,
        )),
        tokio::spawn(periodic_backfill_check(
            config.backfill_interval_secs,
            db_tx.clone(),
        )),
        tokio::spawn(process_backfill_task(
            backfill_reconnect_provider,
            backfill_rx,
            backfill_tx.clone(),
            db_tx.clone(),
            config.backfill_chunk_size,
            validator_filter.clone(),
            metrics_tx.clone(),
        )),
        tokio::spawn(process_live_blocks(
            live_reconnect_provider,
            max_block_on_startup,
            db_tx,
            backfill_tx,
            config.db_batch_size,
            validator_filter,
            metrics_tx.clone(),
        )),
    ];

    for task in tasks {
        if let Err(e) = task.await {
            error!("Task panicked: {:?}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn periodic_backfill_check(
    interval_secs: u64,
    db_tx: mpsc::UnboundedSender<DbRequest>,
) -> Result<()> {
    let mut interval = interval(Duration::from_secs(interval_secs));
    interval.tick().await;
    loop {
        info!("Running periodic backfill check...");
        let _ = db_tx.send(DbRequest::GetBlockGaps);
        let _ = db_tx.send(DbRequest::GetTransactionGaps);
        interval.tick().await;
    }
}

async fn process_block_gap(
    client: &monad_staking_indexer::provider::ConnectedProvider,
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

    let mut had_error = false;

    for chunk_range in chunks.iter() {
        debug!("Backfilling chunk: blocks {:?}", chunk_range);
        let blocks_processed = chunk_range.end - chunk_range.start;

        let res = client.historical_logs(chunk_range).await.and_then(|logs| {
            process_historical_logs(logs, log_tx.clone(), tx_fetch_tx, validator_filter)
        });

        let metric = match &res {
            Ok(()) => {
                debug!("Successfully backfilled {chunk_range:?}");
                metrics::Metric::BackfilledBlocks(blocks_processed)
            }
            Err(e) => {
                error!("Failed to backfill {chunk_range:?}: {e:?}");
                had_error = true;
                metrics::Metric::FailedToBackfill(blocks_processed)
            }
        };
        let _ = metrics_tx.send(metric);
    }
    info!(
        "Finished backfilling range: {range:?} ({} blocks)",
        range.end - range.start
    );

    if had_error {
        Err(eyre::eyre!("One or more chunks failed in range {range:?}"))
    } else {
        Ok(())
    }
}

async fn process_transaction_fetch(
    client: &monad_staking_indexer::provider::ConnectedProvider,
    request: &monad_staking_indexer::TransactionFetchRequest,
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

async fn process_backfill_task(
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
            BackfillWork::NoBlockGaps(_) => {
                continue;
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

async fn process_live_blocks(
    mut reconnect_provider: ReconnectProvider,
    mut start_block: Option<u64>,
    tx: mpsc::UnboundedSender<DbRequest>,
    backfill_tx: mpsc::UnboundedSender<monad_staking_indexer::BackfillWork>,
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
                                .send(monad_staking_indexer::BackfillWork::BlockGap(
                                    start..event_block_num,
                                ))
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

                    if let events::Event::Staking(ref staking_event) = event
                        && validator_filter.contains(&staking_event.val_id())
                    {
                        let req = monad_staking_indexer::TransactionFetchRequest {
                            transaction_hash: staking_event.tx_hash().to_string(),
                            block_number: event_block_num,
                            event_type: staking_event.event_type(),
                        };
                        let _ = backfill_tx.send(BackfillWork::TransactionFetch(req));
                    }

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

fn process_historical_logs(
    mut logs: Vec<alloy::rpc::types::Log>,
    tx: mpsc::UnboundedSender<DbRequest>,
    backfill_tx: &mpsc::UnboundedSender<monad_staking_indexer::BackfillWork>,
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

            if let events::Event::Staking(ref staking_event) = event
                && validator_filter.contains(&staking_event.val_id())
            {
                let req = monad_staking_indexer::TransactionFetchRequest {
                    transaction_hash: staking_event.tx_hash().to_string(),
                    block_number: event.block_meta().block_number,
                    event_type: staking_event.event_type(),
                };
                let _ = backfill_tx.send(BackfillWork::TransactionFetch(req));
            }
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
