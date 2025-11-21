use env_logger::TimestampPrecision;
use monad_staking_indexer::provider::ReconnectProvider;
use monad_staking_indexer::{
    BlockBatch, DbRequest, chunk_range, config::Config, db, events, metrics, process_db_requests,
};

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

    info!("Starting Monad Staking Indexer...");

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

    let gaps_reconnect_provider =
        ReconnectProvider::new(config.rpc_urls.clone(), config.watchdog_timeout_secs);

    let (gap_tx, gap_rx) = mpsc::unbounded_channel();

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
            gap_tx.clone(),
            metrics_tx.clone(),
            config.db_operation_timeout_secs,
        )),
        tokio::spawn(periodic_gap_check(
            config.gap_check_interval_secs,
            db_tx.clone(),
        )),
        tokio::spawn(process_gaps_task(
            gaps_reconnect_provider,
            db_tx.clone(),
            gap_rx,
            config.backfill_chunk_size,
            metrics_tx.clone(),
        )),
        tokio::spawn(process_live_blocks(
            live_reconnect_provider,
            max_block_on_startup,
            db_tx,
            gap_tx,
            config.db_batch_size,
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

async fn periodic_gap_check(
    interval_secs: u64,
    gap_tx: mpsc::UnboundedSender<DbRequest>,
) -> Result<()> {
    let mut interval = interval(Duration::from_secs(interval_secs));
    interval.tick().await;
    loop {
        info!("Running periodic gap check...");
        let _ = gap_tx.send(DbRequest::GetBlockGaps);
        interval.tick().await;
    }
}

async fn process_gaps_task(
    reconnect_provider: ReconnectProvider,
    log_tx: mpsc::UnboundedSender<DbRequest>,
    mut gap_rx: mpsc::UnboundedReceiver<Range<u64>>,
    chunk_size: u64,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let mut attempts = 0usize;

    while let Some(range) = gap_rx.recv().await {
        let client = loop {
            match reconnect_provider.connect(attempts).await {
                Ok(client) => break client,
                Err(e) => {
                    attempts += 1;
                    error!("Gaps task connection failed: {e:?}");
                    metrics_tx.send(e).unwrap();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

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

            let res = client
                .historical_logs(chunk_range)
                .await
                .and_then(|logs| process_historical_logs(logs, log_tx.clone()));

            let metric = match res {
                Ok(()) => {
                    debug!("Successfully backfilled {chunk_range:?}");
                    metrics::Metric::BackfilledBlocks(blocks_processed)
                }
                Err(e) => {
                    error!("Failed to backfill {chunk_range:?}: {e:?}");
                    metrics::Metric::FailedToBackfill(blocks_processed)
                }
            };
            let _ = metrics_tx.send(metric);
        }
        info!(
            "Finished backfilling range: {range:?} ({} blocks)",
            range.end - range.start
        );
    }
    Ok(())
}

async fn process_live_blocks(
    reconnect_provider: ReconnectProvider,
    mut start_block: Option<u64>,
    tx: mpsc::UnboundedSender<DbRequest>,
    gap_tx: mpsc::UnboundedSender<Range<u64>>,
    batch_size: usize,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let mut current_block_buffer: Vec<events::StakingEvent> = Vec::new();
    let mut current_block_meta: Option<events::BlockMeta> = None;
    let mut batch = BlockBatch::new();
    let mut block_count = 0;
    let mut attempts = 0usize;

    info!("Starting live event stream from block {:?}", start_block);

    loop {
        let client = loop {
            match reconnect_provider.connect(attempts).await {
                Ok(c) => break c,
                Err(e) => {
                    error!("Live blocks connection failed: {e:?}");
                    attempts += 1;
                    metrics_tx.send(e).unwrap();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

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
                            gap_tx.send(start..event_block_num).unwrap();
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

                    current_block_meta = Some(event.block_meta().clone());
                    current_block_buffer.push(event);

                    if block_count >= batch_size {
                        tx.send(DbRequest::InsertCompleteBlocks(Box::new(std::mem::take(
                            &mut batch,
                        ))))
                        .expect("Channel closed");
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
) -> Result<()> {
    logs.sort_by_key(|l| (l.block_number, l.transaction_index, l.log_index));

    let mut blocks_map: std::collections::HashMap<
        u64,
        (events::BlockMeta, Vec<events::StakingEvent>),
    > = std::collections::HashMap::new();

    for log in logs {
        if let Some(event) = events::extract_event(&log)? {
            let block_num = event.block_meta().block_number;
            blocks_map
                .entry(block_num)
                .or_insert_with(|| (event.block_meta().clone(), Vec::new()))
                .1
                .push(event);
        }
    }

    let mut block_metas_and_events: Vec<(u64, events::BlockMeta, Vec<events::StakingEvent>)> =
        blocks_map
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
