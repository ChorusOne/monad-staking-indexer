use monad_staking_indexer::{
    DbRequest, STAKING_CONTRACT_ADDRESS, chunk_range, config::Config, db, events, metrics,
    process_db_requests,
};

use std::ops::Range;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    pubsub::PubSubFrontend,
    rpc::types::Filter,
};
use eyre::Result;
use futures_util::stream::StreamExt;
use log::{error, info};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load().expect("Failed to load configuration");

    env_logger::builder()
        .filter_level(config.parse_log_level())
        .format_target(false)
        .init();

    info!("Starting Monad Staking Indexer...");

    info!("Connecting to database...");
    let pool = db::create_pool(&config.database_url).await?;
    info!("Database connected");

    info!("Getting current indexing state...");
    let max_block_on_startup = db::repository::get_max_block_number(&pool).await?;
    info!("Max block at startup {max_block_on_startup:?}");

    info!("Creating RPC connections...");
    let ws_live = WsConnect::new(&config.rpc_url);
    let live_provider = ProviderBuilder::new().on_ws(ws_live).await?;

    let ws_gaps = WsConnect::new(&config.rpc_url);
    let gaps_provider = ProviderBuilder::new().on_ws(ws_gaps).await?;
    let (gap_tx, gap_rx) = mpsc::unbounded_channel();

    let (db_tx, db_rx) = mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();
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
        )),
        tokio::spawn(periodic_gap_check(
            config.gap_check_interval_secs,
            db_tx.clone(),
        )),
        tokio::spawn(process_gaps_task(
            gaps_provider,
            STAKING_CONTRACT_ADDRESS,
            db_tx.clone(),
            gap_rx,
            metrics_tx.clone(),
            config.backfill_chunk_size,
        )),
        tokio::spawn(process_live_blocks(
            live_provider,
            STAKING_CONTRACT_ADDRESS,
            max_block_on_startup,
            db_tx,
            gap_tx,
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
    provider: RootProvider<PubSubFrontend>,
    staking_contract_address: Address,
    log_tx: mpsc::UnboundedSender<DbRequest>,
    mut gap_rx: mpsc::UnboundedReceiver<Range<u64>>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
    chunk_size: u64,
) -> Result<()> {
    while let Some(range) = gap_rx.recv().await {
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
            info!("Backfilling chunk: blocks {:?}", chunk_range);
            let blocks_processed = chunk_range.end - chunk_range.start;

            let res = process_historical_blocks(
                &provider,
                staking_contract_address,
                chunk_range,
                log_tx.clone(),
            )
            .await;
            let metric = match res {
                Ok(()) => {
                    info!("Successfully backfilled {chunk_range:?}");
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
    provider: RootProvider<PubSubFrontend>,
    staking_contract_address: Address,
    mut start_block: Option<u64>,
    tx: mpsc::UnboundedSender<DbRequest>,
    gap_tx: mpsc::UnboundedSender<Range<u64>>,
) -> Result<()> {
    let filter = Filter::new().address(staking_contract_address);

    info!("Starting live event stream from block {:?}", start_block);
    let mut stream = provider.subscribe_logs(&filter).await?.into_stream();
    info!("Got liveblock stream");

    while let Some(log) = stream.next().await {
        match events::extract_event(&log) {
            Ok(Some(event)) => {
                if let Some(start) = start_block {
                    let end = event.block_meta().block_number;
                    gap_tx.send(start..end).unwrap();
                    start_block = None;
                }
                tx.send(DbRequest::InsertStakingEvent(event))
                    .expect("Channel closed");
            }
            Ok(None) => (),
            Err(e) => {
                error!("Error extracting event: {}", e);
            }
        }
    }

    Ok(())
}

async fn process_historical_blocks(
    provider: &RootProvider<PubSubFrontend>,
    staking_contract_address: Address,
    range: &Range<u64>,
    tx: mpsc::UnboundedSender<DbRequest>,
) -> Result<()> {
    let filter = Filter::new()
        .address(staking_contract_address)
        .from_block(range.start)
        .to_block(range.end.saturating_sub(1));

    let logs = provider.get_logs(&filter).await?;

    for log in logs {
        if let Some(event) = events::extract_event(&log)? {
            tx.send(DbRequest::InsertStakingEvent(event))
                .expect("Channel closed");
        }
    }

    Ok(())
}
