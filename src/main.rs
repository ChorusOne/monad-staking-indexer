mod contract_abi;
mod db;
mod events;
mod metrics;

use std::ops::Range;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    pubsub::PubSubFrontend,
    rpc::types::Filter,
};
use eyre::Result;
use futures_util::stream::StreamExt;
use log::{error, info, log};
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::events::{StakingEvent, StakingEventType};

fn chunk_range(range: Range<u64>, chunk_size: u64) -> Vec<Range<u64>> {
    let mut chunks = Vec::with_capacity(((range.end - range.start) / chunk_size) as usize);
    let mut chunk_start = range.start;

    while chunk_start < range.end {
        let chunk_end = std::cmp::min(chunk_start + chunk_size, range.end);
        chunks.push(chunk_start..chunk_end);
        chunk_start = chunk_end;
    }

    chunks
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    info!("Starting Monad Staking Indexer...");

    let rpc_url = std::env::var("RPC_URL").expect("Missing RPC_URL env var");
    let database_url = std::env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");

    info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    info!("Database connected");

    let staking_contract_address: Address = "0x0000000000000000000000000000000000001000".parse()?;

    info!("Getting current indexing state...");
    let max_block_on_startup = db::repository::get_max_block_number(&pool).await?;
    info!("Max block at startup {max_block_on_startup:?}");
    let gaps = db::repository::get_block_gaps(&pool).await?;

    info!("Creating RPC connections...");
    let ws_live = WsConnect::new(&rpc_url);
    let live_provider = ProviderBuilder::new().on_ws(ws_live).await?;

    let ws_gaps = WsConnect::new(&rpc_url);
    let gaps_provider = ProviderBuilder::new().on_ws(ws_gaps).await?;
    let (gap_tx, gap_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();
    let (metrics_request_tx, metrics_request_rx) = mpsc::unbounded_channel();
    info!("Watching staking contract at: {}", staking_contract_address);

    tokio::spawn(async move {
        if let Err(e) = metrics::process_metrics(metrics_rx, metrics_request_rx).await {
            error!("Metrics processing task failed: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = metrics::run_metrics_server(metrics_request_tx).await {
            error!("Metrics server task failed: {}", e);
        }
    });

    let pool_clone = pool.clone();
    let event_metrics_tx = metrics_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = process_event_logs(pool_clone, rx, event_metrics_tx).await {
            error!("Event processing task failed: {}", e);
        }
    });

    let gaps_process_tx = tx.clone();
    let gaps_metrics_tx = metrics_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = process_gaps_task(
            gaps_provider,
            staking_contract_address,
            gaps_process_tx,
            gap_rx,
            gaps_metrics_tx,
        )
        .await
        {
            error!("Gap processing task failed: {}", e);
        }
    });

    for range in gaps {
        gap_tx.send(range).ok();
    }

    process_live_blocks(
        &live_provider,
        staking_contract_address,
        max_block_on_startup.map(|b| b as u64 + 1),
        tx,
        gap_tx,
    )
    .await
}

async fn process_gaps_task(
    provider: RootProvider<PubSubFrontend>,
    staking_contract_address: Address,
    log_tx: mpsc::UnboundedSender<StakingEvent>,
    mut gap_rx: mpsc::UnboundedReceiver<Range<u64>>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    while let Some(range) = gap_rx.recv().await {
        let chunks = chunk_range(range.clone(), 100);
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

            let res = process_historical_blocks(
                &provider,
                staking_contract_address,
                chunk_range,
                log_tx.clone(),
                metrics_tx.clone(),
            )
            .await;

            log!(
                log_level_res(&res),
                "Chunk backfill completed for blocks {} to {}: {:?}",
                chunk_range.start,
                chunk_range.end - 1,
                res
            );
        }
        info!(
            "Finished backfilling range: {range:?} ({} blocks)",
            range.end - range.start
        );
    }
    Ok(())
}

fn log_level_res<T, E>(r: &Result<T, E>) -> log::Level {
    if r.is_err() {
        log::Level::Error
    } else {
        log::Level::Debug
    }
}

async fn process_event_logs(
    pool: PgPool,
    mut rx: mpsc::UnboundedReceiver<StakingEvent>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    while let Some(event) = rx.recv().await {
        match db::repository::insert_staking_event(&pool, &event).await {
            Ok(()) => {
                let level = if event.event_type() == StakingEventType::ValidatorRewarded {
                    log::Level::Debug
                } else {
                    log::Level::Info
                };

                log!(level, "{} stored in database", event);
                let _ = metrics_tx.send(metrics::Metric::InsertedEvent(event.event_type()));
            }
            Err(db::repository::DbError::DuplicateEvent {
                event_type,
                block_meta,
                tx_meta,
            }) => {
                info!(
                    "Duplicate event: {:?} at block {} with tx {:?}",
                    event_type, block_meta.block_number, tx_meta
                );
                let _ = metrics_tx.send(metrics::Metric::DuplicateEvent(event_type));
            }
            Err(e) => {
                error!("Failed to insert event: {}", e);
            }
        }
    }
    Ok(())
}

async fn process_live_blocks(
    provider: &RootProvider<PubSubFrontend>,
    staking_contract_address: Address,
    mut start_block: Option<u64>,
    tx: mpsc::UnboundedSender<StakingEvent>,
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
                tx.send(event).expect("Channel closed");
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
    tx: mpsc::UnboundedSender<StakingEvent>,
    metrics_tx: mpsc::UnboundedSender<metrics::Metric>,
) -> Result<()> {
    let filter = Filter::new()
        .address(staking_contract_address)
        .from_block(range.start)
        .to_block(range.end.saturating_sub(1));

    let logs = provider.get_logs(&filter).await?;

    for log in logs {
        match events::extract_event(&log) {
            Ok(Some(event)) => tx.send(event).expect("Channel closed"),
            Ok(None) => (),
            Err(e) => {
                error!("Error extracting event: {}", e);
            }
        }
    }

    let blocks_processed = range.end - range.start;
    let _ = metrics_tx.send(metrics::Metric::BackfilledBlocks(blocks_processed));

    Ok(())
}
