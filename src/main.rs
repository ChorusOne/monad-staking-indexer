mod contract_abi;
mod db;
mod events;

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
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::events::StakingEvent;

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
    info!("Watching staking contract at: {}", staking_contract_address);

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = process_event_logs(pool_clone, rx).await {
            error!("Event processing task failed: {}", e);
        }
    });

    let gaps_process_tx = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = process_gaps_task(
            gaps_provider,
            staking_contract_address,
            gaps_process_tx,
            gap_rx,
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
) -> Result<()> {
    while let Some(range) = gap_rx.recv().await {
        info!("Processing gap: blocks {} to {}", range.start, range.end);
        if let Err(e) =
            process_historical_blocks(&provider, staking_contract_address, &range, log_tx.clone())
                .await
        {
            error!(
                "Gap backfill failed for blocks {} to {}: {}",
                range.start, range.end, e
            );
        } else {
            info!(
                "Gap backfill completed for blocks {} to {}",
                range.start, range.end
            );
        }
    }
    Ok(())
}

async fn process_event_logs(
    pool: PgPool,
    mut rx: mpsc::UnboundedReceiver<StakingEvent>,
) -> Result<()> {
    while let Some(event) = rx.recv().await {
        if let Err(e) = db::repository::insert_staking_event(&pool, &event).await {
            error!("Failed to insert event: {}", e);
        } else {
            info!("{} stored in database", event);
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

    while let Some(log) = stream.next().await {
        match events::extract_event(&log) {
            Ok(Some(event)) => {
                if let Some(start) = start_block {
                    let end = event.block_meta().block_number;
                    gap_tx.send(start..end - 1).unwrap();
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
) -> Result<()> {
    let filter = Filter::new()
        .address(staking_contract_address)
        .from_block(range.start)
        .to_block(range.end);

    info!("Processing block range: {range:?}");
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

    info!("Completed processing range {range:?}");
    Ok(())
}
