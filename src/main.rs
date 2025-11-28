use env_logger::TimestampPrecision;
use monad_staking_indexer::provider::ReconnectProvider;
use monad_staking_indexer::config::Config;
use monad_staking_indexer::{db, metrics};
use monad_staking_indexer::tasks::{
    periodic_backfill_check_task, process_backfill_task, process_db_requests_task,
    process_live_blocks_task,
};

use eyre::Result;
use log::{error, info};
use tokio::sync::mpsc;

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
        tokio::spawn(process_db_requests_task(
            pool.clone(),
            db_rx,
            config.fetch_tx_meta_for_validators.clone(),
            metrics_tx.clone(),
            config.db_operation_timeout_secs,
        )),
        tokio::spawn(periodic_backfill_check_task(
            config.backfill_interval_secs,
            db_tx.clone(),
            backfill_tx.clone(),
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
        tokio::spawn(process_live_blocks_task(
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
