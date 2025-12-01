use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{BackfillWork, DbRequest, metrics, process_db_requests};

pub fn init_test_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .is_test(true)
        .try_init();
}

pub fn spawn_process_event_logs(
    pool: &PgPool,
) -> (
    UnboundedSender<DbRequest>,
    UnboundedReceiver<BackfillWork>,
    UnboundedReceiver<metrics::Metric>,
) {
    let validator_ids = vec![1];
    let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel();
    let (backfill_tx, backfill_rx) = tokio::sync::mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::unbounded_channel();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = process_db_requests(
            pool_clone,
            db_rx,
            backfill_tx,
            validator_ids,
            metrics_tx,
            30,
        )
        .await
        {
            eprintln!("process_db_requests failed: {}", e);
        }
    });

    (db_tx, backfill_rx, metrics_rx)
}
