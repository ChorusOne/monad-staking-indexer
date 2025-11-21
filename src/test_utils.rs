use std::ops::Range;

use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{DbRequest, metrics, process_db_requests};

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
    UnboundedReceiver<Range<u64>>,
    UnboundedReceiver<metrics::Metric>,
) {
    let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel();
    let (gap_tx, gap_rx) = tokio::sync::mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::unbounded_channel();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = process_db_requests(pool_clone, db_rx, gap_tx, metrics_tx, 30).await {
            eprintln!("process_db_requests failed: {}", e);
        }
    });

    (db_tx, gap_rx, metrics_rx)
}
