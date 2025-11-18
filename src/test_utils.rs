use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{events, metrics, process_event_logs};

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
    UnboundedSender<events::StakingEvent>,
    UnboundedReceiver<metrics::Metric>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::unbounded_channel();

    let pool_clone = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = process_event_logs(pool_clone, rx, metrics_tx).await {
            eprintln!("process_event_logs failed: {}", e);
        }
    });

    (tx, metrics_rx)
}
