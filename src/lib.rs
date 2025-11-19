pub mod config;
pub mod contract_abi;
pub mod db;
pub mod error;
pub mod events;
pub mod metrics;
pub mod pg_utils;

pub mod test_utils;

use alloy::primitives::Address;

pub const STAKING_CONTRACT_ADDRESS: Address =
    alloy::primitives::address!("0000000000000000000000000000000000001000");

use std::ops::Range;

use eyre::Result;
use log::{error, info, log};
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::events::{StakingEvent, StakingEventType};

pub fn chunk_range(range: Range<u64>, chunk_size: u64) -> Vec<Range<u64>> {
    let mut chunks = Vec::with_capacity(((range.end - range.start) / chunk_size) as usize);
    let mut chunk_start = range.start;

    while chunk_start < range.end {
        let chunk_end = std::cmp::min(chunk_start + chunk_size, range.end);
        chunks.push(chunk_start..chunk_end);
        chunk_start = chunk_end;
    }

    chunks
}

pub async fn process_event_logs(
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
                error!("Failed to insert event {event:?}: {e:?}");
                let _ = metrics_tx.send(metrics::Metric::FailedToInsert);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_range_even_division() {
        let chunks = chunk_range(0..100, 10);
        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0], 0..10);
        assert_eq!(chunks[9], 90..100);
    }

    #[test]
    fn test_chunk_range_uneven_division() {
        let chunks = chunk_range(0..105, 10);
        assert_eq!(chunks.len(), 11);
        assert_eq!(chunks[0], 0..10);
        assert_eq!(chunks[9], 90..100);
        assert_eq!(chunks[10], 100..105);
    }

    #[test]
    fn test_chunk_range_smaller_than_chunk() {
        let chunks = chunk_range(0..5, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], 0..5);
    }

    #[test]
    fn test_chunk_range_single_block() {
        let chunks = chunk_range(5..6, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], 5..6);
    }

    #[test]
    fn test_chunk_range_empty() {
        let chunks = chunk_range(5..5, 100);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_chunk_range_contiguous() {
        let chunks = chunk_range(0..100, 30);
        assert_eq!(chunks.len(), 4);

        // contiguous
        for i in 0..chunks.len() - 1 {
            assert_eq!(chunks[i].end, chunks[i + 1].start);
        }

        // full range
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[chunks.len() - 1].end, 100);
    }
}
