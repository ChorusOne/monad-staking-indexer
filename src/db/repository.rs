use std::ops::Range;

use sqlx::PgPool;
use thiserror::Error;

use crate::events::{BlockMeta, StakingEventType, TxMeta};

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Duplicate event: {event_type} at block {} tx {}", block_meta.block_number, tx_meta.transaction_hash)]
    DuplicateEvent {
        event_type: StakingEventType,
        block_meta: BlockMeta,
        tx_meta: TxMeta,
    },
}

pub async fn get_max_block_number(pool: &PgPool) -> Result<Option<u64>, DbError> {
    let row = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(block_number) FROM blocks")
        .fetch_one(pool)
        .await?;

    Ok(row.map(|b| b as u64))
}

pub async fn get_block_gaps(pool: &PgPool) -> Result<Vec<Range<u64>>, DbError> {
    let rows = sqlx::query_as::<_, (i64, i64)>(
        r#"
        WITH gaps AS (
            SELECT block_number + 1 AS gap_start,
                   LEAD(block_number) OVER (ORDER BY block_number) - 1 AS gap_end
            FROM blocks
        )
        SELECT gap_start, gap_end
        FROM gaps
        WHERE gap_end IS NOT NULL
        AND gap_end >= gap_start
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| Range {
            start: u64::try_from(r.0).unwrap(),
            end: u64::try_from(r.1).unwrap() + 1, // +1 because Range is exclusive end
        })
        .collect())
}
