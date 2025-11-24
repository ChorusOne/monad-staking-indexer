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

pub async fn get_missing_transaction_hashes(
    pool: &PgPool,
    validator_ids: &[u64],
) -> Result<Vec<(String, u64, crate::events::StakingEventType)>, DbError> {
    if validator_ids.is_empty() {
        return Ok(Vec::new());
    }

    let validator_ids_i64: Vec<i64> = validator_ids.iter().map(|&id| id as i64).collect();

    let rows = sqlx::query_as::<_, (String, i64, String)>(
        r#"
        SELECT DISTINCT e.transaction_hash, e.block_number, e.event_type
        FROM (
            SELECT transaction_hash, block_number, 'delegate' as event_type
            FROM delegate_events
            WHERE val_id = ANY($1)

            UNION ALL

            SELECT transaction_hash, block_number, 'undelegate' as event_type
            FROM undelegate_events
            WHERE val_id = ANY($1)

            UNION ALL

            SELECT transaction_hash, block_number, 'withdraw' as event_type
            FROM withdraw_events
            WHERE val_id = ANY($1)

            UNION ALL

            SELECT transaction_hash, block_number, 'claim_rewards' as event_type
            FROM claim_rewards_events
            WHERE val_id = ANY($1)
        ) e
        LEFT JOIN events_tx_data tx ON e.transaction_hash = tx.transaction_hash
        WHERE tx.transaction_hash IS NULL
        "#,
    )
    .bind(&validator_ids_i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(hash, block_num, event_type_str)| {
            let event_type = match event_type_str.as_str() {
                "delegate" => crate::events::StakingEventType::Delegate,
                "undelegate" => crate::events::StakingEventType::Undelegate,
                "withdraw" => crate::events::StakingEventType::Withdraw,
                "claim_rewards" => crate::events::StakingEventType::ClaimRewards,
                _ => crate::events::StakingEventType::Delegate,
            };
            (hash, block_num as u64, event_type)
        })
        .collect())
}
