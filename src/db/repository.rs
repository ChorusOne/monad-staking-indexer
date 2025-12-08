use std::ops::Range;

use bigdecimal::BigDecimal;
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

pub async fn get_block_sync_checkpoint(pool: &PgPool) -> Result<u64, DbError> {
    let row = sqlx::query_scalar::<_, i64>(
        "SELECT last_verified_block FROM block_sync_checkpoint WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;

    Ok(row as u64)
}

pub async fn update_block_sync_checkpoint(pool: &PgPool, block_number: u64) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE block_sync_checkpoint SET last_verified_block = $1, updated_at = CURRENT_TIMESTAMP WHERE id = 1"
    )
    .bind(block_number as i64)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn set_block_tip(
    pool: &PgPool,
    block_number: u64,
    validator_id: u64,
    tips: BigDecimal,
) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO block_tips (block_number, val_id, tips) VALUES ($1, $2, $3) ON CONFLICT (block_number) DO UPDATE SET tips = $3, val_id = $2"
    )
    .bind(block_number as i64)
    .bind(validator_id as i64)
    .bind(tips)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_block_gaps(pool: &PgPool, from_block: u64) -> Result<Vec<Range<u64>>, DbError> {
    let rows = sqlx::query_as::<_, (i64, i64)>(
        r#"
        WITH gaps AS (
            SELECT block_number + 1 AS gap_start,
                   LEAD(block_number) OVER (ORDER BY block_number) - 1 AS gap_end
            FROM blocks
            WHERE block_number >= $1
        )
        SELECT gap_start, gap_end
        FROM gaps
        WHERE gap_end IS NOT NULL
        AND gap_end >= gap_start
        "#,
    )
    .bind(from_block as i64)
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

pub async fn get_missing_block_tips(
    pool: &PgPool,
    validator_ids: &[u64],
) -> Result<Vec<(u64, u64)>, DbError> {
    if validator_ids.is_empty() {
        return Ok(Vec::new());
    }

    let validator_ids_i64: Vec<i64> = validator_ids.iter().map(|&id| id as i64).collect();

    let rows = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT vre.block_number, vre.validator_id
        FROM validator_rewarded_events vre
        LEFT JOIN block_tips bt ON vre.block_number = bt.block_number
        WHERE vre.validator_id = ANY($1)
        AND vre.epoch > 746
        AND bt.block_number IS NULL
        ORDER BY vre.block_number
        "#,
    )
    .bind(&validator_ids_i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(block_number, validator_id)| (block_number as u64, validator_id as u64))
        .collect())
}

pub async fn get_missing_delegator_snapshots(
    pool: &PgPool,
    validator_id: u64,
) -> Result<Vec<(u64, u64)>, DbError> {
    let rows = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT ece.new_epoch, ece.block_number
        FROM epoch_changed_events ece
        LEFT JOIN delegator_snapshots ds
            ON ds.epoch = ece.new_epoch
            AND ds.block_number = ece.block_number
            AND ds.validator_id = $1
        WHERE ds.epoch IS NULL
        AND ece.new_epoch > 746
        ORDER BY ece.new_epoch
        "#,
    )
    .bind(validator_id as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(epoch, block_number)| (epoch as u64, block_number as u64))
        .collect())
}
