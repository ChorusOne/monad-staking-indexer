use std::ops::Range;

use eyre::Result;
use log::info;
use sqlx::PgPool;

use crate::events::{self, BlockMeta};

pub async fn ensure_block(pool: &PgPool, block_meta: &BlockMeta) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO blocks (block_number, block_hash, block_timestamp)
        VALUES ($1, $2, $3)
        ON CONFLICT (block_number) DO NOTHING
        "#,
    )
    .bind(block_meta.block_number as i64)
    .bind(&block_meta.block_hash)
    .bind(block_meta.block_timestamp as i64)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_max_block_number(pool: &PgPool) -> Result<Option<i64>> {
    let row = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(block_number) FROM blocks")
        .fetch_one(pool)
        .await?;

    Ok(row)
}

pub async fn get_block_gaps(pool: &PgPool) -> Result<Vec<Range<u64>>> {
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
            end: u64::try_from(r.1).unwrap(),
        })
        .collect())
}

pub async fn insert_delegate_event(pool: &PgPool, event: &events::DelegateEvent) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO delegate_events (
            val_id, delegator, amount, activation_epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.val_id as i64)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(event.activation_epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("Delegate event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_undelegate_event(pool: &PgPool, event: &events::UndelegateEvent) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO undelegate_events (
            val_id, delegator, withdrawal_id, amount, activation_epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.val_id as i64)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(event.activation_epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("Undelegate event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_withdraw_event(pool: &PgPool, event: &events::WithdrawEvent) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO withdraw_events (
            val_id, delegator, withdrawal_id, amount, activation_epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.val_id as i64)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(event.activation_epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("Withdraw event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_claim_rewards_event(
    pool: &PgPool,
    event: &events::ClaimRewardsEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO claim_rewards_events (
            val_id, delegator, amount, epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.val_id as i64)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(event.epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("ClaimRewards event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_validator_rewarded_event(
    pool: &PgPool,
    event: &events::ValidatorRewardedEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO validator_rewarded_events (
            validator_id, from_address, amount, epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.validator_id as i64)
    .bind(&event.from)
    .bind(&event.amount)
    .bind(event.epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("ValidatorRewarded event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_epoch_changed_event(
    pool: &PgPool,
    event: &events::EpochChangedEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO epoch_changed_events (
            old_epoch, new_epoch,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.old_epoch as i64)
    .bind(event.new_epoch as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("EpochChanged event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_validator_created_event(
    pool: &PgPool,
    event: &events::ValidatorCreatedEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO validator_created_events (
            validator_id, auth_address, commission,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.validator_id as i64)
    .bind(&event.auth_address)
    .bind(&event.commission)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("ValidatorCreated event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_validator_status_changed_event(
    pool: &PgPool,
    event: &events::ValidatorStatusChangedEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO validator_status_changed_events (
            validator_id, flags,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.validator_id as i64)
    .bind(event.flags as i64)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("ValidatorStatusChanged event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_commission_changed_event(
    pool: &PgPool,
    event: &events::CommissionChangedEvent,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO commission_changed_events (
            validator_id, old_commission, new_commission,
            block_number, transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(event.validator_id as i64)
    .bind(&event.old_commission)
    .bind(&event.new_commission)
    .bind(event.block_meta.block_number as i64)
    .bind(&event.tx_meta.transaction_hash)
    .bind(event.tx_meta.transaction_index as i64)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("CommissionChanged event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_staking_event(pool: &PgPool, event: &events::StakingEvent) -> Result<()> {
    let result = match event {
        events::StakingEvent::Delegate(e) => insert_delegate_event(pool, e).await,
        events::StakingEvent::Undelegate(e) => insert_undelegate_event(pool, e).await,
        events::StakingEvent::Withdraw(e) => insert_withdraw_event(pool, e).await,
        events::StakingEvent::ClaimRewards(e) => insert_claim_rewards_event(pool, e).await,
        events::StakingEvent::ValidatorRewarded(e) => {
            insert_validator_rewarded_event(pool, e).await
        }
        events::StakingEvent::EpochChanged(e) => insert_epoch_changed_event(pool, e).await,
        events::StakingEvent::ValidatorCreated(e) => insert_validator_created_event(pool, e).await,
        events::StakingEvent::ValidatorStatusChanged(e) => {
            insert_validator_status_changed_event(pool, e).await
        }
        events::StakingEvent::CommissionChanged(e) => {
            insert_commission_changed_event(pool, e).await
        }
    };

    result?;

    // only create the block data _after_ successfully inserting an event
    // to prevent having an empty block
    ensure_block(pool, event.block_meta()).await?;

    Ok(())
}
