use eyre::Result;
use log::info;
use sqlx::PgPool;

use crate::events;

pub async fn insert_delegate_event(pool: &PgPool, event: &events::DelegateEvent) -> Result<()> {
    let result = sqlx::query(
        r#"
        INSERT INTO delegate_events (
            val_id, delegator, amount, activation_epoch,
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index, log_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_number)
    .bind(&event.block_hash)
    .bind(&event.block_timestamp)
    .bind(&event.transaction_hash)
    .bind(&event.transaction_index)
    .bind(&event.log_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index, log_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_number)
    .bind(&event.block_hash)
    .bind(&event.block_timestamp)
    .bind(&event.transaction_hash)
    .bind(&event.transaction_index)
    .bind(&event.log_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index, log_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_number)
    .bind(&event.block_hash)
    .bind(&event.block_timestamp)
    .bind(&event.transaction_hash)
    .bind(&event.transaction_index)
    .bind(&event.log_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index, log_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(&event.epoch)
    .bind(&event.block_number)
    .bind(&event.block_hash)
    .bind(&event.block_timestamp)
    .bind(&event.transaction_hash)
    .bind(&event.transaction_index)
    .bind(&event.log_index)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("ClaimRewards event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_staking_event(pool: &PgPool, event: &events::StakingEvent) -> Result<()> {
    match event {
        events::StakingEvent::Delegate(e) => insert_delegate_event(pool, e).await,
        events::StakingEvent::Undelegate(e) => insert_undelegate_event(pool, e).await,
        events::StakingEvent::Withdraw(e) => insert_withdraw_event(pool, e).await,
        events::StakingEvent::ClaimRewards(e) => insert_claim_rewards_event(pool, e).await,
    }
}
