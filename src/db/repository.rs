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
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(event.withdrawal_id)
    .bind(&event.amount)
    .bind(&event.activation_epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.val_id)
    .bind(&event.delegator)
    .bind(&event.amount)
    .bind(&event.epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.validator_id)
    .bind(&event.from)
    .bind(&event.amount)
    .bind(&event.epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.old_epoch)
    .bind(&event.new_epoch)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.validator_id)
    .bind(&event.auth_address)
    .bind(&event.commission)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.validator_id)
    .bind(&event.flags)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
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
            block_number, block_hash, block_timestamp,
            transaction_hash, transaction_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_hash) DO NOTHING
        "#,
    )
    .bind(&event.validator_id)
    .bind(&event.old_commission)
    .bind(&event.new_commission)
    .bind(&event.block_meta.block_number)
    .bind(&event.block_meta.block_hash)
    .bind(&event.block_meta.block_timestamp)
    .bind(&event.tx_meta.transaction_hash)
    .bind(&event.tx_meta.transaction_index)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        info!("CommissionChanged event already exists in database (duplicate)");
    }

    Ok(())
}

pub async fn insert_staking_event(pool: &PgPool, event: &events::StakingEvent) -> Result<()> {
    match event {
        events::StakingEvent::Delegate(e) => insert_delegate_event(pool, e).await,
        events::StakingEvent::Undelegate(e) => insert_undelegate_event(pool, e).await,
        events::StakingEvent::Withdraw(e) => insert_withdraw_event(pool, e).await,
        events::StakingEvent::ClaimRewards(e) => insert_claim_rewards_event(pool, e).await,
        events::StakingEvent::ValidatorRewarded(e) => insert_validator_rewarded_event(pool, e).await,
        events::StakingEvent::EpochChanged(e) => insert_epoch_changed_event(pool, e).await,
        events::StakingEvent::ValidatorCreated(e) => insert_validator_created_event(pool, e).await,
        events::StakingEvent::ValidatorStatusChanged(e) => insert_validator_status_changed_event(pool, e).await,
        events::StakingEvent::CommissionChanged(e) => insert_commission_changed_event(pool, e).await,
    }
}
