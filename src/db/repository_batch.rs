use sqlx::PgPool;
use tokio::time::Duration;

use crate::db::repository::DbError;
use crate::events::{self, BlockMeta, StakingEventType};

async fn insert_delegate_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::DelegateEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO delegate_events (val_id, delegator, amount, activation_epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.val_id as i64)
            .push_bind(&event.delegator)
            .push_bind(&event.amount)
            .push_bind(event.activation_epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (val_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_undelegate_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::UndelegateEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO undelegate_events (val_id, delegator, withdrawal_id, amount, activation_epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.val_id as i64)
            .push_bind(&event.delegator)
            .push_bind(event.withdrawal_id)
            .push_bind(&event.amount)
            .push_bind(event.activation_epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (val_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_withdraw_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::WithdrawEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO withdraw_events (val_id, delegator, withdrawal_id, amount, activation_epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.val_id as i64)
            .push_bind(&event.delegator)
            .push_bind(event.withdrawal_id)
            .push_bind(&event.amount)
            .push_bind(event.activation_epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (val_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_claim_rewards_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::ClaimRewardsEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO claim_rewards_events (val_id, delegator, amount, epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.val_id as i64)
            .push_bind(&event.delegator)
            .push_bind(&event.amount)
            .push_bind(event.epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (val_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_validator_rewarded_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::ValidatorRewardedEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO validator_rewarded_events (validator_id, from_address, amount, epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.validator_id as i64)
            .push_bind(&event.from)
            .push_bind(&event.amount)
            .push_bind(event.epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_epoch_changed_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::EpochChangedEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO epoch_changed_events (old_epoch, new_epoch, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.old_epoch as i64)
            .push_bind(event.new_epoch as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_validator_created_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::ValidatorCreatedEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO validator_created_events (validator_id, auth_address, commission, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.validator_id as i64)
            .push_bind(&event.auth_address)
            .push_bind(&event.commission)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_validator_status_changed_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::ValidatorStatusChangedEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO validator_status_changed_events (validator_id, flags, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.validator_id as i64)
            .push_bind(event.flags as i64)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (validator_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_commission_changed_events_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[events::CommissionChangedEvent],
) -> Result<(u64, u64), DbError> {
    let total = events.len() as u64;
    if events.is_empty() {
        return Ok((0, 0));
    }

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO commission_changed_events (validator_id, old_commission, new_commission, block_number, transaction_hash, transaction_index) ",
    );

    query_builder.push_values(events, |mut b, event| {
        b.push_bind(event.validator_id as i64)
            .push_bind(&event.old_commission)
            .push_bind(&event.new_commission)
            .push_bind(event.block_meta.block_number as i64)
            .push_bind(&event.tx_meta.transaction_hash)
            .push_bind(event.tx_meta.transaction_index as i64);
    });

    query_builder.push(" ON CONFLICT (validator_id, transaction_hash) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok((res.rows_affected(), total))
}

async fn insert_blocks_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blocks: &[BlockMeta],
) -> Result<u64, DbError> {
    if blocks.is_empty() {
        return Ok(0);
    }

    let mut query_builder =
        sqlx::QueryBuilder::new("INSERT INTO blocks (block_number, block_hash, block_timestamp) ");

    query_builder.push_values(blocks, |mut b, block_meta| {
        b.push_bind(block_meta.block_number as i64)
            .push_bind(&block_meta.block_hash)
            .push_bind(block_meta.block_timestamp as i64);
    });

    query_builder.push(" ON CONFLICT (block_number) DO NOTHING");

    let res = query_builder.build().execute(&mut **tx).await?;

    Ok(res.rows_affected())
}

async fn insert_many_blocks_inner(
    pool: &PgPool,
    batch: &crate::BlockBatch,
) -> Result<std::collections::HashMap<StakingEventType, (u64, u64)>, DbError> {
    if batch.block_meta.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let mut tx = pool.begin().await?;

    let mut result = std::collections::HashMap::new();
    result.insert(
        StakingEventType::Delegate,
        insert_delegate_events_in_tx(&mut tx, batch.delegate.as_slice()).await?,
    );
    result.insert(
        StakingEventType::Undelegate,
        insert_undelegate_events_in_tx(&mut tx, batch.undelegate.as_slice()).await?,
    );
    result.insert(
        StakingEventType::Withdraw,
        insert_withdraw_events_in_tx(&mut tx, batch.withdraw.as_slice()).await?,
    );
    result.insert(
        StakingEventType::ClaimRewards,
        insert_claim_rewards_events_in_tx(&mut tx, batch.claim_rewards.as_slice()).await?,
    );
    result.insert(
        StakingEventType::ValidatorRewarded,
        insert_validator_rewarded_events_in_tx(&mut tx, batch.validator_rewarded.as_slice())
            .await?,
    );
    result.insert(
        StakingEventType::EpochChanged,
        insert_epoch_changed_events_in_tx(&mut tx, batch.epoch_changed.as_slice()).await?,
    );
    result.insert(
        StakingEventType::ValidatorCreated,
        insert_validator_created_events_in_tx(&mut tx, batch.validator_created.as_slice()).await?,
    );
    result.insert(
        StakingEventType::ValidatorStatusChanged,
        insert_validator_status_changed_events_in_tx(
            &mut tx,
            batch.validator_status_changed.as_slice(),
        )
        .await?,
    );
    result.insert(
        StakingEventType::CommissionChanged,
        insert_commission_changed_events_in_tx(&mut tx, batch.commission_changed.as_slice())
            .await?,
    );

    insert_blocks_in_tx(&mut tx, batch.block_meta.as_slice()).await?;

    tx.commit().await?;

    Ok(result)
}

pub async fn insert_blocks(
    pool: &PgPool,
    batch: &crate::BlockBatch,
    timeout: Duration,
) -> Result<std::collections::HashMap<StakingEventType, (u64, u64)>, DbError> {
    tokio::time::timeout(timeout, insert_many_blocks_inner(pool, batch))
        .await
        .map_err(|_| DbError::Sqlx(sqlx::Error::PoolTimedOut))?
}
