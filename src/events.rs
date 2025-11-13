use alloy::{primitives::Log as PrimitiveLog, rpc::types::Log, sol_types::SolEvent};
use bigdecimal::BigDecimal;
use eyre::Result;

use crate::StakingPrecompile;

#[derive(Debug, Clone)]
pub struct DelegateEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_number: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
    pub transaction_hash: String,
    pub transaction_index: BigDecimal,
    pub log_index: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct UndelegateEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_number: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
    pub transaction_hash: String,
    pub transaction_index: BigDecimal,
    pub log_index: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct WithdrawEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_number: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
    pub transaction_hash: String,
    pub transaction_index: BigDecimal,
    pub log_index: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct ClaimRewardsEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub amount: BigDecimal,
    pub epoch: BigDecimal,
    pub block_number: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
    pub transaction_hash: String,
    pub transaction_index: BigDecimal,
    pub log_index: BigDecimal,
}

#[derive(Debug, Clone)]
pub enum StakingEvent {
    Delegate(DelegateEvent),
    Undelegate(UndelegateEvent),
    Withdraw(WithdrawEvent),
    ClaimRewards(ClaimRewardsEvent),
}

pub fn extract_event(log: &Log) -> Result<Option<StakingEvent>> {
    let block_number = log
        .block_number
        .ok_or_else(|| eyre::eyre!("Missing block number"))?;
    let block_hash = log
        .block_hash
        .ok_or_else(|| eyre::eyre!("Missing block hash"))?;
    let block_timestamp = log
        .block_timestamp
        .ok_or_else(|| eyre::eyre!("Missing block timestamp"))?;
    let transaction_hash = log
        .transaction_hash
        .ok_or_else(|| eyre::eyre!("Missing transaction hash"))?;
    let transaction_index = log
        .transaction_index
        .ok_or_else(|| eyre::eyre!("Missing transaction index"))?;
    let log_index = log
        .log_index
        .ok_or_else(|| eyre::eyre!("Missing log index"))?;

    let Some(topic0) = log.topic0() else {
        return Ok(None);
    };

    let inner_log = PrimitiveLog {
        address: log.address(),
        data: log.data().clone(),
    };

    match topic0 {
        &StakingPrecompile::Delegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Delegate::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Delegate(DelegateEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator.as_slice()),
                amount: decoded.amount.to_string().parse()?,
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_number: BigDecimal::from(block_number),
                block_hash: hex::encode(block_hash.as_slice()),
                block_timestamp: BigDecimal::from(block_timestamp),
                transaction_hash: hex::encode(transaction_hash.as_slice()),
                transaction_index: BigDecimal::from(transaction_index),
                log_index: BigDecimal::from(log_index),
            })))
        }
        &StakingPrecompile::Undelegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Undelegate::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Undelegate(UndelegateEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator.as_slice()),
                withdrawal_id: decoded.withdrawal_id as i16,
                amount: decoded.amount.to_string().parse()?,
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_number: BigDecimal::from(block_number),
                block_hash: hex::encode(block_hash.as_slice()),
                block_timestamp: BigDecimal::from(block_timestamp),
                transaction_hash: hex::encode(transaction_hash.as_slice()),
                transaction_index: BigDecimal::from(transaction_index),
                log_index: BigDecimal::from(log_index),
            })))
        }
        &StakingPrecompile::Withdraw::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Withdraw::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Withdraw(WithdrawEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator.as_slice()),
                withdrawal_id: decoded.withdrawal_id as i16,
                amount: decoded.amount.to_string().parse()?,
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_number: BigDecimal::from(block_number),
                block_hash: hex::encode(block_hash.as_slice()),
                block_timestamp: BigDecimal::from(block_timestamp),
                transaction_hash: hex::encode(transaction_hash.as_slice()),
                transaction_index: BigDecimal::from(transaction_index),
                log_index: BigDecimal::from(log_index),
            })))
        }
        &StakingPrecompile::ClaimRewards::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ClaimRewards::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::ClaimRewards(ClaimRewardsEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator.as_slice()),
                amount: decoded.amount.to_string().parse()?,
                epoch: BigDecimal::from(decoded.epoch),
                block_number: BigDecimal::from(block_number),
                block_hash: hex::encode(block_hash.as_slice()),
                block_timestamp: BigDecimal::from(block_timestamp),
                transaction_hash: hex::encode(transaction_hash.as_slice()),
                transaction_index: BigDecimal::from(transaction_index),
                log_index: BigDecimal::from(log_index),
            })))
        }
        _ => Ok(None),
    }
}
