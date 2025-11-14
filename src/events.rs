use alloy::{primitives::Log as PrimitiveLog, rpc::types::Log, sol_types::SolEvent};
use bigdecimal::{
    BigDecimal,
    num_bigint::{BigInt, Sign},
};
use eyre::Result;
use std::fmt;

use crate::StakingPrecompile;

fn u256_to_bigdecimal(value: alloy::primitives::U256) -> BigDecimal {
    let bytes = value.as_le_bytes();
    let bigint = BigInt::from_bytes_le(Sign::Plus, bytes.as_ref());
    BigDecimal::from(bigint)
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub block_number: BigDecimal,
    pub block_hash: String,
    pub block_timestamp: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct TxMeta {
    pub transaction_hash: String,
    pub transaction_index: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct DelegateEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for DelegateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Delegate block={} validator={}",
            self.block_meta.block_number, self.val_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct UndelegateEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for UndelegateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Undelegate block={} validator={}",
            self.block_meta.block_number, self.val_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct WithdrawEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for WithdrawEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Withdraw block={} validator={}",
            self.block_meta.block_number, self.val_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct ClaimRewardsEvent {
    pub val_id: BigDecimal,
    pub delegator: String,
    pub amount: BigDecimal,
    pub epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for ClaimRewardsEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClaimRewards block={} validator={}",
            self.block_meta.block_number, self.val_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorRewardedEvent {
    pub validator_id: BigDecimal,
    pub from: String,
    pub amount: BigDecimal,
    pub epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for ValidatorRewardedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidatorRewarded block={} validator={}",
            self.block_meta.block_number, self.validator_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct EpochChangedEvent {
    pub old_epoch: BigDecimal,
    pub new_epoch: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for EpochChangedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EpochChanged block={}", self.block_meta.block_number)
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorCreatedEvent {
    pub validator_id: BigDecimal,
    pub auth_address: String,
    pub commission: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for ValidatorCreatedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidatorCreated block={} validator={}",
            self.block_meta.block_number, self.validator_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorStatusChangedEvent {
    pub validator_id: BigDecimal,
    pub flags: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for ValidatorStatusChangedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValidatorStatusChanged block={} validator={}",
            self.block_meta.block_number, self.validator_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct CommissionChangedEvent {
    pub validator_id: BigDecimal,
    pub old_commission: BigDecimal,
    pub new_commission: BigDecimal,
    pub block_meta: BlockMeta,
    pub tx_meta: TxMeta,
}

impl fmt::Display for CommissionChangedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CommissionChanged block={} validator={}",
            self.block_meta.block_number, self.validator_id
        )
    }
}

#[derive(Debug, Clone)]
pub enum StakingEvent {
    Delegate(DelegateEvent),
    Undelegate(UndelegateEvent),
    Withdraw(WithdrawEvent),
    ClaimRewards(ClaimRewardsEvent),
    ValidatorRewarded(ValidatorRewardedEvent),
    EpochChanged(EpochChangedEvent),
    ValidatorCreated(ValidatorCreatedEvent),
    ValidatorStatusChanged(ValidatorStatusChangedEvent),
    CommissionChanged(CommissionChangedEvent),
}

impl fmt::Display for StakingEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StakingEvent::Delegate(e) => write!(f, "{}", e),
            StakingEvent::Undelegate(e) => write!(f, "{}", e),
            StakingEvent::Withdraw(e) => write!(f, "{}", e),
            StakingEvent::ClaimRewards(e) => write!(f, "{}", e),
            StakingEvent::ValidatorRewarded(e) => write!(f, "{}", e),
            StakingEvent::EpochChanged(e) => write!(f, "{}", e),
            StakingEvent::ValidatorCreated(e) => write!(f, "{}", e),
            StakingEvent::ValidatorStatusChanged(e) => write!(f, "{}", e),
            StakingEvent::CommissionChanged(e) => write!(f, "{}", e),
        }
    }
}

impl StakingEvent {
    pub fn block_meta(&self) -> &BlockMeta {
        match self {
            StakingEvent::Delegate(e) => &e.block_meta,
            StakingEvent::Undelegate(e) => &e.block_meta,
            StakingEvent::Withdraw(e) => &e.block_meta,
            StakingEvent::ClaimRewards(e) => &e.block_meta,
            StakingEvent::ValidatorRewarded(e) => &e.block_meta,
            StakingEvent::EpochChanged(e) => &e.block_meta,
            StakingEvent::ValidatorCreated(e) => &e.block_meta,
            StakingEvent::ValidatorStatusChanged(e) => &e.block_meta,
            StakingEvent::CommissionChanged(e) => &e.block_meta,
        }
    }
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

    let Some(topic0) = log.topic0() else {
        return Ok(None);
    };

    let block_meta = BlockMeta {
        block_number: BigDecimal::from(block_number),
        block_hash: hex::encode(block_hash),
        block_timestamp: BigDecimal::from(block_timestamp),
    };

    let tx_meta = TxMeta {
        transaction_hash: hex::encode(transaction_hash),
        transaction_index: BigDecimal::from(transaction_index),
    };

    let inner_log = PrimitiveLog {
        address: log.address(),
        data: log.data().clone(),
    };

    match *topic0 {
        StakingPrecompile::Delegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Delegate::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Delegate(DelegateEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator),
                amount: u256_to_bigdecimal(decoded.amount),
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_meta,
                tx_meta,
            })))
        }
        StakingPrecompile::Undelegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Undelegate::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Undelegate(UndelegateEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator),
                withdrawal_id: decoded.withdrawal_id as i16,
                amount: u256_to_bigdecimal(decoded.amount),
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_meta,
                tx_meta,
            })))
        }
        StakingPrecompile::Withdraw::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Withdraw::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::Withdraw(WithdrawEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator),
                withdrawal_id: decoded.withdrawal_id as i16,
                amount: u256_to_bigdecimal(decoded.amount),
                activation_epoch: BigDecimal::from(decoded.activationEpoch),
                block_meta,
                tx_meta,
            })))
        }
        StakingPrecompile::ClaimRewards::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ClaimRewards::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::ClaimRewards(ClaimRewardsEvent {
                val_id: BigDecimal::from(decoded.valId),
                delegator: hex::encode(decoded.delegator),
                amount: u256_to_bigdecimal(decoded.amount),
                epoch: BigDecimal::from(decoded.epoch),
                block_meta,
                tx_meta,
            })))
        }
        StakingPrecompile::ValidatorRewarded::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorRewarded::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::ValidatorRewarded(
                ValidatorRewardedEvent {
                    validator_id: BigDecimal::from(decoded.validatorId),
                    from: hex::encode(decoded.from),
                    amount: u256_to_bigdecimal(decoded.amount),
                    epoch: BigDecimal::from(decoded.epoch),
                    block_meta,
                    tx_meta,
                },
            )))
        }
        StakingPrecompile::EpochChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::EpochChanged::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::EpochChanged(EpochChangedEvent {
                old_epoch: BigDecimal::from(decoded.oldEpoch),
                new_epoch: BigDecimal::from(decoded.newEpoch),
                block_meta,
                tx_meta,
            })))
        }
        StakingPrecompile::ValidatorCreated::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorCreated::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::ValidatorCreated(
                ValidatorCreatedEvent {
                    validator_id: BigDecimal::from(decoded.validatorId),
                    auth_address: hex::encode(decoded.authAddress),
                    commission: u256_to_bigdecimal(decoded.commission),
                    block_meta,
                    tx_meta,
                },
            )))
        }
        StakingPrecompile::ValidatorStatusChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorStatusChanged::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::ValidatorStatusChanged(
                ValidatorStatusChangedEvent {
                    validator_id: BigDecimal::from(decoded.validatorId),
                    flags: BigDecimal::from(decoded.flags),
                    block_meta,
                    tx_meta,
                },
            )))
        }
        StakingPrecompile::CommissionChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::CommissionChanged::decode_log(&inner_log, true)?;
            Ok(Some(StakingEvent::CommissionChanged(
                CommissionChangedEvent {
                    validator_id: BigDecimal::from(decoded.validatorId),
                    old_commission: u256_to_bigdecimal(decoded.oldCommission),
                    new_commission: u256_to_bigdecimal(decoded.newCommission),
                    block_meta,
                    tx_meta,
                },
            )))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[test]
    fn test_u256_to_bigdecimal_small_value() {
        let u256_value = U256::from(12345u64);
        let result = u256_to_bigdecimal(u256_value);
        let expected = BigDecimal::from(12345u64);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_u256_to_bigdecimal_large_value() {
        let u256_str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let u256_value = U256::from_str(u256_str).unwrap();
        let result = u256_to_bigdecimal(u256_value);
        let expected = BigDecimal::from_str(u256_str).unwrap();
        assert_eq!(result, expected);
    }
}
