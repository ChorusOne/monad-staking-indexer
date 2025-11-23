use alloy::{primitives::Log as PrimitiveLog, rpc::types::Log, sol_types::SolEvent};
use bigdecimal::{
    BigDecimal,
    num_bigint::{BigInt, Sign},
};
use eyre::Result;
use std::fmt;

use crate::contract_abi::StakingPrecompile;

fn u256_to_bigdecimal(value: alloy::primitives::U256) -> BigDecimal {
    let bytes = value.as_le_bytes();
    let bigint = BigInt::from_bytes_le(Sign::Plus, bytes.as_ref());
    BigDecimal::from(bigint)
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct TxMeta {
    pub transaction_hash: String,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct DelegateEvent {
    pub val_id: u64,
    pub delegator: String,
    pub amount: BigDecimal,
    pub activation_epoch: u64,
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
    pub val_id: u64,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: u64,
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
    pub val_id: u64,
    pub delegator: String,
    pub withdrawal_id: i16,
    pub amount: BigDecimal,
    pub activation_epoch: u64,
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
    pub val_id: u64,
    pub delegator: String,
    pub amount: BigDecimal,
    pub epoch: u64,
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
    pub validator_id: u64,
    pub from: String,
    pub amount: BigDecimal,
    pub epoch: u64,
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
    pub old_epoch: u64,
    pub new_epoch: u64,
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
    pub validator_id: u64,
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
    pub validator_id: u64,
    pub flags: u64,
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
    pub validator_id: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(type_name = "staking_event_type", rename_all = "snake_case")]
pub enum StakingEventType {
    Delegate,
    Undelegate,
    Withdraw,
    ClaimRewards,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemEventType {
    ValidatorRewarded,
    EpochChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidatorEventType {
    Created,
    StatusChanged,
    CommissionChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Staking(StakingEventType),
    System(SystemEventType),
    Validator(ValidatorEventType),
}

#[derive(Debug, Clone)]
pub enum StakingEvent {
    Delegate(DelegateEvent),
    Undelegate(UndelegateEvent),
    Withdraw(WithdrawEvent),
    ClaimRewards(ClaimRewardsEvent),
}

#[derive(Debug, Clone)]
pub enum SystemEvent {
    ValidatorRewarded(ValidatorRewardedEvent),
    EpochChanged(EpochChangedEvent),
}

#[derive(Debug, Clone)]
pub enum ValidatorEvent {
    Created(ValidatorCreatedEvent),
    StatusChanged(ValidatorStatusChangedEvent),
    CommissionChanged(CommissionChangedEvent),
}

#[derive(Debug, Clone)]
pub enum Event {
    Staking(StakingEvent),
    System(SystemEvent),
    Validator(ValidatorEvent),
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Staking(e) => write!(f, "{}", e),
            Event::System(e) => write!(f, "{}", e),
            Event::Validator(e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for StakingEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StakingEvent::Delegate(e) => write!(f, "{}", e),
            StakingEvent::Undelegate(e) => write!(f, "{}", e),
            StakingEvent::Withdraw(e) => write!(f, "{}", e),
            StakingEvent::ClaimRewards(e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for SystemEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemEvent::ValidatorRewarded(e) => write!(f, "{}", e),
            SystemEvent::EpochChanged(e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for ValidatorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidatorEvent::Created(e) => write!(f, "{}", e),
            ValidatorEvent::StatusChanged(e) => write!(f, "{}", e),
            ValidatorEvent::CommissionChanged(e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventType::Staking(t) => write!(f, "{}", t),
            EventType::System(t) => write!(f, "{}", t),
            EventType::Validator(t) => write!(f, "{}", t),
        }
    }
}

impl fmt::Display for StakingEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StakingEventType::Delegate => write!(f, "Delegate"),
            StakingEventType::Undelegate => write!(f, "Undelegate"),
            StakingEventType::Withdraw => write!(f, "Withdraw"),
            StakingEventType::ClaimRewards => write!(f, "ClaimRewards"),
        }
    }
}

impl fmt::Display for SystemEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemEventType::ValidatorRewarded => write!(f, "ValidatorRewarded"),
            SystemEventType::EpochChanged => write!(f, "EpochChanged"),
        }
    }
}

impl fmt::Display for ValidatorEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidatorEventType::Created => write!(f, "ValidatorCreated"),
            ValidatorEventType::StatusChanged => write!(f, "ValidatorStatusChanged"),
            ValidatorEventType::CommissionChanged => write!(f, "CommissionChanged"),
        }
    }
}

impl EventType {
    pub fn all_types() -> Vec<EventType> {
        vec![
            EventType::Staking(StakingEventType::Delegate),
            EventType::Staking(StakingEventType::Undelegate),
            EventType::Staking(StakingEventType::Withdraw),
            EventType::Staking(StakingEventType::ClaimRewards),
            EventType::System(SystemEventType::ValidatorRewarded),
            EventType::System(SystemEventType::EpochChanged),
            EventType::Validator(ValidatorEventType::Created),
            EventType::Validator(ValidatorEventType::StatusChanged),
            EventType::Validator(ValidatorEventType::CommissionChanged),
        ]
    }
}

impl Event {
    pub fn event_type(&self) -> EventType {
        match self {
            Event::Staking(e) => EventType::Staking(e.event_type()),
            Event::System(e) => EventType::System(e.event_type()),
            Event::Validator(e) => EventType::Validator(e.event_type()),
        }
    }

    pub fn block_meta(&self) -> &BlockMeta {
        match self {
            Event::Staking(e) => e.block_meta(),
            Event::System(e) => e.block_meta(),
            Event::Validator(e) => e.block_meta(),
        }
    }
}

impl StakingEvent {
    pub fn event_type(&self) -> StakingEventType {
        match self {
            StakingEvent::Delegate(_) => StakingEventType::Delegate,
            StakingEvent::Undelegate(_) => StakingEventType::Undelegate,
            StakingEvent::Withdraw(_) => StakingEventType::Withdraw,
            StakingEvent::ClaimRewards(_) => StakingEventType::ClaimRewards,
        }
    }

    pub fn block_meta(&self) -> &BlockMeta {
        match self {
            StakingEvent::Delegate(e) => &e.block_meta,
            StakingEvent::Undelegate(e) => &e.block_meta,
            StakingEvent::Withdraw(e) => &e.block_meta,
            StakingEvent::ClaimRewards(e) => &e.block_meta,
        }
    }

    pub fn val_id(&self) -> u64 {
        match self {
            StakingEvent::Delegate(e) => e.val_id,
            StakingEvent::Undelegate(e) => e.val_id,
            StakingEvent::Withdraw(e) => e.val_id,
            StakingEvent::ClaimRewards(e) => e.val_id,
        }
    }

    pub fn tx_hash(&self) -> &str {
        match self {
            StakingEvent::Delegate(e) => &e.tx_meta.transaction_hash,
            StakingEvent::Undelegate(e) => &e.tx_meta.transaction_hash,
            StakingEvent::Withdraw(e) => &e.tx_meta.transaction_hash,
            StakingEvent::ClaimRewards(e) => &e.tx_meta.transaction_hash,
        }
    }
}

impl SystemEvent {
    pub fn event_type(&self) -> SystemEventType {
        match self {
            SystemEvent::ValidatorRewarded(_) => SystemEventType::ValidatorRewarded,
            SystemEvent::EpochChanged(_) => SystemEventType::EpochChanged,
        }
    }

    pub fn block_meta(&self) -> &BlockMeta {
        match self {
            SystemEvent::ValidatorRewarded(e) => &e.block_meta,
            SystemEvent::EpochChanged(e) => &e.block_meta,
        }
    }
}

impl ValidatorEvent {
    pub fn event_type(&self) -> ValidatorEventType {
        match self {
            ValidatorEvent::Created(_) => ValidatorEventType::Created,
            ValidatorEvent::StatusChanged(_) => ValidatorEventType::StatusChanged,
            ValidatorEvent::CommissionChanged(_) => ValidatorEventType::CommissionChanged,
        }
    }

    pub fn block_meta(&self) -> &BlockMeta {
        match self {
            ValidatorEvent::Created(e) => &e.block_meta,
            ValidatorEvent::StatusChanged(e) => &e.block_meta,
            ValidatorEvent::CommissionChanged(e) => &e.block_meta,
        }
    }
}

pub fn extract_event(log: &Log) -> Result<Option<Event>> {
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
        block_number,
        block_hash: hex::encode(block_hash),
        block_timestamp,
    };

    let tx_meta = TxMeta {
        transaction_hash: hex::encode(transaction_hash),
        transaction_index,
    };

    let inner_log = PrimitiveLog {
        address: log.address(),
        data: log.data().clone(),
    };

    match *topic0 {
        StakingPrecompile::Delegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Delegate::decode_log(&inner_log, true)?;
            Ok(Some(Event::Staking(StakingEvent::Delegate(
                DelegateEvent {
                    val_id: decoded.valId,
                    delegator: hex::encode(decoded.delegator),
                    amount: u256_to_bigdecimal(decoded.amount),
                    activation_epoch: decoded.activationEpoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::Undelegate::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Undelegate::decode_log(&inner_log, true)?;
            Ok(Some(Event::Staking(StakingEvent::Undelegate(
                UndelegateEvent {
                    val_id: decoded.valId,
                    delegator: hex::encode(decoded.delegator),
                    withdrawal_id: decoded.withdrawal_id as i16,
                    amount: u256_to_bigdecimal(decoded.amount),
                    activation_epoch: decoded.activationEpoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::Withdraw::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::Withdraw::decode_log(&inner_log, true)?;
            Ok(Some(Event::Staking(StakingEvent::Withdraw(
                WithdrawEvent {
                    val_id: decoded.valId,
                    delegator: hex::encode(decoded.delegator),
                    withdrawal_id: decoded.withdrawal_id as i16,
                    amount: u256_to_bigdecimal(decoded.amount),
                    activation_epoch: decoded.activationEpoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::ClaimRewards::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ClaimRewards::decode_log(&inner_log, true)?;
            Ok(Some(Event::Staking(StakingEvent::ClaimRewards(
                ClaimRewardsEvent {
                    val_id: decoded.valId,
                    delegator: hex::encode(decoded.delegator),
                    amount: u256_to_bigdecimal(decoded.amount),
                    epoch: decoded.epoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::ValidatorRewarded::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorRewarded::decode_log(&inner_log, true)?;
            Ok(Some(Event::System(SystemEvent::ValidatorRewarded(
                ValidatorRewardedEvent {
                    validator_id: decoded.validatorId,
                    from: hex::encode(decoded.from),
                    amount: u256_to_bigdecimal(decoded.amount),
                    epoch: decoded.epoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::EpochChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::EpochChanged::decode_log(&inner_log, true)?;
            Ok(Some(Event::System(SystemEvent::EpochChanged(
                EpochChangedEvent {
                    old_epoch: decoded.oldEpoch,
                    new_epoch: decoded.newEpoch,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::ValidatorCreated::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorCreated::decode_log(&inner_log, true)?;
            Ok(Some(Event::Validator(ValidatorEvent::Created(
                ValidatorCreatedEvent {
                    validator_id: decoded.validatorId,
                    auth_address: hex::encode(decoded.authAddress),
                    commission: u256_to_bigdecimal(decoded.commission),
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::ValidatorStatusChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::ValidatorStatusChanged::decode_log(&inner_log, true)?;
            Ok(Some(Event::Validator(ValidatorEvent::StatusChanged(
                ValidatorStatusChangedEvent {
                    validator_id: decoded.validatorId,
                    flags: decoded.flags,
                    block_meta,
                    tx_meta,
                },
            ))))
        }
        StakingPrecompile::CommissionChanged::SIGNATURE_HASH => {
            let decoded = StakingPrecompile::CommissionChanged::decode_log(&inner_log, true)?;
            Ok(Some(Event::Validator(ValidatorEvent::CommissionChanged(
                CommissionChangedEvent {
                    validator_id: decoded.validatorId,
                    old_commission: u256_to_bigdecimal(decoded.oldCommission),
                    new_commission: u256_to_bigdecimal(decoded.newCommission),
                    block_meta,
                    tx_meta,
                },
            ))))
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
