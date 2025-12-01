pub mod config;
pub mod contract_abi;
pub mod db;
pub mod error;
pub mod events;
pub mod metrics;
pub mod pg_utils;
pub mod processing;
pub mod provider;
pub mod tasks;
pub mod transaction;

pub mod test_utils;

use events::{BlockMeta, Event, StakingEvent, SystemEvent, ValidatorEvent};
use events::{
    ClaimRewardsEvent, CommissionChangedEvent, DelegateEvent, EpochChangedEvent, UndelegateEvent,
    ValidatorCreatedEvent, ValidatorRewardedEvent, ValidatorStatusChangedEvent, WithdrawEvent,
};

use alloy::primitives::{Address, U256};

pub const STAKING_CONTRACT_ADDRESS: Address =
    alloy::primitives::address!("0000000000000000000000000000000000001000");

use std::ops::Range;
use tokio::sync::oneshot;

pub fn chunk_range(range: Range<u64>, chunk_size: u64) -> Vec<Range<u64>> {
    let mut chunks = Vec::with_capacity(((range.end - range.start) / chunk_size) as usize);
    let mut chunk_start = range.start;

    while chunk_start < range.end {
        let chunk_end = std::cmp::min(chunk_start + chunk_size, range.end);
        chunks.push(chunk_start..chunk_end);
        chunk_start = chunk_end;
    }

    chunks
}

#[derive(Debug)]
pub struct CompleteBlock {
    pub block_meta: BlockMeta,
    pub events: Vec<StakingEvent>,
}

#[derive(Debug, Default)]
pub struct BlockBatch {
    pub block_meta: Vec<BlockMeta>,
    pub delegate: Vec<DelegateEvent>,
    pub undelegate: Vec<UndelegateEvent>,
    pub withdraw: Vec<WithdrawEvent>,
    pub claim_rewards: Vec<ClaimRewardsEvent>,
    pub validator_rewarded: Vec<ValidatorRewardedEvent>,
    pub epoch_changed: Vec<EpochChangedEvent>,
    pub validator_created: Vec<ValidatorCreatedEvent>,
    pub validator_status_changed: Vec<ValidatorStatusChangedEvent>,
    pub commission_changed: Vec<CommissionChangedEvent>,
}

impl BlockBatch {
    pub fn new() -> Self {
        Self {
            block_meta: Vec::new(),
            delegate: Vec::new(),
            undelegate: Vec::new(),
            withdraw: Vec::new(),
            claim_rewards: Vec::new(),
            validator_rewarded: Vec::new(),
            epoch_changed: Vec::new(),
            validator_created: Vec::new(),
            validator_status_changed: Vec::new(),
            commission_changed: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: events::Event) {
        match event {
            Event::Staking(StakingEvent::Delegate(e)) => self.delegate.push(e),
            Event::Staking(StakingEvent::Undelegate(e)) => self.undelegate.push(e),
            Event::Staking(StakingEvent::Withdraw(e)) => self.withdraw.push(e),
            Event::Staking(StakingEvent::ClaimRewards(e)) => self.claim_rewards.push(e),
            Event::System(SystemEvent::ValidatorRewarded(e)) => self.validator_rewarded.push(e),
            Event::System(SystemEvent::EpochChanged(e)) => self.epoch_changed.push(e),
            Event::Validator(ValidatorEvent::Created(e)) => self.validator_created.push(e),
            Event::Validator(ValidatorEvent::StatusChanged(e)) => {
                self.validator_status_changed.push(e)
            }
            Event::Validator(ValidatorEvent::CommissionChanged(e)) => {
                self.commission_changed.push(e)
            }
        }
    }

    pub fn add_block_meta(&mut self, meta: BlockMeta) {
        self.block_meta.push(meta);
    }
}

#[derive(PartialEq, Debug)]
pub struct TransactionFetchRequest {
    pub transaction_hash: String,
    pub block_number: u64,
    pub event_type: events::StakingEventType,
}

#[derive(PartialEq, Debug)]
pub struct BlockTipsFetchRequest {
    pub block_number: u64,
}

#[derive(PartialEq, Debug)]
pub enum BackfillWork {
    BlockGap(Range<u64>),
    TransactionFetch(TransactionFetchRequest),
    BlockTipsFetch(BlockTipsFetchRequest),
}

pub struct BlockGapsResponse {
    pub gaps: Vec<Range<u64>>,
    pub checkpoint: u64,
    pub max_block: u64,
}

pub enum DbRequest {
    InsertCompleteBlocks(Box<BlockBatch>),
    InsertTransactions(Vec<transaction::EventTxData>),
    InsertBlockTip((u64, U256)),
    GetBlockGaps(oneshot::Sender<Option<BlockGapsResponse>>),
    GetTransactionGaps(oneshot::Sender<Vec<TransactionFetchRequest>>),
    GetBlockTipsGaps(oneshot::Sender<Vec<BlockTipsFetchRequest>>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_range_even_division() {
        let chunks = chunk_range(0..100, 10);
        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0], 0..10);
        assert_eq!(chunks[9], 90..100);
    }

    #[test]
    fn test_chunk_range_uneven_division() {
        let chunks = chunk_range(0..105, 10);
        assert_eq!(chunks.len(), 11);
        assert_eq!(chunks[0], 0..10);
        assert_eq!(chunks[9], 90..100);
        assert_eq!(chunks[10], 100..105);
    }

    #[test]
    fn test_chunk_range_smaller_than_chunk() {
        let chunks = chunk_range(0..5, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], 0..5);
    }

    #[test]
    fn test_chunk_range_single_block() {
        let chunks = chunk_range(5..6, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], 5..6);
    }

    #[test]
    fn test_chunk_range_empty() {
        let chunks = chunk_range(5..5, 100);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_chunk_range_contiguous() {
        let chunks = chunk_range(0..100, 30);
        assert_eq!(chunks.len(), 4);

        // contiguous
        for i in 0..chunks.len() - 1 {
            assert_eq!(chunks[i].end, chunks[i + 1].start);
        }

        // full range
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[chunks.len() - 1].end, 100);
    }
}
