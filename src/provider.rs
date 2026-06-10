use crate::contract_abi::StakingPrecompile::{getDelegatorCall, getDelegatorsCall};
use crate::events::u256_to_bigdecimal;
use crate::{DelegatorInfo, STAKING_CONTRACT_ADDRESS, metrics::Metric};

use std::ops::Range;

use async_stream::stream;
use eyre::Result;
use futures_util::stream::{Stream, StreamExt};
use log::{debug, error, info};
use tokio::sync::mpsc;
use tokio::time::Duration;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    pubsub::PubSubFrontend,
    rpc::types::{BlockTransactionsKind, Filter},
    sol_types::SolCall,
};

pub struct ReconnectProvider {
    urls: Vec<String>,
    watchdog_timeout: Duration,
    attempts: u64,
}

pub struct ConnectedProvider {
    provider: RootProvider<PubSubFrontend>,
    watchdog_timeout: Duration,
}

impl ReconnectProvider {
    pub fn new(urls: Vec<String>, watchdog_timeout_secs: u64) -> Self {
        assert!(!urls.is_empty(), "RPC URLs list cannot be empty");

        ReconnectProvider {
            urls,
            watchdog_timeout: Duration::from_secs(watchdog_timeout_secs),
            attempts: 0,
        }
    }

    pub fn mark_current_failed(&mut self) {
        self.attempts += 1;
    }

    pub async fn connect(
        &mut self,
        metrics_tx: &mpsc::UnboundedSender<Metric>,
    ) -> ConnectedProvider {
        loop {
            let url = &self.urls[(self.attempts as usize) % self.urls.len()];
            debug!("Attempting to connect to RPC: {}", url);

            let ws = WsConnect::new(url);
            let connection_timeout = Duration::from_secs(5);

            match tokio::time::timeout(connection_timeout, ProviderBuilder::new().on_ws(ws)).await {
                Ok(Ok(provider)) => {
                    info!("Successfully connected to RPC: {}", url);
                    return ConnectedProvider {
                        provider,
                        watchdog_timeout: self.watchdog_timeout,
                    };
                }
                Ok(Err(e)) => {
                    error!("Failed to connect to {url}: {e:?}");
                    let _ = metrics_tx.send(Metric::RpcConnRefused);
                    self.mark_current_failed();
                }
                Err(_) => {
                    error!("Timed out connecting to {url}");
                    let _ = metrics_tx.send(Metric::RpcTimeout);
                    self.mark_current_failed();
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl ConnectedProvider {
    pub async fn get_transaction(
        &self,
        hash: &str,
    ) -> Result<Option<alloy::rpc::types::Transaction>> {
        let hash_bytes = hex::decode(hash)?;
        let tx_hash = alloy::primitives::FixedBytes::from_slice(&hash_bytes);
        self.provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(Into::into)
    }

    pub async fn get_full_block(&self, block_number: u64) -> Result<alloy::rpc::types::Block> {
        match self
            .provider
            .get_block_by_number(block_number.into(), BlockTransactionsKind::Full)
            .await
        {
            Ok(Some(b)) => Ok(b),
            Ok(None) => {
                error!("Block {} not found", block_number);
                Err(eyre::eyre!("Block {} not found", block_number))
            }
            Err(e) => {
                error!("Failed to fetch block {}: {:?}", block_number, e);
                Err(e.into())
            }
        }
    }

    pub async fn historical_logs(&self, range: &Range<u64>) -> Result<Vec<alloy::rpc::types::Log>> {
        let filter = Filter::new()
            .address(STAKING_CONTRACT_ADDRESS)
            .from_block(range.start)
            .to_block(range.end.saturating_sub(1));

        self.provider.get_logs(&filter).await.map_err(Into::into)
    }

    pub async fn stream_events(self) -> Result<impl Stream<Item = alloy::rpc::types::Log>> {
        let filter = Filter::new().address(STAKING_CONTRACT_ADDRESS);
        let event_stream = self.provider.subscribe_logs(&filter).await?.into_stream();

        let watchdog_timeout = self.watchdog_timeout;
        let provider_monitor = self.provider;

        Ok(stream! {
            let mut stream = event_stream;
            let _keep_alive = provider_monitor;

            loop {
                match tokio::time::timeout(watchdog_timeout, stream.next()).await {
                    Ok(Some(log)) => yield log,
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        })
    }

    async fn get_all_delegators(
        &self,
        validator_id: u64,
        block_number: u64,
    ) -> Result<Vec<Address>> {
        let mut all_delegators = Vec::new();
        let mut start_delegator = Address::ZERO;
        let mut is_done = false;

        while !is_done {
            let call_data = getDelegatorsCall {
                validatorId: validator_id,
                startDelegator: start_delegator,
            }
            .abi_encode();

            let tx = alloy::rpc::types::TransactionRequest::default()
                .to(STAKING_CONTRACT_ADDRESS)
                .input(call_data.into());

            let result = self.provider.call(&tx).block(block_number.into()).await?;

            let decoded = getDelegatorsCall::abi_decode_returns(&result, true)?;

            is_done = decoded.isDone;
            start_delegator = decoded.nextDelegator;

            let delegator_count = decoded.delegators.len();
            all_delegators.extend(decoded.delegators);

            info!(
                "Fetched {} delegators (total: {}, done: {})",
                delegator_count,
                all_delegators.len(),
                is_done
            );
        }

        Ok(all_delegators)
    }

    pub async fn get_delegator_snapshot(
        &self,
        height: u64,
        validator_id: u64,
    ) -> Result<Vec<DelegatorInfo>> {
        let delegators = self.get_all_delegators(validator_id, height).await?;
        let mut result = Vec::new();

        for delegator in delegators {
            match self
                .get_delegator_info(validator_id, &delegator, height)
                .await
            {
                Ok(info) => result.push(info),
                Err(e) => {
                    error!("Failed to get delegator info for {:?}: {:?}", delegator, e);
                }
            }
        }

        Ok(result)
    }

    async fn get_delegator_info(
        &self,
        validator_id: u64,
        delegator: &Address,
        block_number: u64,
    ) -> Result<DelegatorInfo> {
        let call_data = getDelegatorCall {
            validatorId: validator_id,
            delegator: *delegator,
        }
        .abi_encode();

        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(STAKING_CONTRACT_ADDRESS)
            .input(call_data.into());

        let result = self.provider.call(&tx).block(block_number.into()).await?;

        let decoded = getDelegatorCall::abi_decode_returns(&result, true)?;

        Ok(DelegatorInfo {
            delegator: *delegator,
            stake: u256_to_bigdecimal(decoded.stake),
            acc_reward_per_token: u256_to_bigdecimal(decoded.accRewardPerToken),
            unclaimed_rewards: u256_to_bigdecimal(decoded.unclaimedRewards),
            delta_stake: u256_to_bigdecimal(decoded.deltaStake),
            next_delta_stake: u256_to_bigdecimal(decoded.nextDeltaStake),
            delta_epoch: decoded.deltaEpoch,
            next_delta_epoch: decoded.nextDeltaEpoch,
        })
    }
}
