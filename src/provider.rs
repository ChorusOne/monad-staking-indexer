use crate::{STAKING_CONTRACT_ADDRESS, metrics::Metric};

use std::ops::Range;

use async_stream::stream;
use eyre::Result;
use futures_util::stream::{Stream, StreamExt};
use log::{debug, error, info};
use tokio::sync::mpsc;
use tokio::time::Duration;

use alloy::{
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    pubsub::PubSubFrontend,
    rpc::types::Filter,
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
                    self.attempts += 1;
                }
                Err(_) => {
                    error!("Timed out connecting to {url}");
                    let _ = metrics_tx.send(Metric::RpcTimeout);
                    self.attempts += 1;
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
}
