mod db;
mod events;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
    sol,
};
use eyre::Result;
use futures_util::stream::StreamExt;
use log::{error, info};

// https://docs.monad.xyz/developer-essentials/staking/staking-precompile#events
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    contract StakingPrecompile {
        event Delegate(
            uint64 indexed valId,
            address indexed delegator,
            uint256 amount,
            uint64 activationEpoch
        );

        event Undelegate(
            uint64 indexed valId,
            address indexed delegator,
            uint8 withdrawal_id,
            uint256 amount,
            uint64 activationEpoch
        );

        event Withdraw(
            uint64 indexed valId,
            address indexed delegator,
            uint8 withdrawal_id,
            uint256 amount,
            uint64 activationEpoch
        );

        event ClaimRewards(
            uint64 indexed valId,
            address indexed delegator,
            uint256 amount,
            uint64 epoch
        );

        event ValidatorRewarded(
            uint64 indexed validatorId,
            address indexed from,
            uint256 amount,
            uint64 epoch
        );

        event EpochChanged(
            uint64 oldEpoch,
            uint64 newEpoch
        );

        event ValidatorCreated(
            uint64 indexed validatorId,
            address indexed authAddress,
            uint256 commission
        );

        event ValidatorStatusChanged(
            uint64 indexed validatorId,
            uint64 flags
        );

        event CommissionChanged(
            uint64 indexed validatorId,
            uint256 oldCommission,
            uint256 newCommission
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    info!("Starting Monad Staking Indexer...");

    let rpc_url = std::env::var("RPC_URL").expect("Missing RPC_URL env var");
    let database_url = std::env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");

    info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    info!("Database connected");

    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new().on_ws(ws).await?;

    let staking_contract_address: Address = "0x0000000000000000000000000000000000001000".parse()?;

    let filter = Filter::new()
        .address(staking_contract_address)
        .from_block(0);

    info!("Watching staking contract at: {}", staking_contract_address);
    info!("Listening for events...");

    let mut stream = provider.subscribe_logs(&filter).await?.into_stream();

    while let Some(log) = stream.next().await {
        match events::extract_event(&log) {
            Ok(Some(event)) => {
                if let Err(e) = db::repository::insert_staking_event(&pool, &event).await {
                    error!("Failed to insert event ({event}) into database: {}", e);
                } else {
                    info!("{event} stored in database");
                }
            }
            Ok(None) => (),
            Err(e) => {
                error!("Error extracting event: {}", e);
            }
        }
    }

    Ok(())
}
