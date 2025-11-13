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

use events::StakingEvent;

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
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting Monad Staking Indexer...");

    let rpc_url = std::env::var("RPC_URL").expect("Missing RPC_URL env var");
    let database_url = std::env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");

    println!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    println!("Database connected");

    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new().on_ws(ws).await?;

    let staking_contract_address: Address = "0x0000000000000000000000000000000000001000".parse()?;

    let filter = Filter::new()
        .address(staking_contract_address)
        .from_block(0);

    println!("Watching staking contract at: {}", staking_contract_address);
    println!("Listening for events...\n");

    let mut stream = provider.subscribe_logs(&filter).await?.into_stream();

    while let Some(log) = stream.next().await {
        match events::extract_event(&log) {
            Ok(Some(event)) => {
                match &event {
                    StakingEvent::Delegate(e) => {
                        println!("DELEGATE EVENT");
                        println!("  Validator ID: {}", e.val_id);
                        println!("  Delegator: {}", e.delegator);
                        println!("  Amount: {}", e.amount);
                        println!("  Activation Epoch: {}", e.activation_epoch);
                        println!("  Block: {}", e.block_number);
                        println!("  Block Timestamp: {}", e.block_timestamp);
                        println!("  Transaction: {}", e.transaction_hash);
                    }
                    StakingEvent::Undelegate(e) => {
                        println!("UNDELEGATE EVENT");
                        println!("  Validator ID: {}", e.val_id);
                        println!("  Delegator: {}", e.delegator);
                        println!("  Withdrawal ID: {}", e.withdrawal_id);
                        println!("  Amount: {}", e.amount);
                        println!("  Activation Epoch: {}", e.activation_epoch);
                        println!("  Block: {}", e.block_number);
                        println!("  Block Timestamp: {}", e.block_timestamp);
                        println!("  Transaction: {}", e.transaction_hash);
                    }
                    StakingEvent::Withdraw(e) => {
                        println!("WITHDRAW EVENT");
                        println!("  Validator ID: {}", e.val_id);
                        println!("  Delegator: {}", e.delegator);
                        println!("  Withdrawal ID: {}", e.withdrawal_id);
                        println!("  Amount: {}", e.amount);
                        println!("  Activation Epoch: {}", e.activation_epoch);
                        println!("  Block: {}", e.block_number);
                        println!("  Block Timestamp: {}", e.block_timestamp);
                        println!("  Transaction: {}", e.transaction_hash);
                    }
                    StakingEvent::ClaimRewards(e) => {
                        println!("CLAIM REWARDS EVENT");
                        println!("  Validator ID: {}", e.val_id);
                        println!("  Delegator: {}", e.delegator);
                        println!("  Amount: {}", e.amount);
                        println!("  Epoch: {}", e.epoch);
                        println!("  Block: {}", e.block_number);
                        println!("  Block Timestamp: {}", e.block_timestamp);
                        println!("  Transaction: {}", e.transaction_hash);
                    }
                }

                if let Err(e) = db::repository::insert_staking_event(&pool, &event).await {
                    eprintln!("Failed to insert event into database: {}", e);
                } else {
                    println!("  ✓ Stored in database");
                }
                println!();
            }
            Ok(None) => (),
            Err(e) => {
                eprintln!("Error extracting event: {}", e);
            }
        }
    }

    Ok(())
}
