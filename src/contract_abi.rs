use alloy::sol;

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
