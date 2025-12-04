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

        function getDelegators(
            uint64 validatorId,
            address startDelegator
        ) external view returns (
            bool isDone,
            address nextDelegator,
            address[] memory delegators
        );

        function getDelegator(
            uint64 validatorId,
            address delegator
        ) external view returns (
            uint256 stake,
            uint256 accRewardPerToken,
            uint256 unclaimedRewards,
            uint256 deltaStake,
            uint256 nextDeltaStake,
            uint64 deltaEpoch,
            uint64 nextDeltaEpoch
        );
    }
}
