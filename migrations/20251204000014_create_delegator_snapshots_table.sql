CREATE TABLE delegator_snapshots (
    validator_id BIGINT NOT NULL,
    delegator VARCHAR(40) NOT NULL,
    epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    stake NUMERIC(78, 0) NOT NULL,
    acc_reward_per_token NUMERIC(78, 0) NOT NULL,
    unclaimed_rewards NUMERIC(78, 0) NOT NULL,
    delta_stake NUMERIC(78, 0) NOT NULL,
    next_delta_stake NUMERIC(78, 0) NOT NULL,
    delta_epoch BIGINT NOT NULL,
    next_delta_epoch BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (validator_id, delegator, epoch, block_number)
);

CREATE INDEX idx_delegator_snapshots_epoch ON delegator_snapshots(epoch);
CREATE INDEX idx_delegator_snapshots_block_number ON delegator_snapshots(block_number);
CREATE INDEX idx_delegator_snapshots_validator ON delegator_snapshots(validator_id);
