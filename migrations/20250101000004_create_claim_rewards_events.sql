CREATE TABLE claim_rewards_events (
    id BIGSERIAL PRIMARY KEY,
    val_id BIGINT NOT NULL,
    delegator VARCHAR(40) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_claim_rewards_val_id ON claim_rewards_events(val_id);
CREATE INDEX idx_claim_rewards_delegator ON claim_rewards_events(delegator);
CREATE INDEX idx_claim_rewards_block_number ON claim_rewards_events(block_number);
CREATE INDEX idx_claim_rewards_epoch ON claim_rewards_events(epoch);
