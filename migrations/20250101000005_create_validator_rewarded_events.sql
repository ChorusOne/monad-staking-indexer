CREATE TABLE validator_rewarded_events (
    id BIGSERIAL PRIMARY KEY,
    validator_id BIGINT NOT NULL,
    from_address VARCHAR(40) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_validator_rewarded_validator_id ON validator_rewarded_events(validator_id);
CREATE INDEX idx_validator_rewarded_from_address ON validator_rewarded_events(from_address);
CREATE INDEX idx_validator_rewarded_block_number ON validator_rewarded_events(block_number);
CREATE INDEX idx_validator_rewarded_epoch ON validator_rewarded_events(epoch);
