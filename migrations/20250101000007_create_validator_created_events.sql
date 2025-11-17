CREATE TABLE validator_created_events (
    id BIGSERIAL PRIMARY KEY,
    validator_id BIGINT NOT NULL,
    auth_address VARCHAR(40) NOT NULL,
    commission NUMERIC(78, 0) NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_validator_created_validator_id ON validator_created_events(validator_id);
CREATE INDEX idx_validator_created_auth_address ON validator_created_events(auth_address);
CREATE INDEX idx_validator_created_block_number ON validator_created_events(block_number);
