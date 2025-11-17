CREATE TABLE validator_status_changed_events (
    id BIGSERIAL PRIMARY KEY,
    validator_id BIGINT NOT NULL,
    flags BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_validator_status_changed_validator_id ON validator_status_changed_events(validator_id);
CREATE INDEX idx_validator_status_changed_block_number ON validator_status_changed_events(block_number);
