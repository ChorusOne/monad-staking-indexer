CREATE TABLE commission_changed_events (
    id BIGSERIAL PRIMARY KEY,
    validator_id BIGINT NOT NULL,
    old_commission NUMERIC(78, 0) NOT NULL,
    new_commission NUMERIC(78, 0) NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(validator_id, transaction_hash)
);

CREATE INDEX idx_commission_changed_validator_id ON commission_changed_events(validator_id);
CREATE INDEX idx_commission_changed_block_number ON commission_changed_events(block_number);
