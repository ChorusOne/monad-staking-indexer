CREATE TABLE delegate_events (
    id BIGSERIAL PRIMARY KEY,
    val_id BIGINT NOT NULL,
    delegator VARCHAR(40) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    activation_epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_delegate_val_id ON delegate_events(val_id);
CREATE INDEX idx_delegate_delegator ON delegate_events(delegator);
CREATE INDEX idx_delegate_block_number ON delegate_events(block_number);
CREATE INDEX idx_delegate_activation_epoch ON delegate_events(activation_epoch);
