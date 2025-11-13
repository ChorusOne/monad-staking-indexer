CREATE TABLE withdraw_events (
    id BIGSERIAL PRIMARY KEY,
    val_id BIGINT NOT NULL,
    delegator VARCHAR(40) NOT NULL,
    withdrawal_id SMALLINT NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    activation_epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(64) NOT NULL,
    block_timestamp BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    log_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash, log_index)
);

CREATE INDEX idx_withdraw_val_id ON withdraw_events(val_id);
CREATE INDEX idx_withdraw_delegator ON withdraw_events(delegator);
CREATE INDEX idx_withdraw_block_number ON withdraw_events(block_number);
CREATE INDEX idx_withdraw_activation_epoch ON withdraw_events(activation_epoch);
