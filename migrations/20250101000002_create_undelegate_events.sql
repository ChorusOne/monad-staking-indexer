CREATE TABLE undelegate_events (
    id BIGSERIAL PRIMARY KEY,
    val_id BIGINT NOT NULL,
    delegator VARCHAR(40) NOT NULL,
    withdrawal_id SMALLINT NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    activation_epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_undelegate_val_id ON undelegate_events(val_id);
CREATE INDEX idx_undelegate_delegator ON undelegate_events(delegator);
CREATE INDEX idx_undelegate_block_number ON undelegate_events(block_number);
CREATE INDEX idx_undelegate_activation_epoch ON undelegate_events(activation_epoch);
