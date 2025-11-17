CREATE TABLE epoch_changed_events (
    id BIGSERIAL PRIMARY KEY,
    old_epoch BIGINT NOT NULL,
    new_epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_hash)
);

CREATE INDEX idx_epoch_changed_old_epoch ON epoch_changed_events(old_epoch);
CREATE INDEX idx_epoch_changed_new_epoch ON epoch_changed_events(new_epoch);
CREATE INDEX idx_epoch_changed_block_number ON epoch_changed_events(block_number);
