CREATE TYPE staking_event_type AS ENUM ('delegate', 'undelegate', 'withdraw', 'claim_rewards');

CREATE TABLE events_tx_data (
    transaction_hash VARCHAR(64) NOT NULL,
    block_number BIGINT NOT NULL,
    event_type staking_event_type NOT NULL,
    access_list JSONB,
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_hash, event_type)
);

CREATE INDEX idx_events_tx_data_block_number ON events_tx_data(block_number);
CREATE INDEX idx_events_tx_data_event_type ON events_tx_data(event_type);
