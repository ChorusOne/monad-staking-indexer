CREATE TABLE block_sync_checkpoint (
    id INT PRIMARY KEY DEFAULT 1,
    last_verified_block BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (id = 1)
);

INSERT INTO block_sync_checkpoint (id, last_verified_block) VALUES (1, 0);
