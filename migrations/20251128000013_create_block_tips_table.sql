CREATE TABLE block_tips (
    block_number BIGINT PRIMARY KEY,
    tips NUMERIC(78, 0) NOT NULL
);

CREATE INDEX idx_block_tips_block_number ON block_tips(block_number);
