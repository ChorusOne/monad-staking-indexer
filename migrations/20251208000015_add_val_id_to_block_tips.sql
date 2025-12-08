ALTER TABLE block_tips ADD COLUMN val_id BIGINT NULL;

UPDATE block_tips bt
SET val_id = vre.validator_id
FROM validator_rewarded_events vre
WHERE bt.block_number = vre.block_number;

ALTER TABLE block_tips ALTER COLUMN val_id SET NOT NULL;
