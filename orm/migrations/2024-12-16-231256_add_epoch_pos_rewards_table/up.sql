-- Alters the pos_rewards table to add a fourth column:
-- epoch: INTEGER, representing the epoch number
-- Populate existing records with epoch = 0
ALTER TABLE pos_rewards ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;
-- Now we can safely drop the default
ALTER TABLE pos_rewards ALTER COLUMN epoch DROP DEFAULT;
-- Also update the UNIQUE constraint to include the epoch column
ALTER table pos_rewards ADD CONSTRAINT pos_rewards_owner_validator_id_epoch_key unique (owner, validator_id, epoch);
ALTER table pos_rewards DROP CONSTRAINT pos_rewards_owner_validator_id_key;
