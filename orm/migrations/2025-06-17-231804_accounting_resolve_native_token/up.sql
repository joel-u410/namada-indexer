CREATE VIEW u410.transfers_with_token AS
SELECT
    t.block_height,
    t.block_hash,
    t.block_timestamp,
    t.epoch,
    t.wrapper_id,
    t.inner_id,
    t.tx_type,
    t.address,
    t.counterparty,
    t.amount,
    CASE tok.token_type
    WHEN 'native' THEN
        'nam'
    ELSE
        t.token
    END AS token
FROM
    u410.transfers t
    LEFT JOIN token tok ON t.token = tok.address;

ALTER VIEW u410.accounting RENAME TO accounting_v1;

CREATE VIEW u410.accounting AS
SELECT
    transfers.block_height,
    transfers.block_hash,
    transfers.block_timestamp,
    date(transfers.block_timestamp) AS date,
    transfers.epoch,
    transfers.wrapper_id,
    transfers.tx_type,
    transfers.address,
    transfers.counterparty,
    transfers.amount,
    transfers.token
FROM
    u410.transfers_with_token transfers
UNION ALL
SELECT
    manual_entries.block_height,
    manual_entries.block_hash,
    manual_entries.block_timestamp,
    date(manual_entries.block_timestamp) AS date,
    manual_entries.epoch,
    manual_entries.wrapper_id,
    manual_entries.tx_type,
    manual_entries.address,
    manual_entries.counterparty,
    manual_entries.amount,
    manual_entries.token
FROM
    u410.manual_entries
UNION ALL
SELECT
    reward_claims.block_height,
    reward_claims.block_hash,
    reward_claims.block_timestamp,
    date(reward_claims.block_timestamp) AS date,
    reward_claims.epoch,
    reward_claims.wrapper_id,
    reward_claims.tx_type,
    reward_claims.address,
    reward_claims.counterparty,
    (reward_claims.amount::numeric(1000, 6) / power(10::double precision, 6::double precision)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    reward_claims.token
FROM
    u410.reward_claims
UNION ALL
SELECT
    rewards_earned.block_height,
    rewards_earned.block_hash,
    rewards_earned.block_timestamp,
    date(rewards_earned.block_timestamp) AS date,
    rewards_earned.epoch,
    (('reward:'::text || rewards_earned.address::text) || ':'::text) || rewards_earned.epoch AS wrapper_id,
    rewards_earned.tx_type,
    rewards_earned.address,
    rewards_earned.counterparty,
    (rewards_earned.amount::numeric(1000, 6) / power(10::double precision, 6::double precision)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    rewards_earned.token
FROM
    u410.rewards_earned
UNION ALL
SELECT
    pos_entries.block_height,
    pos_entries.block_hash,
    pos_entries.block_timestamp,
    date(pos_entries.block_timestamp) AS date,
    pos_entries.epoch,
    pos_entries.wrapper_id,
    pos_entries.tx_type,
    pos_entries.address,
    pos_entries.counterparty,
    (pos_entries.amount::numeric(1000, 6) / power(10::double precision, 6::double precision)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    pos_entries.token
FROM
    u410.pos_entries
UNION ALL
SELECT
    other_txs.block_height,
    other_txs.block_hash,
    other_txs.block_timestamp,
    date(other_txs.block_timestamp) AS date,
    other_txs.epoch,
    other_txs.wrapper_id,
    other_txs.tx_type,
    other_txs.address,
    other_txs.counterparty,
    other_txs.amount,
    other_txs.token
FROM
    u410.other_txs
UNION ALL
SELECT
    fees_paid.block_height,
    fees_paid.block_hash,
    fees_paid.block_timestamp,
    date(fees_paid.block_timestamp) AS date,
    fees_paid.epoch,
    fees_paid.wrapper_id,
    fees_paid.tx_type,
    fees_paid.address,
    fees_paid.counterparty,
    (fees_paid.amount::numeric(1000, 6) / power(10::double precision, 6::double precision)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    fees_paid.token
FROM
    u410.fees_paid
ORDER BY
    1,
    7;

