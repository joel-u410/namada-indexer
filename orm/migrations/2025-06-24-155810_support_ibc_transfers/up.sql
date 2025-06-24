CREATE OR REPLACE VIEW u410.ibc_transfers AS
WITH all_transfers AS (
    SELECT
        w.block_height,
        b.hash AS block_hash,
        b."timestamp" AS block_timestamp,
        b.epoch,
        w.id AS wrapper_id,
        it.id AS inner_id,
        it.exit_code,
        it.kind,
        'source'::text AS entry_type,
        src.value ->> 'owner'::text AS address,
        string_agg(tgt.value ->> 'owner'::text, ', '::text ORDER BY tgt.ordinality) AS counterparty,
        ((- 1::numeric) * ((src.value ->> 'amount'::text)::numeric) / power(10, 6)::numeric) AS amount,
        src.value ->> 'token'::text AS token_address
    FROM
        inner_transactions it
        JOIN wrapper_transactions w ON it.wrapper_id::text = w.id::text
        JOIN blocks b ON w.block_height = b.height,
        LATERAL jsonb_array_elements((
            CASE it.kind
            WHEN 'transparent_transfer' THEN
                it.data::jsonb
            ELSE
                it.data::jsonb -> 1
            END) -> 'sources'::text) src (value),
        LATERAL jsonb_array_elements((
            CASE it.kind
            WHEN 'transparent_transfer' THEN
                it.data::jsonb
            ELSE
                it.data::jsonb -> 1
            END) -> 'targets'::text)
        WITH ORDINALITY tgt (value, ORDINALITY)
        WHERE
            it.kind = 'ibc_transparent_transfer'::transaction_kind
            AND it.exit_code = 'applied'::transaction_result
        GROUP BY
            w.block_height,
            b.hash,
            b."timestamp",
            b.epoch,
            w.id,
            it.id,
            it.exit_code,
            src.value
        UNION ALL
        SELECT
            w.block_height,
            b.hash AS block_hash,
            b."timestamp" AS block_timestamp,
            b.epoch,
            w.id AS wrapper_id,
            it.id AS inner_id,
            it.exit_code,
            it.kind,
            'target'::text AS entry_type,
            tgt.value ->> 'owner'::text AS address,
            string_agg(src.value ->> 'owner'::text, ', '::text ORDER BY src.ordinality) AS counterparty,
            ((tgt.value ->> 'amount'::text)::numeric / power(10, 6)::numeric) AS amount,
            tgt.value ->> 'token'::text AS token_address
        FROM
            inner_transactions it
            JOIN wrapper_transactions w ON it.wrapper_id::text = w.id::text
            JOIN blocks b ON w.block_height = b.height,
            LATERAL jsonb_array_elements((it.data::jsonb -> 1) -> 'sources'::text)
            WITH ORDINALITY src (value, ORDINALITY), LATERAL jsonb_array_elements((it.data::jsonb -> 1) -> 'targets'::text) tgt (value)
                    WHERE
                        it.kind = 'ibc_transparent_transfer'::transaction_kind
                        AND it.exit_code = 'applied'::transaction_result
                    GROUP BY
                        w.block_height,
                        b.hash,
                        b."timestamp",
                        b.epoch,
                        w.id,
                        it.id,
                        it.exit_code,
                        tgt.value
)
                SELECT
                    t.block_height,
                    t.block_hash,
                    t.block_timestamp,
                    t.epoch,
                    t.wrapper_id,
                    t.inner_id,
                    t.kind::text AS tx_type,
                    t.address,
                    t.counterparty,
                    t.amount,
                    CASE tok.token_type
                    WHEN 'native'::token_type THEN
                        'nam'::text
                    ELSE
                        t.token_address
                    END AS token
                FROM
                    all_transfers t
                    JOIN u410.addresses a ON t.address = a.address
                    JOIN token tok ON t.token_address = tok.address
                WHERE
                    NOT a.hidden;

CREATE TABLE u410.transaction_kinds (
    kind transaction_kind PRIMARY KEY,
    supported boolean NOT NULL DEFAULT TRUE
);

INSERT INTO u410.transaction_kinds (kind, supported)
    VALUES ('transparent_transfer'::transaction_kind, TRUE),
    ('ibc_transparent_transfer'::transaction_kind, TRUE),
    ('claim_rewards'::transaction_kind, TRUE),
    ('bond'::transaction_kind, TRUE),
    ('unbond'::transaction_kind, TRUE),
    ('redelegation'::transaction_kind, TRUE),
    ('shielded_transfer'::transaction_kind, FALSE),
    ('shielding_transfer'::transaction_kind, FALSE),
    ('unshielding_transfer'::transaction_kind, FALSE),
    ('withdraw'::transaction_kind, FALSE),
    ('vote_proposal'::transaction_kind, FALSE),
    ('init_proposal'::transaction_kind, FALSE),
    ('change_metadata'::transaction_kind, FALSE),
    ('change_commission'::transaction_kind, FALSE),
    ('reveal_pk'::transaction_kind, FALSE),
    ('become_validator'::transaction_kind, FALSE),
    ('unknown'::transaction_kind, FALSE),
    ('reactivate_validator'::transaction_kind, FALSE),
    ('deactivate_validator'::transaction_kind, FALSE),
    ('unjail_validator'::transaction_kind, FALSE),
    ('mixed_transfer'::transaction_kind, FALSE),
    ('ibc_msg_transfer'::transaction_kind, FALSE),
    ('ibc_shielding_transfer'::transaction_kind, FALSE),
    ('ibc_unshielding_transfer'::transaction_kind, FALSE),
    ('init_account'::transaction_kind, FALSE),
    ('change_consensus_key'::transaction_kind, FALSE);

CREATE OR REPLACE VIEW u410.other_txs AS
SELECT
    w.block_height,
    b.hash AS block_hash,
    b."timestamp" AS block_timestamp,
    b.epoch,
    w.id AS wrapper_id,
    it.id AS inner_id,
    CASE it.kind::text
    WHEN 'unknown'::text THEN
        it.data::jsonb ->> 'name'::text
    ELSE
        it.kind::text
    END AS tx_type,
    w.fee_payer AS address,
    NULL::text AS counterparty,
    0::numeric(1000, 6) AS amount,
    'nam'::text AS token
FROM
    inner_transactions it
    JOIN wrapper_transactions w ON it.wrapper_id::text = w.id::text
    JOIN blocks b ON w.block_height = b.height
    JOIN u410.addresses a ON w.fee_payer::text = a.address
    LEFT JOIN u410.transaction_kinds k ON it.kind = k.kind
WHERE (k.supported = FALSE
    OR k.kind IS NULL)
AND it.exit_code = 'applied'::transaction_result
AND NOT a.hidden
ORDER BY
    w.block_height;

ALTER VIEW u410.accounting RENAME TO accounting_v2;

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
    ibc_transfers.block_height,
    ibc_transfers.block_hash,
    ibc_transfers.block_timestamp,
    date(ibc_transfers.block_timestamp) AS date,
    ibc_transfers.epoch,
    ibc_transfers.wrapper_id,
    ibc_transfers.tx_type,
    ibc_transfers.address,
    ibc_transfers.counterparty,
    ibc_transfers.amount,
    ibc_transfers.token
FROM
    u410.ibc_transfers
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

