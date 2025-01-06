-- Your SQL goes here
CREATE SCHEMA u410;

-- Reportable addresses
CREATE TABLE IF NOT EXISTS u410.addresses (
    address text PRIMARY KEY,
    hidden boolean DEFAULT FALSE,
    created_at timestamp without time zone DEFAULT now()
);

-- Transfers
CREATE OR REPLACE VIEW u410.transfers AS
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
        (src.value ->> 'owner'::text) AS address,
        string_agg((tgt.value ->> 'owner'::text), ', ' ORDER BY tgt.ordinality) AS counterparty,
        (- 1::numeric * (src.value ->> 'amount'::text)::numeric) AS amount,
        (src.value ->> 'token'::text) AS token
    FROM
        public.inner_transactions it
        JOIN public.wrapper_transactions w ON (it.wrapper_id)::text = (w.id)::text
        JOIN public.blocks b ON w.block_height = b.height,
        LATERAL jsonb_array_elements(((it.data)::jsonb -> 'sources'::text)) src (value),
        LATERAL jsonb_array_elements(((it.data)::jsonb -> 'targets'::text))
        WITH ORDINALITY tgt (value, ORDINALITY)
        WHERE
            it.kind = 'transparent_transfer'::public.transaction_kind
            AND it.exit_code = 'applied'::public.transaction_result
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
            (tgt.value ->> 'owner'::text) AS address,
            string_agg((src.value ->> 'owner'::text), ', ' ORDER BY src.ordinality) AS counterparty,
            (tgt.value ->> 'amount'::text)::numeric AS amount,
            (tgt.value ->> 'token'::text) AS token
        FROM
            public.inner_transactions it
            JOIN public.wrapper_transactions w ON (it.wrapper_id)::text = (w.id)::text
            JOIN public.blocks b ON w.block_height = b.height,
            LATERAL jsonb_array_elements(((it.data)::jsonb -> 'sources'::text))
            WITH ORDINALITY src (value, ORDINALITY), LATERAL jsonb_array_elements(((it.data)::jsonb -> 'targets'::text)) tgt (value)
                    WHERE
                        it.kind = 'transparent_transfer'::public.transaction_kind
                        AND it.exit_code = 'applied'::public.transaction_result
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
                    t.token
                FROM
                    all_transfers t
                    JOIN u410.addresses a ON t.address = a.address
                WHERE
                    NOT a.hidden;

-- Reward claims
CREATE OR REPLACE VIEW u410.reward_claims AS
WITH reward_txs AS (
    -- Get the claim_rewards transactions
    SELECT
        w.block_height,
        b.hash AS block_hash,
        b."timestamp" AS block_timestamp,
        b.epoch,
        w.id AS wrapper_id,
        it.id AS inner_id,
        it.kind AS tx_type,
        (it.data::json ->> 'source') AS address,
        (it.data::json ->> 'validator') AS validator_address,
        -- Add row number to identify first claim in epoch
        ROW_NUMBER() OVER (PARTITION BY b.epoch,
            (it.data::json ->> 'source'),
        (it.data::json ->> 'validator') ORDER BY w.block_height,
    it.id) AS claim_order
FROM
    public.inner_transactions it
    JOIN public.wrapper_transactions w ON it.wrapper_id = w.id
    JOIN public.blocks b ON w.block_height = b.height
    WHERE
        it.kind = 'claim_rewards'
        AND it.exit_code = 'applied'
)
SELECT
    rt.block_height,
    rt.block_hash,
    rt.block_timestamp,
    rt.epoch,
    rt.wrapper_id,
    rt.inner_id,
    rt.tx_type::text AS tx_type,
    rt.address,
    rt.validator_address AS counterparty,
    CASE WHEN rt.claim_order = 1 THEN
        pr.raw_amount::numeric
    ELSE
        0
    END AS amount,
    'nam' AS token
FROM
    reward_txs rt
    JOIN public.pos_rewards pr ON pr.owner = rt.address
        AND pr.epoch = rt.epoch
    JOIN u410.addresses a ON rt.address = a.address
WHERE
    NOT a.hidden
ORDER BY
    rt.block_height;

-- Rewards earned
CREATE OR REPLACE VIEW u410.rewards_earned AS
WITH epoch_starts AS (
    -- Get first block of each epoch
    SELECT DISTINCT ON (epoch)
        height AS block_height,
        hash AS block_hash,
        timestamp AS block_timestamp,
        epoch
    FROM
        public.blocks
    ORDER BY
        epoch,
        height
),
prev_epoch_rewards AS (
    -- Pre-calculate previous epoch rewards
    SELECT
        pr.owner,
        pr.validator_id,
        pr.epoch + 1 AS next_epoch,
        pr.raw_amount::numeric AS prev_amount
    FROM
        public.pos_rewards pr
),
prev_epoch_claims AS (
    -- Pre-calculate previous epoch claims
    SELECT
        rc.address,
        v.id AS validator_id,
        rc.epoch + 1 AS next_epoch,
        pr.raw_amount::numeric AS claimed_amount
    FROM
        u410.reward_claims rc
        JOIN public.validators v ON v.namada_address = rc.counterparty
        JOIN public.pos_rewards pr ON pr.owner = rc.address
            AND pr.validator_id = v.id
            AND pr.epoch = rc.epoch
),
reward_changes AS (
    -- Calculate reward changes between epochs
    SELECT
        e.block_height,
        e.block_hash,
        e.block_timestamp,
        pr.epoch,
        pr.owner AS address,
        v.namada_address AS validator_address,
        (pr.raw_amount::numeric - COALESCE(per.prev_amount, 0) - COALESCE(pec.claimed_amount, 0)) AS amount
    FROM
        public.pos_rewards pr
    JOIN public.validators v ON v.id = pr.validator_id
    JOIN u410.addresses a ON pr.owner = a.address
    JOIN epoch_starts e ON e.epoch = pr.epoch
        LEFT JOIN prev_epoch_rewards per ON per.owner = pr.owner
            AND per.validator_id = pr.validator_id
            AND per.next_epoch = pr.epoch
        LEFT JOIN prev_epoch_claims pec ON pec.address = pr.owner
            AND pec.validator_id = pr.validator_id
            AND pec.next_epoch = pr.epoch
    WHERE
        NOT a.hidden
)
SELECT
    rc.block_height,
    rc.block_hash,
    rc.block_timestamp,
    rc.epoch,
    NULL AS wrapper_id,
    NULL AS inner_id,
    'rewards_earned'::text AS tx_type,
    rc.address,
    rc.validator_address AS counterparty,
    rc.amount,
    'nam' AS token
FROM
    reward_changes rc
WHERE
    rc.amount > 0
ORDER BY
    rc.block_height;

-- Bond, unbond, redelegation entries
CREATE OR REPLACE VIEW u410.pos_entries AS
WITH tx_base AS (
    -- Get base transaction info
    SELECT
        w.block_height,
        b.hash AS block_hash,
        b."timestamp" AS block_timestamp,
        b.epoch,
        w.id AS wrapper_id,
        it.id AS inner_id,
        it.kind::text AS tx_type,
        CASE it.kind
        WHEN 'redelegation' THEN
            (it.data::json ->> 'owner')
        ELSE
            (it.data::json ->> 'source')
        END AS address,
        CASE it.kind
        WHEN 'redelegation' THEN
            ARRAY[(it.data::json ->> 'src_validator'),
            (it.data::json ->> 'dest_validator')]
        ELSE
            ARRAY[(it.data::json ->> 'validator')]
        END AS counterparties,
        CASE it.kind
        WHEN 'redelegation' THEN
            ARRAY[(it.data::json ->> 'amount')::numeric,
            - (it.data::json ->> 'amount')::numeric]
        WHEN 'bond' THEN
            ARRAY[- (it.data::json ->> 'amount')::numeric]
        ELSE
            ARRAY[(it.data::json ->> 'amount')::numeric]
        END AS amounts
    FROM
        public.inner_transactions it
        JOIN public.wrapper_transactions w ON it.wrapper_id = w.id
        JOIN public.blocks b ON w.block_height = b.height
    WHERE
        it.kind IN ('bond', 'unbond', 'redelegation')
        AND it.exit_code = 'applied'
)
SELECT
    t.block_height,
    t.block_hash,
    t.block_timestamp,
    t.epoch,
    t.wrapper_id,
    t.inner_id,
    t.tx_type::text AS tx_type,
    t.address,
    u.counterparty,
    u.amount,
    'nam' AS token
FROM
    tx_base t
    JOIN u410.addresses a ON a.address = t.address,
    LATERAL unnest(t.counterparties, t.amounts) AS u (counterparty,
        amount)
WHERE
    NOT a.hidden
ORDER BY
    t.block_height,
    t.inner_id;

CREATE OR REPLACE VIEW u410.fees_paid AS
SELECT
    w.block_height,
    b.hash AS block_hash,
    b."timestamp" AS block_timestamp,
    b.epoch,
    w.id AS wrapper_id,
    NULL AS inner_id,
    'fee' AS tx_type,
    w.fee_payer AS address,
    NULL AS counterparty,
    - 1::numeric * w.gas_limit::numeric AS amount,
    'nam' AS token
FROM
    public.wrapper_transactions w
    JOIN public.blocks b ON w.block_height = b.height
    JOIN u410.addresses a ON w.fee_payer = a.address
WHERE
    NOT a.hidden
ORDER BY
    block_height;

CREATE OR REPLACE VIEW u410.other_txs AS
SELECT
    w.block_height,
    b.hash AS block_hash,
    b."timestamp" AS block_timestamp,
    b.epoch,
    w.id AS wrapper_id,
    it.id AS inner_id,
    CASE it.kind::text
    WHEN 'unknown' THEN
        it.data::jsonb ->> 'name'
    ELSE
        it.kind::text
    END AS tx_type,
    w.fee_payer AS address,
    NULL AS counterparty,
    0::numeric(1000, 6) AS amount,
    'nam' AS token
FROM
    public.inner_transactions it
    JOIN public.wrapper_transactions w ON it.wrapper_id = w.id
    JOIN public.blocks b ON w.block_height = b.height
    JOIN u410.addresses a ON w.fee_payer = a.address
WHERE
    it.kind NOT IN ('transparent_transfer', 'claim_rewards', 'bond', 'unbond', 'redelegation')
    AND it.exit_code = 'applied'
    AND NOT a.hidden
ORDER BY
    block_height;

CREATE TABLE IF NOT EXISTS u410.manual_entries (
    block_height integer NOT NULL,
    block_hash varchar(64) NOT NULL,
    block_timestamp timestamp without time zone NOT NULL,
    epoch integer NOT NULL,
    wrapper_id text,
    inner_id text,
    tx_type text NOT NULL,
    address text NOT NULL,
    counterparty text,
    amount numeric(1000, 6) NOT NULL,
    token text NOT NULL,
    note text, -- optional field for explaining the manual entry
    created_at timestamp without time zone DEFAULT now()
);

CREATE OR REPLACE VIEW u410.accounting AS
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    (amount::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    token
FROM
    u410.transfers
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    amount,
    token
FROM
    u410.manual_entries
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    (amount::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    token
FROM
    u410.reward_claims
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    'reward:' || address || ':' || epoch AS wrapper_id,
    tx_type,
    address,
    counterparty,
    (amount::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    token
FROM
    u410.rewards_earned
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    (amount::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    token
FROM
    u410.pos_entries
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    amount,
    token
FROM
    u410.other_txs
UNION ALL
SELECT
    block_height,
    block_hash,
    block_timestamp,
    date(block_timestamp) AS date,
    epoch,
    wrapper_id,
    tx_type,
    address,
    counterparty,
    (amount::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS amount,
    token
FROM
    u410.fees_paid
ORDER BY
    block_height,
    tx_type;

-- Reconciliation view computes balances from accounting view
CREATE OR REPLACE VIEW u410.reconciliation AS
WITH bonds_by_address AS (
    SELECT
        address,
        sum(raw_amount) AS raw_amount
    FROM
        public.bonds
    GROUP BY
        address
),
unbonds_by_address AS (
    SELECT
        address,
        sum(raw_amount) AS raw_amount
    FROM
        public.unbonds
    GROUP BY
        address
),
recon AS (
    SELECT
        a.address,
        sum(
            CASE a.tx_type
            WHEN 'rewards_earned' THEN
                0::numeric(1000, 6)
            WHEN 'unbond' THEN
                0::numeric(1000, 6)
            ELSE
                a.amount
            END) AS available_balance,
        (b.raw_amount / power(10, 6)::numeric)::numeric(1000, 6) AS available_rt,
        sum(
            CASE a.tx_type IN ('rewards_earned')
            WHEN TRUE THEN
                a.amount
            ELSE
                0::numeric(1000, 6)
            END) AS claimable_balance,
        (coalesce(r.raw_amount / power(10, 6)::numeric, 0))::numeric(1000, 6) AS claimable_rt,
        sum(
            CASE a.tx_type IN ('bond', 'unbond', 'redelegate')
            WHEN TRUE THEN
                - 1::numeric * a.amount
            ELSE
                0::numeric(1000, 6)
            END) AS bonded_balance,
        (coalesce(bba.raw_amount / power(10, 6)::numeric, 0))::numeric(1000, 6) AS bonded_rt,
        sum(
            CASE a.tx_type
            WHEN 'unbond' THEN
                a.amount
            WHEN 'withdraw' THEN
                - 1::numeric * a.amount
            ELSE
                0::numeric(1000, 6)
            END) AS unbonding_balance,
        (coalesce(uba.raw_amount / power(10, 6)::numeric, 0))::numeric(1000, 6) AS unbonding_rt
    FROM
        u410.accounting a
        LEFT JOIN public.balance_changes b ON (a.address = b.owner
                AND b.height = (
                    SELECT
                        max(height)
                    FROM
                        public.balance_changes
                WHERE
                    OWNER = a.address))
            LEFT JOIN public.pos_rewards r ON (a.address = r.owner
                    AND r.epoch = (
                        SELECT
                            max(epoch)
                        FROM
                            public.pos_rewards))
                LEFT JOIN bonds_by_address bba ON a.address = bba.address
                LEFT JOIN unbonds_by_address uba ON a.address = uba.address
            GROUP BY
                1,
                3,
                5,
                7,
                9
            ORDER BY
                1
)
        SELECT
            r.*,
            (r.available_balance <> r.available_rt)
            OR (r.claimable_balance <> r.claimable_rt)
            OR (r.bonded_balance <> r.bonded_rt)
            OR (r.unbonding_balance <> r.unbonding_rt) AS mismatch
        FROM
            recon r;

