CREATE VIEW u410.withdraws AS
WITH balance_changes_with_prev AS (
    SELECT
        b.height,
        b.owner,
        b.raw_amount,
        b.token,
        -- Get the raw_amount from the previous row for the same owner and token, ordered by height.
        -- If there's no previous row (i.e., it's the first entry for this owner), default to 0.
        LAG(b.raw_amount, 1, 0) OVER (PARTITION BY b.owner,
            b.token ORDER BY b.height) AS prev_raw_amount
    FROM
        balance_changes b
    WHERE
        b.owner IN (
            SELECT
                address
            FROM
                u410.addresses
            WHERE
                hidden = FALSE)
),
withdraws AS (
    SELECT
        i.*,
        w.block_height,
        i.data::json ->> 'source' AS address,
        i.data::json ->> 'validator' AS counterparty,
        -- Calculate the actual change: current_balance - previous_balance
        ((b.raw_amount - b.prev_raw_amount) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS balance_change_amount,
        w.gas_used AS gas_used,
        w.gas_limit AS gas_limit,
        (w.gas_limit::numeric(1000, 6) / power(10, 6)::numeric(1000, 6))::numeric(1000, 6) AS fee_amount,
        b.token
    FROM
        inner_transactions i
        JOIN wrapper_transactions w ON i.wrapper_id = w.id
        JOIN balance_changes_with_prev b ON b.height = w.block_height
            AND b.owner = i.data::json ->> 'source'
    WHERE
        w.fee_payer IN (
            SELECT
                address
            FROM
                u410.pos_entries
            WHERE
                tx_type = 'unbond')
            AND i.kind = 'withdraw'
        ORDER BY
            w.block_height
)
    SELECT
        w.block_height AS block_height,
        b.hash AS block_hash,
        b.timestamp AS block_timestamp,
        b.epoch AS epoch,
        w.wrapper_id AS wrapper_id,
        w.id AS inner_id,
        w.kind::text AS tx_type,
        w.address AS address,
        w.counterparty AS counterparty,
        w.balance_change_amount + w.fee_amount AS amount,
        CASE tok.token_type
        WHEN 'native' THEN
            'nam'
        ELSE
            w.token
        END AS token
    FROM
        withdraws w
        JOIN blocks b ON b.height = w.block_height
        JOIN token tok ON tok.address = w.token;

