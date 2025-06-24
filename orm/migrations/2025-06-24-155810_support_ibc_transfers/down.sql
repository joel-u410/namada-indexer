-- Restore previous definition of u410.other_txs view
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
WHERE (it.kind <> ALL (ARRAY['transparent_transfer'::transaction_kind, 'claim_rewards'::transaction_kind, 'bond'::transaction_kind, 'unbond'::transaction_kind, 'redelegation'::transaction_kind]))
AND it.exit_code = 'applied'::transaction_result
AND NOT a.hidden
ORDER BY
    w.block_height;

DROP TABLE u410.transaction_kinds;

DROP VIEW u410.accounting;

ALTER VIEW u410.accounting_v2 RENAME TO accounting;

