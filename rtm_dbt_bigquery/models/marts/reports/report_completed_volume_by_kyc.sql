SELECT
    kyc_level_at_transaction,
    COUNT(tx_id) AS completed_transaction_count,
    SUM(amount_usd) AS total_completed_volume_usd
FROM
    {{ ref('fact_transactions') }}
WHERE
    status = 'COMPLETED'
GROUP BY
    kyc_level_at_transaction
ORDER BY
    total_completed_volume_usd DESC;
