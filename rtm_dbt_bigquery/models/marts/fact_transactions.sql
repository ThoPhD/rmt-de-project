with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    tx_id,
    user_id,
    cast(transaction_at as date) as date_key,
    transaction_at,
    destination_currency,
    status,
    kyc_level_at_transaction,
    destination_amount,
    amount_usd
from enriched
