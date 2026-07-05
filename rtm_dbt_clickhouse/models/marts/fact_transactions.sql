{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(user_id, kyc_level_at_transaction)',
    partition_by='toYYYYMM(created_at)'
) }}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    tx_id,
    user_id,
    CAST(created_at AS Date) as date_key,
    created_at,
    destination_currency,
    status,
    kyc_level_at_transaction,
    destination_amount,
    amount_usd
from enriched
