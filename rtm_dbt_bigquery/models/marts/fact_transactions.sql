{{ config(
    materialized='table',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp"
    },
    cluster_by=["user_id", "kyc_level_at_transaction"]
) }}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    tx_id,
    user_id,
    cast(transaction_at as date) as date_key,
    created_at,
    destination_currency,
    status,
    kyc_level_at_transaction,
    destination_amount,
    amount_usd
from enriched
