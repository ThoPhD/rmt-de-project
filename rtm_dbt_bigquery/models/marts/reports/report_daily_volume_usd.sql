{{ config(
    materialized='table',
    partition_by={
        "field": "date_key",
        "data_type": "date"
    },
    cluster_by=["kyc_level_at_transaction"]
) }}

select
    date_key,
    sum(amount_usd) as total_volume_usd,
    count(tx_id) as total_transactions,
    count(distinct user_id) as active_users
from {{ ref('fact_transactions') }}
group by 1
