{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date_key',
    partition_by='toYYYYMM(date_key)'
) }}

select
    date_key,
    sum(amount_usd) as total_volume_usd,
    count(tx_id) as total_transactions,
    count(distinct user_id) as active_users
from {{ ref('fact_transactions') }}
group by date_key
