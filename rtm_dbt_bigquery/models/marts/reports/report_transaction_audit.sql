{{ config(materialized='view') }}

with transactions as (
    select * from {{ ref('fact_transactions') }}
),

current_users as (
    select
        user_id,
        kyc_level as current_kyc_level
    from {{ ref('dim_users') }}
    where is_current = true
)

select
    t.tx_id,
    t.transaction_at,
    t.user_id,
    t.kyc_level_at_transaction as historical_kyc_level,
    u.current_kyc_level,
    t.amount_usd,
    t.status
from transactions t
left join current_users u on t.user_id = u.user_id
order by t.transaction_at desc
