{{ config(
    materialized='table',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp"
    },
    cluster_by=["user_id"]
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

rates as (
    select * from {{ ref('stg_rates') }}
),

user_history as (
    select * from {{ ref('users_snapshot') }}
),

tx_with_asset as (
    select
        t.*,
        case
            when t.destination_currency != 'USDT'
                then t.destination_currency
            else t.source_currency
        end as asset_currency,
        case
            when t.destination_currency != 'USDT'
                then t.destination_amount
            else t.source_amount
        end as asset_amount
    from transactions t
),

joined as (
    select
        tx.tx_id,
        tx.user_id,
        tx.created_at,
        tx.status,
        tx.source_currency,
        tx.source_amount,
        tx.destination_currency,
        tx.destination_amount,
        tx.asset_currency,
        tx.asset_amount,
        case
            when tx.asset_currency = 'USDT' then 1
            else coalesce(r.close_price, 0)
        end as exchange_rate,
        tx.asset_amount *
        case when tx.asset_currency = 'USDT' then 1 coalesce(r.close_price, 0) end
        as amount_usd,
        u.kyc_level as kyc_level_at_transaction
    from tx_with_asset tx

    left join rates r
        on (tx.asset_currency || 'USDT') = r.symbol
        and date_trunc('hour', tx.created_at) = r.open_time

    left join user_history u
        on tx.user_id = u.user_id
        and tx.created_at >= u.dbt_valid_from
        and (tx.created_at < u.dbt_valid_to or u.dbt_valid_to is null)
)

select * from joined;
