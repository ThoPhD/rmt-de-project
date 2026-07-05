{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='user_id',
    partition_by='toYYYYMM(created_at)'
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
        tx.tx_id as tx_id,
        tx.user_id as user_id,
        tx.created_at as created_at,
        tx.status as status,
        tx.source_currency as source_currency,
        tx.source_amount as source_amount,
        tx.destination_currency as destination_currency,
        tx.destination_amount as destination_amount,
        tx.asset_currency as asset_currency,
        tx.asset_amount as asset_amount,
        case
            when tx.asset_currency = 'USDT' then 1
            else coalesce(r.close_price, 0)
        end as exchange_rate,
        tx.asset_amount *
        case when tx.asset_currency = 'USDT' then 1 else coalesce(r.close_price, 0) end
        as amount_usd,
        u.kyc_level as kyc_level_at_transaction
    from tx_with_asset tx

    left join rates r
        on concat(tx.asset_currency, 'USDT') = r.symbol
        and dateTrunc('hour', tx.created_at) = r.open_time

    left join user_history u
        on tx.user_id = u.user_id
        and tx.created_at >= u.dbt_valid_from
        and (tx.created_at < u.dbt_valid_to or u.dbt_valid_to is null)
)

select * from joined
