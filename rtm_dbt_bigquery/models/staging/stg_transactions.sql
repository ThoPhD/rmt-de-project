with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        tx_id::int as tx_id,
        user_id::int as user_id,
        source_currency,
        destination_currency,
        source_amount::float as source_amount,
        destination_amount::float as destination_amount,
        status,
        created_at::timestamp as created_at
    from source
)

select * from renamed;
