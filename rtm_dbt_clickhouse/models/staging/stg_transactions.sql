with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        CAST(txn_id AS Int32) as tx_id,
        CAST(user_id AS Int32) as user_id,
        source_currency,
        destination_currency,
        case 
            when CAST(source_amount AS String) in ('null', 'NaN', 'nan', '') then null 
            else CAST(source_amount AS Float64) 
        end as source_amount,
        case 
            when CAST(destination_amount AS String) in ('null', 'NaN', 'nan', '') then null 
            else CAST(destination_amount AS Float64) 
        end as destination_amount,
        status,
        CAST(created_at AS DateTime) as created_at
    from source
)

select * from renamed
