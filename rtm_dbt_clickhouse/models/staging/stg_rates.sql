with source as (
    select * from {{ source('raw', 'rates') }}
),

renamed as (
    select
        symbol,
        CAST(open_time AS DateTime) as open_time,
        CAST(close_time AS DateTime) as close_time,
        CAST(close AS Float64) as close_price
    from source
)

select * from renamed
