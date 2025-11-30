with source as (
    select * from {{ source('raw', 'rates') }}
),

renamed as (
    select
        symbol,
        open_time::timestamp as open_time,
        close_time::timestamp as close_time,
        close::float as close_price
    from source
)

select * from renamed;
