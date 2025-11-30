with source as (
    select * from {{ source('raw', 'users') }}
),

renamed as (
    select
        user_id::int as user_id,
        kyc_level::int as kyc_level,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at
    from source
)

select * from renamed;
