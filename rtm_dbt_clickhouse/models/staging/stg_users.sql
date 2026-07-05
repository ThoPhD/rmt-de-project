with source as (
    select * from {{ source('raw', 'users') }}
),

renamed as (
    select
        CAST(user_id AS Int32) as user_id,
        CAST(kyc_level AS Int32) as kyc_level,
        CAST(created_at AS DateTime) as created_at,
        CAST(updated_at AS DateTime) as updated_at
    from source
)

select * from renamed
