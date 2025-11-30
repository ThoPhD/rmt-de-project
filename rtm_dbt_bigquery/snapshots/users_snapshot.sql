{% snapshot users_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    user_id,
    kyc_level,
    created_at,
    updated_at
from {{ ref('stg_users') }}

{% endsnapshot %}
