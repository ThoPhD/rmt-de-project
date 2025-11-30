select
    date_key,
    sum(amount_usd) as total_volume_usd,
    count(tx_id) as total_transactions,
    count(distinct user_id) as active_users
from {{ ref('fct_transactions') }}
group by 1
