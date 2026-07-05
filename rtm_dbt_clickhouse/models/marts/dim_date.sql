{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date_day'
) }}

with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2020-01-01' as date)",
      end_date="cast('2030-01-01' as date)"
     )
  }}
)

select
    date_day as date_key,
    date_day,
    toYear(date_day) as year,
    toMonth(date_day) as month,
    toQuarter(date_day) as quarter,
    toDayOfWeek(date_day) as day_of_week,
    formatDateTime(date_day, '%b') as month_name,
    case
        when toDayOfWeek(date_day) in (6, 7) then true
        else false
    end as is_weekend

from date_spine
