{{ config(
    materialized='incremental',
    unique_key=['event_date', 'event_hour'],
    on_schema_change='append_new_columns'
) }}

-- Satu baris per (tanggal, jam). Heatmap di Power BI meringkas ini jadi
-- jam x hari-dalam-seminggu lintas semua tanggal yang sudah di-backfill.
select
    event_date,
    hour(event_time)                as event_hour,
    dayofweek(event_date)           as day_of_week,
    date_format(event_date, 'EEEE') as day_name,
    count(*)                        as events,
    count(distinct user_session)    as sessions,
    sum(case when event_type = 'purchase' then 1 else 0 end) as purchases
from {{ ref('stg_events') }}
where event_date = '{{ var("run_date") }}'
group by 1, 2, 3, 4
