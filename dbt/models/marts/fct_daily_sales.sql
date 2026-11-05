{{ config(
    materialized='incremental',
    unique_key='event_date',
    on_schema_change='append_new_columns'
) }}

-- Satu baris per tanggal. unique_key = event_date bikin run ulang
-- tanggal yang sama cuma menimpa barisnya, bukan nambah baris baru.
select
    event_date,
    sum(case when is_price_zero then 0 else price end) as revenue,
    count(*)                                           as purchase_events,
    count(distinct user_session)                       as orders,
    count(distinct user_id)                            as buyers
from {{ ref('stg_events') }}
where event_type = 'purchase'
  and event_date = '{{ var("run_date") }}'
group by event_date