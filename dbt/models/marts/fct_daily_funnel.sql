{{ config(
    materialized='incremental',
    unique_key='event_date',
    on_schema_change='append_new_columns'
) }}

-- Funnel dihitung per sesi, bukan per event: satu sesi yang lihat produk
-- 20 kali tetap dihitung satu, biar conversion rate-nya masuk akal.
with per_session as (
    select
        event_date,
        user_session,
        max(case when event_type = 'view'     then 1 else 0 end) as did_view,
        max(case when event_type = 'cart'     then 1 else 0 end) as did_cart,
        max(case when event_type = 'purchase' then 1 else 0 end) as did_purchase
    from {{ ref('stg_events') }}
    where event_date = '{{ var("run_date") }}'
    group by event_date, user_session
)
select
    event_date,
    count(*)          as sessions,
    sum(did_view)     as sessions_view,
    sum(did_cart)     as sessions_cart,
    sum(did_purchase) as sessions_purchase,
    -- Dataset ini nggak nyatat semua event cart: sekitar 65% sesi yang beli
    -- nggak punya event cart sama sekali. Dihitung eksplisit supaya bolongnya
    -- terukur tiap hari, bukan cuma jadi catatan kaki di README.
    sum(case when did_purchase = 1 and did_cart = 0 then 1 else 0 end)
        as sessions_purchase_no_cart,
    round(sum(did_purchase) / nullif(sum(did_view), 0), 4) as conversion_rate
from per_session
group by event_date