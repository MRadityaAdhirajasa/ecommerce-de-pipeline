{{ config(
    materialized='incremental',
    unique_key=['week_start', 'product_id'],
    on_schema_change='append_new_columns'
) }}

-- Grain mingguan tapi dijalankan harian, jadi tiap run menghitung ULANG
-- seluruh minggu berjalan (Senin s/d run_date), bukan cuma satu hari.
-- Kalau cuma satu hari, merge akan mengganti baris minggu itu dengan angka
-- satu hari dan menghapus akumulasi hari sebelumnya.
-- Maksimal 7 hari yang dipindai, jadi tetap murah.
--
-- brand dan kategori sengaja tidak disimpan di sini: Power BI menggabungkan
-- tabel ini dengan dim_products. Satu sumber kebenaran untuk atribut produk.
select
    date_trunc('week', event_date) as week_start,
    product_id,
    count(*)                       as purchase_events,
    count(distinct user_session)   as orders,
    count(distinct user_id)        as buyers,
    sum(case when is_price_zero then 0 else price end) as revenue
from {{ ref('stg_events') }}
where event_type = 'purchase'
  and event_date between date_trunc('week', DATE('{{ var("run_date") }}'))
                     and DATE('{{ var("run_date") }}')
group by 1, 2
