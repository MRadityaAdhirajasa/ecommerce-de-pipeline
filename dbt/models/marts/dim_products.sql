{{ config(materialized='table') }}

-- Satu baris per produk. Harga terakhir diambil dari event paling baru,
-- tapi event berharga 0 dikalahkan duluan lewat urutan is_price_zero --
-- supaya produk yang kebetulan selalu berharga 0 tetap punya baris di sini,
-- bukan hilang dan bikin test relationships merah.
--
-- Dibangun ulang penuh tiap run. Aman di 31 hari; kalau nanti datanya
-- nambah banyak, ganti ke incremental merge per product_id.
with ranked as (
    select
        product_id,
        brand,
        category_id,
        category_code,
        category_l1,
        category_l2,
        price,
        row_number() over (
            partition by product_id
            order by is_price_zero asc, event_time desc
        ) as rn
    from {{ ref('stg_events') }}
)
select
    product_id,
    brand,
    category_id,
    category_code,
    category_l1,
    category_l2,
    price as last_price
from ranked
where rn = 1