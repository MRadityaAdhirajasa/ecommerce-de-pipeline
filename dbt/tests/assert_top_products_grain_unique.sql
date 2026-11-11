-- Grain mart_top_products harus satu baris per (minggu, produk).
select week_start, product_id, count(*) as n
from {{ ref('mart_top_products') }}
group by 1, 2
having count(*) > 1
