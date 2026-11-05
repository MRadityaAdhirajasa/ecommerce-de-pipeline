-- Lapisan tipis di atas silver. Tugasnya cuma satu: menyeragamkan
-- perlakuan terhadap kategori kosong, biar semua model gold sepakat.
select
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    coalesce(category_l1, '(unknown)') as category_l1,
    coalesce(category_l2, '(unknown)') as category_l2,
    coalesce(brand, '(unknown)')       as brand,
    price,
    is_price_zero,
    user_id,
    user_session,
    event_date
from {{ source('silver', 'events') }}