-- Grain fct_hourly_activity harus satu baris per (tanggal, jam).
-- dbt tidak punya test `unique` untuk kombinasi kolom tanpa package tambahan.
select event_date, event_hour, count(*) as n
from {{ ref('fct_hourly_activity') }}
group by 1, 2
having count(*) > 1
