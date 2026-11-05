-- Custom test: test lulus kalau query ini nggak balikin baris apa pun.
select event_date, revenue
from {{ ref('fct_daily_sales') }}
where revenue < 0