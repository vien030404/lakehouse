{{ config(materialized = 'table') }}

-- Khách hàng: lịch sử Olist (bronze.customers) + khách mới từ luồng stream
-- (bronze.customer_events, Spark ghi từ topic olist.customers).
--
-- Gold join bảng này thay vì bronze.customers, vì khách của các đơn mới chỉ
-- có trong customer_events; join với bảng lịch sử sẽ làm rơi các đơn đó.
--
-- cutoff_ts: mốc cắt theo ingest_ts do Airflow truyền, để mọi model silver và
-- gold chốt tại cùng một thời điểm. Không truyền thì lấy toàn bộ.

with hist as (

    select customer_id, customer_unique_id, customer_zip_code_prefix,
           customer_city, customer_state
    from {{ source('bronze', 'customers') }}

),

stream as (

    select customer_id, customer_unique_id, customer_zip_code_prefix,
           customer_city, customer_state,
           -- bronze có thể chứa sự kiện gửi lại (trùng event_id): giữ bản mới nhất
           row_number() over (partition by customer_id order by ingest_ts desc) as rn
    from {{ source('bronze', 'customer_events') }}
    {% if var('cutoff_ts', none) %}
    where ingest_ts < timestamp '{{ var("cutoff_ts") }}'
    {% endif %}

)

select * from hist

union all

select customer_id, customer_unique_id, customer_zip_code_prefix,
       customer_city, customer_state
from stream
where rn = 1
  and customer_id not in (select customer_id from hist)
