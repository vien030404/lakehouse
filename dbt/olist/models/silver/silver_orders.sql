{{
    config(
        materialized = 'table',
        properties = {
            'partitioning': "ARRAY['month(order_purchase_timestamp)']",
            'sorted_by':    "ARRAY['order_purchase_timestamp']",
            'format':       "'PARQUET'"
        }
    )
}}

-- Bronze -> Silver: đơn hàng đã làm sạch, hợp nhất lịch sử Olist và trạng
-- thái mới nhất của các đơn từ luồng stream (tới mốc cắt).
-- Partition theo tháng (bronze theo ngày): với khối lượng dữ liệu này, mức
-- tháng cho file đủ lớn mà vẫn cắt tỉa partition được khi lọc theo thời gian.

with stream as (

    -- trạng thái mới nhất (seq cao nhất) của mỗi đơn từ luồng stream, tới mốc cắt
    select order_id, customer_id, order_status, order_purchase_timestamp,
           order_approved_at, order_delivered_carrier_date,
           order_delivered_customer_date, order_estimated_delivery_date,
           row_number() over (partition by order_id order by seq desc, ingest_ts desc) as rn
    from {{ source('bronze', 'order_events') }}
    where {{ cutoff_filter() }}

),

src as (

    select * from {{ source('bronze', 'orders') }}

    union all

    select order_id, customer_id, order_status, order_purchase_timestamp,
           order_approved_at, order_delivered_carrier_date,
           order_delivered_customer_date, order_estimated_delivery_date
    from stream
    where rn = 1
      and order_id not in (select order_id from {{ source('bronze', 'orders') }})

),

cleaned as (

    select
        order_id,
        customer_id,
        lower(trim(order_status))                      as order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- các chỉ số phái sinh dùng lại ở gold
        date_diff('day', order_purchase_timestamp, order_delivered_customer_date)
            as delivery_days,
        date_diff('day', order_estimated_delivery_date, order_delivered_customer_date)
            as delivery_delay_days,
        date_trunc('month', order_purchase_timestamp) as order_month

    from src
    where order_id is not null
      and order_purchase_timestamp is not null

)

select * from cleaned
