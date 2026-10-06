{{
    config(
        materialized      = 'incremental',
        incremental_strategy = 'merge',
        unique_key        = ['order_id', 'order_item_id'],
        on_schema_change  = 'append_new_columns',
        properties = {
            'partitioning': "ARRAY['month(order_purchase_timestamp)']",
            'format':       "'PARQUET'"
        }
    )
}}

-- =========================================================================
-- Model incremental (strategy merge) - hạt: 1 dòng / (order_id, order_item_id)
--
-- Bảng fact trung tâm của silver: dòng hàng ⋈ đơn ⋈ thanh toán ⋈ đánh giá,
-- hợp nhất lịch sử Olist và sự kiện stream. Mọi bảng gold đọc từ đây.
-- dbt-trino dịch strategy merge thành MERGE INTO trên Iceberg, nên từ lần
-- chạy thứ hai chỉ các đơn có thay đổi bị ghi lại.
--
-- Chạy lại toàn bộ:  dbt run --select silver_order_facts --full-refresh
-- =========================================================================

with items as (

    select
        order_id,
        cast(order_item_id as integer)  as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    from (
        select order_id, order_item_id, product_id, seller_id,
               shipping_limit_date, price, freight_value
        from {{ source('bronze', 'order_items') }}

        union all

        select order_id, order_item_id, product_id, seller_id,
               shipping_limit_date, price, freight_value
        from (
            select distinct event_id, order_id, order_item_id, product_id, seller_id,
                   shipping_limit_date, price, freight_value
            from {{ source('bronze', 'order_item_events') }}
            where {{ cutoff_filter() }}
        )
    )

),

orders as (

    select * from {{ ref('silver_orders') }}

    {% if is_incremental() %}
      -- Đơn mua trong cửa sổ gần đây. Mốc tính từ chính bảng đích, không
      -- phụ thuộc đồng hồ của máy chạy dbt.
      where order_purchase_timestamp >= (
          select coalesce(
                     max(order_purchase_timestamp) - interval '{{ var("incremental_lookback_days") }}' day,
                     timestamp '1970-01-01 00:00:00'
                 )
          from {{ this }}
      )
      {% if var('prev_cutoff_ts', none) %}
      -- Cộng thêm mọi đơn có sự kiện (trạng thái, thanh toán, đánh giá) giữa
      -- hai lần chốt, bất kể mua từ bao giờ. Cửa sổ ở trên tính theo thời
      -- gian mô phỏng, mà giữa hai lần chốt đồng hồ mô phỏng có thể tiến hàng
      -- chục ngày: một đơn mua từ lâu vừa được giao sẽ nằm ngoài cửa sổ và
      -- không được cập nhật nếu chỉ dựa vào nó.
      or order_id in (
          {%- for t in ['order_events', 'payment_events', 'review_events'] %}
          select order_id from {{ source('bronze', t) }}
          where ingest_ts >= timestamp '{{ var("prev_cutoff_ts") }}' and {{ cutoff_filter() }}
          {%- if not loop.last %} union{% endif %}
          {%- endfor %}
      )
      {% endif %}
    {% endif %}

),

payments as (

    select * from {{ ref('silver_payments') }}

),

reviews as (

    select
        order_id,
        avg(cast(review_score as double)) as review_score_avg
    from (
        select order_id, review_score from {{ source('bronze', 'reviews') }}
        union all
        select order_id, review_score
        from (
            select distinct event_id, order_id, review_score
            from {{ source('bronze', 'review_events') }}
            where {{ cutoff_filter() }}
        )
    )
    group by order_id

)

select
    o.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,

    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_month,
    o.delivery_days,
    o.delivery_delay_days,

    i.price,
    i.freight_value,
    i.price + i.freight_value                       as item_revenue,
    i.shipping_limit_date,

    -- thuộc tính cấp đơn: lặp lại trên mọi dòng hàng của đơn, không cộng dồn
    p.payment_total,
    p.primary_payment_type,
    p.max_installments,

    r.review_score_avg,

    current_timestamp(6)                            as dbt_loaded_at

from orders o
join items i
  on o.order_id = i.order_id
left join payments p
  on o.order_id = p.order_id
left join reviews r
  on o.order_id = r.order_id
