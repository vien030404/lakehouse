-- ===========================================================================
-- View truy vấn tức thì trên silver: dữ liệu đã chốt (dbt) + dữ liệu live (Spark)
--
--   docker compose run --rm trino-cli -f /sql/streaming/silver_views.sql
--
-- Bản live được ưu tiên: đơn nào có trong silver.orders_live thì lấy bản
-- live (mới nhất), còn lại lấy bản chuẩn. Mỗi view có cùng hạt và cùng cột
-- với bảng chuẩn tương ứng (cộng cột `nguon`), nên dùng thay thế trực tiếp.
--
-- bronze.*_events có thể chứa sự kiện trùng event_id khi producer gửi lại
-- sau khoảng dài hơn cửa sổ khử trùng 10 phút của Spark; các view khử trùng
-- thêm một lần theo event_id.
--
-- Gold không có view tức thì: gold là số liệu đã chốt, cập nhật mỗi ngày
-- bởi Airflow.
-- ===========================================================================

-- Khách: lịch sử Olist + khách mới từ luồng stream
CREATE OR REPLACE VIEW lakehouse.silver.v_customers_now AS
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM lakehouse.bronze.customers
UNION ALL
SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM lakehouse.bronze.customer_events;


-- Đơn: cùng cột với silver_orders, cộng cột nguồn
CREATE OR REPLACE VIEW lakehouse.silver.v_orders_now AS
SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
       delivery_days, delivery_delay_days, order_month,
       'live' AS nguon
FROM lakehouse.silver.orders_live
UNION ALL
SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
       delivery_days, delivery_delay_days, order_month,
       'chuan' AS nguon
FROM lakehouse.silver.silver_orders s
WHERE NOT EXISTS (SELECT 1 FROM lakehouse.silver.orders_live l WHERE l.order_id = s.order_id);


-- Dòng hàng: cùng hạt và cùng cột với silver_order_facts, cộng cột nguồn
CREATE OR REPLACE VIEW lakehouse.silver.v_order_facts_now AS
WITH items AS (
    SELECT DISTINCT event_id, order_id, order_item_id, product_id, seller_id,
           shipping_limit_date, price, freight_value
    FROM lakehouse.bronze.order_item_events
),
pay AS (
    SELECT order_id,
           sum(payment_value)                  AS payment_total,
           max_by(payment_type, payment_value) AS primary_payment_type,
           max(payment_installments)           AS max_installments
    FROM (SELECT DISTINCT event_id, order_id, payment_type, payment_installments, payment_value
          FROM lakehouse.bronze.payment_events)
    GROUP BY order_id
),
rev AS (
    SELECT order_id, avg(CAST(review_score AS double)) AS review_score_avg
    FROM (SELECT DISTINCT event_id, order_id, review_score FROM lakehouse.bronze.review_events)
    GROUP BY order_id
)
SELECT o.order_id, i.order_item_id, o.customer_id, i.product_id, i.seller_id,
       o.order_status, o.order_purchase_timestamp, o.order_delivered_customer_date,
       o.order_month, o.delivery_days, o.delivery_delay_days,
       i.price, i.freight_value, i.price + i.freight_value AS item_revenue,
       i.shipping_limit_date, p.payment_total, p.primary_payment_type, p.max_installments,
       r.review_score_avg,
       CAST(o.updated_at AS timestamp(6) with time zone) AS dbt_loaded_at,
       'live' AS nguon
FROM lakehouse.silver.orders_live o
JOIN items i ON i.order_id = o.order_id
LEFT JOIN pay p ON p.order_id = o.order_id
LEFT JOIN rev r ON r.order_id = o.order_id
UNION ALL
SELECT f.*, 'chuan' AS nguon
FROM lakehouse.silver.silver_order_facts f
WHERE NOT EXISTS (SELECT 1 FROM lakehouse.silver.orders_live l WHERE l.order_id = f.order_id);


-- ===========================================================================
-- Ví dụ truy vấn tức thì (không phải view)
-- ===========================================================================

-- Phân bố trạng thái đơn tính tới lô Spark gần nhất
-- SELECT order_status, count(*) FROM lakehouse.silver.orders_live GROUP BY 1;

-- Doanh thu theo ngày mô phỏng và theo bang: lịch sử Olist nối liền dữ liệu mới
-- SELECT date(f.order_purchase_timestamp) AS ngay, c.customer_state,
--        count(DISTINCT f.order_id) AS don, round(sum(f.item_revenue), 2) AS doanh_thu
-- FROM lakehouse.silver.v_order_facts_now f
-- JOIN lakehouse.silver.v_customers_now c USING (customer_id)
-- WHERE f.order_purchase_timestamp >= TIMESTAMP '2018-10-01 00:00:00'
-- GROUP BY 1, 2 ORDER BY 1 DESC, 4 DESC;
