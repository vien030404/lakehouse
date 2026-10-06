-- ===========================================================================
-- 01. JOIN TRÊN LAKEHOUSE - 9 bảng Olist, một định dạng, hai engine
--
-- Toàn bộ dữ liệu nằm trong lakehouse.bronze (Iceberg trên MinIO), do Spark
-- nạp từ s3://raw/olist/. Trino đọc chính các bảng Spark đã ghi qua cùng REST
-- catalog: không sao chép, không ETL giữa hai engine.
-- ===========================================================================

-- --- 1.1 Nhìn thấy gì trong cụm ------------------------------------------
SHOW CATALOGS;
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.bronze;


-- --- 1.2 Join 5 bảng trong một câu ---------------------------------------
-- Doanh thu theo bang của khách và nhóm hàng, join 5 bảng Iceberg.
SELECT
    c.customer_state,
    COALESCE(t.product_category_name_english, p.product_category_name) AS category,
    count(DISTINCT o.order_id)          AS orders,
    round(sum(i.price + i.freight_value), 2) AS revenue
FROM lakehouse.bronze.orders        o
JOIN lakehouse.bronze.order_items   i  ON o.order_id = i.order_id
JOIN lakehouse.bronze.customers     c  ON o.customer_id = c.customer_id
LEFT JOIN lakehouse.bronze.products p  ON i.product_id = p.product_id
LEFT JOIN lakehouse.bronze.category_translation t
       ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
  AND o.order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
GROUP BY c.customer_state, COALESCE(t.product_category_name_english, p.product_category_name)
HAVING sum(i.price + i.freight_value) > 10000
ORDER BY revenue DESC
LIMIT 20;


-- --- 1.3 Trino bỏ qua những file nào ------------------------------------
-- Với Iceberg, đẩy điều kiện xuống nguồn nghĩa là không mở các file không
-- cần đọc. Trong kế hoạch, tìm:
--   constraint on [order_purchase_timestamp]  -> cắt tỉa partition theo ngày
--   dynamicFilterAssignments                  -> customers được lọc trước, tập
--                                                khoá dùng để bỏ file của orders
EXPLAIN
SELECT c.customer_state, count(*)
FROM lakehouse.bronze.orders    o
JOIN lakehouse.bronze.customers c ON o.customer_id = c.customer_id
WHERE c.customer_state = 'AC'
  AND o.order_purchase_timestamp >= TIMESTAMP '2018-06-01 00:00:00'
GROUP BY c.customer_state;


-- --- 1.4 Ai đã ghi bảng này: Spark hay Trino ----------------------------
-- Mỗi snapshot Iceberg mang dấu của engine đã commit nó:
--   spark.app.id     -> Spark (spark/jobs/ingest_bronze.py)
--   trino_query_id   -> Trino (dbt, hoặc INSERT/MERGE gõ tay)
SELECT
    committed_at,
    operation,
    CASE
        WHEN element_at(summary, 'spark.app.id')   IS NOT NULL THEN 'Spark'
        WHEN element_at(summary, 'trino_query_id') IS NOT NULL THEN 'Trino'
    END                                                AS engine,
    element_at(summary, 'added-records')               AS added,
    element_at(summary, 'deleted-records')             AS deleted
FROM lakehouse.bronze."customers$snapshots"
ORDER BY committed_at;


-- --- 1.5 Ghi lại kết quả vào lakehouse ----------------------------------
-- Trino đọc bảng do Spark ghi rồi ghi bảng mới - cùng định dạng, cùng
-- catalog; Spark đọc được bảng này theo đúng cách Trino đọc bảng của Spark.
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;

CREATE TABLE IF NOT EXISTS lakehouse.demo.state_revenue_snapshot AS
SELECT
    c.customer_state,
    round(sum(i.price + i.freight_value), 2) AS revenue,
    current_timestamp(6)                     AS captured_at
FROM lakehouse.bronze.orders      o
JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
JOIN lakehouse.bronze.customers   c ON o.customer_id = c.customer_id
GROUP BY c.customer_state;

SELECT * FROM lakehouse.demo.state_revenue_snapshot ORDER BY revenue DESC;
