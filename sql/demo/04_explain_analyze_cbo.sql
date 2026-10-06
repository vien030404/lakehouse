-- ===========================================================================
-- 04. CBO, THỐNG KÊ VÀ EXPLAIN ANALYZE
--
-- Trino chọn kế hoạch dựa trên thống kê. Thiếu thống kê, optimizer chỉ dựa
-- vào heuristic và có thể chọn thứ tự join kém hơn nhiều lần.
-- Dùng catalog `tpch` vì dữ liệu sinh theo scale factor, có sẵn thống kê.
-- ===========================================================================

-- --- 4.1 Thống kê Trino đang có ------------------------------------------
SHOW STATS FOR lakehouse.bronze.orders;
SHOW STATS FOR tpch.sf1.lineitem;

-- Thống kê của một truy vấn có lọc (không phải cả bảng):
SHOW STATS FOR (
    SELECT * FROM lakehouse.bronze.orders WHERE order_status = 'delivered'
);


-- --- 4.2 Join reordering: để CBO tự sắp thứ tự ---------------------------
-- 3 bảng, thứ tự viết trong SQL là lineitem -> orders -> customer.
-- CBO sẽ đảo lại để bảng nhỏ nhất vào build side của hash join.
EXPLAIN
SELECT c.mktsegment, sum(l.extendedprice * (1 - l.discount)) AS revenue
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
JOIN tpch.sf1.customer c ON o.custkey  = c.custkey
WHERE o.orderdate >= DATE '1995-01-01'
  AND o.orderdate <  DATE '1995-04-01'
GROUP BY c.mktsegment;


-- --- 4.3 Tắt CBO để thấy khác biệt ---------------------------------------
SET SESSION join_reordering_strategy = 'NONE';
SET SESSION join_distribution_type   = 'PARTITIONED';

EXPLAIN
SELECT c.mktsegment, sum(l.extendedprice * (1 - l.discount)) AS revenue
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
JOIN tpch.sf1.customer c ON o.custkey  = c.custkey
WHERE o.orderdate >= DATE '1995-01-01'
  AND o.orderdate <  DATE '1995-04-01'
GROUP BY c.mktsegment;

-- Trả lại mặc định
SET SESSION join_reordering_strategy = 'AUTOMATIC';
SET SESSION join_distribution_type   = 'AUTOMATIC';


-- --- 4.4 EXPLAIN ANALYZE: số liệu THẬT sau khi chạy ----------------------
-- Đọc từ dưới lên. Ở mỗi node chú ý:
--   Input avg./std.dev  : dữ liệu có lệch giữa các task không (skew)
--   CPU / Scheduled time: nút thắt nằm ở đâu
--   Input rows vs Output rows: filter có ăn không
EXPLAIN ANALYZE
SELECT
    c.customer_state,
    count(DISTINCT o.order_id)               AS orders,
    round(sum(i.price + i.freight_value), 2) AS revenue
FROM lakehouse.bronze.orders      o
JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
JOIN lakehouse.bronze.customers   c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY revenue DESC;


-- --- 4.5 EXPLAIN ANALYZE VERBOSE: chi tiết từng operator ----------------
EXPLAIN ANALYZE VERBOSE
SELECT order_status, count(*)
FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
GROUP BY order_status;


-- --- 4.6 Các dạng EXPLAIN khác -------------------------------------------
EXPLAIN (TYPE LOGICAL)      SELECT count(*) FROM lakehouse.bronze.orders;
EXPLAIN (TYPE DISTRIBUTED)  SELECT count(*) FROM lakehouse.bronze.orders;
EXPLAIN (TYPE IO, FORMAT JSON)
    SELECT * FROM lakehouse.bronze.orders WHERE order_status = 'delivered';


-- --- 4.7 ANALYZE: cập nhật thống kê cho Iceberg --------------------------
-- Chạy lại sau mỗi lần ghi lớn để CBO không dùng số liệu cũ (DAG Airflow
-- làm việc này cho silver_order_facts).
ANALYZE lakehouse.bronze.orders;
ANALYZE lakehouse.bronze.order_items WITH (columns = ARRAY['order_id', 'product_id', 'price']);

SHOW STATS FOR lakehouse.bronze.order_items;


-- --- 4.8 Soi lại lịch sử truy vấn -----------------------------------------
SELECT
    query_id,
    state,
    date_diff('second', created, "end") AS seconds,
    substr(query, 1, 80)                AS query
FROM system.runtime.queries
WHERE query NOT LIKE '%system.runtime%'
ORDER BY created DESC
LIMIT 20;
