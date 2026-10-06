-- ===========================================================================
-- 05. DYNAMIC FILTERING
--
-- Khi join bảng lớn với bảng nhỏ đã lọc, Trino xây phía build trước, rút ra
-- tập giá trị khoá join rồi đẩy tập đó xuống bước quét bảng lớn. Bảng lớn bỏ
-- qua nhiều file/partition thay vì quét rồi mới loại.
--
-- Cách nhận biết: trong EXPLAIN xuất hiện `dynamicFilterAssignments` /
-- `dynamicFilters = {...}` ở ScanFilterProject của bảng lớn; trong
-- EXPLAIN ANALYZE thì "Input:" của bảng lớn nhỏ hơn hẳn tổng số dòng.
-- ===========================================================================

-- --- 5.1 Kịch bản: bảng lớn (lineitem sf1, ~6M dòng) x bảng nhỏ đã lọc ---
SET SESSION enable_dynamic_filtering = true;

EXPLAIN
SELECT count(*), sum(l.extendedprice)
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
WHERE o.orderdate = DATE '1995-03-15';       -- lọc rất chọn lọc ở bảng nhỏ


-- Chạy thật, ghi lại dòng "Input:" của node quét lineitem
EXPLAIN ANALYZE
SELECT count(*), sum(l.extendedprice)
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
WHERE o.orderdate = DATE '1995-03-15';


-- --- 5.2 Tắt dynamic filtering và so sánh --------------------------------
SET SESSION enable_dynamic_filtering = false;

EXPLAIN ANALYZE
SELECT count(*), sum(l.extendedprice)
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
WHERE o.orderdate = DATE '1995-03-15';

SET SESSION enable_dynamic_filtering = true;


-- --- 5.3 Dynamic filtering trên Iceberg có partition ---------------------
-- bronze.orders partition theo day(order_purchase_timestamp).
-- Khi join với một tập ngày nhỏ, Trino chỉ mở đúng các partition cần.
DROP TABLE IF EXISTS lakehouse.demo.hot_days;
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;

CREATE TABLE lakehouse.demo.hot_days AS
SELECT DISTINCT date(order_purchase_timestamp) AS d
FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-08-01 00:00:00'
  AND order_purchase_timestamp <  TIMESTAMP '2018-08-08 00:00:00';

EXPLAIN ANALYZE
SELECT count(*), round(sum(i.price), 2) AS revenue
FROM lakehouse.bronze.orders      o
JOIN lakehouse.demo.hot_days      d ON date(o.order_purchase_timestamp) = d.d
JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id;


-- --- 5.4 Các núm vặn liên quan --------------------------------------------
-- Ngưỡng kích thước phía build để filter vẫn được đẩy xuống:
SHOW SESSION LIKE 'dynamic_filtering%';

-- Thời gian worker chịu chờ nhận dynamic filter trước khi bắt đầu quét
-- (đặt trong trino/catalog/lakehouse.properties: iceberg.dynamic-filtering.wait-timeout)
SHOW SESSION LIKE 'lakehouse.dynamic_filtering%';


-- --- 5.5 Đo bằng thời gian chạy -------------------------------------------
-- So thời gian giữa lần bật và lần tắt dynamic filtering.
SELECT
    query_id,
    state,
    date_diff('millisecond', created, "end") AS ms,
    substr(query, 1, 70)                     AS query
FROM system.runtime.queries
WHERE query LIKE '%lineitem%'
  AND state = 'FINISHED'
ORDER BY created DESC
LIMIT 10;

-- Số dòng đã thực sự đọc (physical input) chỉ có trong chi tiết query:
--   Web UI  -> http://localhost:8080 -> chọn query -> tab "Stages"
--   REST API-> curl -H "X-Trino-User: admin" \
--                http://localhost:8080/v1/query/<query_id> | jq .queryStats
-- Đây là con số cho thấy dynamic filtering có thực sự giảm dữ liệu đọc hay không.
