-- ===========================================================================
-- 03. DML TRÊN LAKEHOUSE: MERGE / UPDATE / DELETE
--
-- Iceberg hỗ trợ DML ở mức dòng mà data lake kiểu Hive + Parquet không có:
-- sửa một dòng không cần ghi lại cả partition. File này làm việc trên bảng
-- nháp trong schema lakehouse.demo.
-- ===========================================================================

-- --- 3.1 Chuẩn bị bảng đích và một "batch dữ liệu mới" -------------------
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;

DROP TABLE IF EXISTS lakehouse.demo.order_status_scd;

CREATE TABLE lakehouse.demo.order_status_scd
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['month(order_purchase_timestamp)']
) AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    CAST(0 AS bigint)     AS version,
    current_timestamp(6)  AS updated_at
FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00';

SELECT order_status, count(*) FROM lakehouse.demo.order_status_scd GROUP BY order_status;


-- Batch mới: một số đơn đổi trạng thái, một số đơn hoàn toàn mới.
DROP TABLE IF EXISTS lakehouse.demo.order_status_updates;

CREATE TABLE lakehouse.demo.order_status_updates AS
SELECT
    order_id,
    customer_id,
    'delivered'              AS order_status,
    order_purchase_timestamp
FROM lakehouse.bronze.orders
WHERE order_status IN ('shipped', 'processing')
LIMIT 5000;


-- --- 3.2 MERGE (upsert) --------------------------------------------------
-- Cơ chế dbt dùng cho model incremental strategy='merge' (silver_order_facts).
MERGE INTO lakehouse.demo.order_status_scd AS t
USING lakehouse.demo.order_status_updates AS s
   ON t.order_id = s.order_id

WHEN MATCHED AND t.order_status <> s.order_status THEN
    UPDATE SET
        order_status = s.order_status,
        version      = t.version + 1,
        updated_at   = current_timestamp(6)

WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_status, order_purchase_timestamp, version, updated_at)
    VALUES (s.order_id, s.customer_id, s.order_status, s.order_purchase_timestamp, 0, current_timestamp(6));


SELECT version, count(*) AS rows
FROM lakehouse.demo.order_status_scd
GROUP BY version
ORDER BY version;


-- --- 3.3 UPDATE / DELETE có điều kiện ------------------------------------
UPDATE lakehouse.demo.order_status_scd
SET order_status = 'archived'
WHERE order_purchase_timestamp < TIMESTAMP '2018-02-01 00:00:00'
  AND order_status = 'delivered';

DELETE FROM lakehouse.demo.order_status_scd
WHERE order_status = 'canceled';


-- --- 3.4 Chi phí của DML: delete file ------------------------------------
-- Iceberg ghi position delete file thay vì viết lại data file. Delete file
-- tích luỹ làm việc đọc chậm dần, nên cần optimize định kỳ
-- (xem 07_iceberg_maintenance.sql).
SELECT
    snapshot_id,
    operation,
    committed_at,
    element_at(summary, 'added-position-delete-files') AS delete_files_added,
    element_at(summary, 'added-records')               AS records_added,
    element_at(summary, 'deleted-records')             AS records_deleted
FROM lakehouse.demo."order_status_scd$snapshots"
ORDER BY committed_at;

SELECT content, count(*) AS n_files, sum(record_count) AS records
FROM lakehouse.demo."order_status_scd$files"
GROUP BY content;


-- --- 3.5 Ghi có phân vùng động (partitioned INSERT) ----------------------
-- Không cần khai partition ở câu INSERT: Iceberg tự định tuyến theo
-- partition spec của bảng (hidden partitioning).
INSERT INTO lakehouse.demo.order_status_scd
SELECT
    order_id || '-copy',
    customer_id,
    order_status,
    order_purchase_timestamp,
    99,
    current_timestamp(6)
FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-08-01 00:00:00';

SELECT partition, record_count, file_count
FROM lakehouse.demo."order_status_scd$partitions"
ORDER BY record_count DESC;
