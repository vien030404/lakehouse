-- ===========================================================================
-- 07. BẢO TRÌ BẢNG ICEBERG
--
-- Thứ tự các bước (giống sql/maintenance/iceberg_maintenance.sql):
--
--   1. ANALYZE              cập nhật thống kê cho CBO
--   2. optimize             gộp file nhỏ + gộp delete file vào data file
--   3. expire_snapshots     bỏ snapshot cũ -> metadata gọn (mất time travel về trước mốc)
--   4. remove_orphan_files  xoá file không snapshot nào còn trỏ tới
--
-- Bước 4 phải sau bước 3: nếu xoá orphan trước khi expire, file còn được
-- snapshot cũ tham chiếu có thể bị coi là mồ côi.
--
-- File này làm việc trên bảng nháp lakehouse.demo.orders_maint, không tác động
-- tới bronze.orders mà pipeline đang dùng.
--
-- ---------------------------------------------------------------------------
-- Giới hạn của optimize: độ mịn partition
--
-- bronze.orders partition theo day(order_purchase_timestamp). Olist trải 634
-- ngày, nên bảng có 634 partition và 634 file, trung bình ~10KB/file. Chạy
-- optimize trên bảng này cho kết quả:
--
--     rewritten_data_files_count = 0
--     added_data_files_count     = 0
--
-- optimize chỉ gộp file trong cùng một partition; mỗi partition đã có đúng một
-- file thì không còn gì để gộp. Cách xử lý là đổi partition spec thô hơn:
--
--     ALTER TABLE lakehouse.bronze.orders
--       SET PROPERTIES partitioning = ARRAY['month(order_purchase_timestamp)'];
--
-- Đây là lý do các model silver của dbt partition theo tháng thay vì ngày.
-- Độ mịn partition là quyết định thiết kế bảng; bảo trì không bù được nếu
-- chọn quá mịn so với khối lượng dữ liệu.
-- ---------------------------------------------------------------------------
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS lakehouse.demo;
DROP TABLE IF EXISTS lakehouse.demo.orders_maint;

-- Chỉ lấy quý 1/2018 thay vì cả bảng: một câu INSERT ghi vào hơn 100
-- partition cùng lúc vượt giới hạn writer của Trino:
--     ICEBERG_TOO_MANY_OPEN_PARTITIONS:
--     Exceeded limit of 100 open writers for partitions: 105
-- Cửa sổ 90 ngày nằm dưới giới hạn 100 và đủ nhỏ để chạy nhanh. (Spark nạp
-- bronze.orders không gặp giới hạn này vì writer của Spark hoạt động khác.)
CREATE TABLE lakehouse.demo.orders_maint
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(order_purchase_timestamp)']
) AS
SELECT * FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
  AND order_purchase_timestamp <  TIMESTAMP '2018-04-01 00:00:00';


-- --- 7.0 Tái hiện "small file problem" ----------------------------------
-- Mỗi INSERT là một snapshot mới và ít nhất một file mới cho mỗi partition
-- được ghi. Nhiều batch nhỏ là nguyên nhân phổ biến khiến bảng bị phân mảnh.
-- Giữ nguyên cửa sổ quý 1 để không vượt trần 100 writer.
INSERT INTO lakehouse.demo.orders_maint
SELECT * FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
  AND order_purchase_timestamp <  TIMESTAMP '2018-04-01 00:00:00' LIMIT 200;
INSERT INTO lakehouse.demo.orders_maint
SELECT * FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
  AND order_purchase_timestamp <  TIMESTAMP '2018-04-01 00:00:00' LIMIT 200;
INSERT INTO lakehouse.demo.orders_maint
SELECT * FROM lakehouse.bronze.orders
WHERE order_purchase_timestamp >= TIMESTAMP '2018-01-01 00:00:00'
  AND order_purchase_timestamp <  TIMESTAMP '2018-04-01 00:00:00' LIMIT 200;

-- Thêm vài DML để sinh delete file
UPDATE lakehouse.demo.orders_maint
SET order_status = 'archived'
WHERE order_status = 'canceled';

DELETE FROM lakehouse.demo.orders_maint
WHERE order_status = 'unavailable';


-- Đo trước khi bảo trì
SELECT
    count(*)                                      AS n_files,
    sum(record_count)                             AS records,
    round(avg(file_size_in_bytes) / 1024.0, 1)    AS avg_kb,
    round(sum(file_size_in_bytes) / 1048576.0, 1) AS total_mb
FROM lakehouse.demo."orders_maint$files";

SELECT content, count(*) AS n_files
FROM lakehouse.demo."orders_maint$files"
GROUP BY content;   -- content = 0 data, 1 position-delete, 2 equality-delete

SELECT count(*) AS n_snapshots FROM lakehouse.demo."orders_maint$snapshots";


-- --- 7.1 ANALYZE ----------------------------------------------------------
ANALYZE lakehouse.demo.orders_maint;
SHOW STATS FOR lakehouse.demo.orders_maint;


-- --- 7.2 optimize: gộp file nhỏ ------------------------------------------
-- Chỉ gộp các file nhỏ hơn ngưỡng. Ngưỡng càng lớn, mỗi lần chạy càng phải
-- đọc và ghi lại nhiều dữ liệu.
ALTER TABLE lakehouse.demo.orders_maint
    EXECUTE optimize(file_size_threshold => '64MB');

SELECT
    count(*)                                   AS n_files_sau_optimize,
    round(avg(file_size_in_bytes) / 1024.0, 1) AS avg_kb
FROM lakehouse.demo."orders_maint$files";

-- Chỉ tối ưu một phần bảng - rẻ hơn nhiều, phù hợp job hằng ngày (DAG Airflow
-- dùng cách này cho các partition đã đóng của bronze.*_events):
ALTER TABLE lakehouse.demo.orders_maint
    EXECUTE optimize(file_size_threshold => '64MB')
    WHERE order_purchase_timestamp >= TIMESTAMP '2018-03-01 00:00:00';

-- Gộp riêng lớp metadata manifest (rẻ, chạy thường xuyên được):
ALTER TABLE lakehouse.demo.orders_maint EXECUTE optimize_manifests;


-- --- 7.3 expire_snapshots: dọn lịch sử -----------------------------------
-- Sau lệnh này không thể time travel về trước mốc retention. Catalog cho
-- phép tối thiểu 1h (iceberg.expire-snapshots.min-retention).
SELECT snapshot_id, operation, committed_at
FROM lakehouse.demo."orders_maint$snapshots"
ORDER BY committed_at;

ALTER TABLE lakehouse.demo.orders_maint
    EXECUTE expire_snapshots(retention_threshold => '1h');

SELECT count(*) AS n_snapshots_con_lai
FROM lakehouse.demo."orders_maint$snapshots";


-- --- 7.4 remove_orphan_files: dọn file mồ côi ----------------------------
-- File mồ côi xuất hiện khi một lần ghi thất bại giữa chừng, hoặc còn sót
-- lại sau expire.
ALTER TABLE lakehouse.demo.orders_maint
    EXECUTE remove_orphan_files(retention_threshold => '1h');


-- --- 7.5 drop_extended_stats: khi thống kê cũ làm CBO chọn sai -----------
-- ALTER TABLE lakehouse.demo.orders_maint EXECUTE drop_extended_stats;
-- ANALYZE lakehouse.demo.orders_maint;


-- --- 7.6 Toàn cảnh sau bảo trì --------------------------------------------
SELECT
    'orders_maint' AS table_name,
    (SELECT count(*) FROM lakehouse.demo."orders_maint$files")     AS data_files,
    (SELECT count(*) FROM lakehouse.demo."orders_maint$snapshots") AS snapshots,
    (SELECT count(*) FROM lakehouse.demo."orders_maint$manifests") AS manifests,
    (SELECT round(sum(file_size_in_bytes) / 1048576.0, 1)
     FROM lakehouse.demo."orders_maint$files")                     AS total_mb;

SHOW CREATE TABLE lakehouse.demo.orders_maint;


-- --- 7.7 Bản chạy được cho toàn bộ silver/gold ----------------------------
-- File này minh hoạ trên một bảng nháp. Bản áp dụng cho 5 bảng silver/gold
-- nằm ở sql/maintenance/iceberg_maintenance.sql:
--
--   docker compose run --rm trino-cli -f /sql/maintenance/iceberg_maintenance.sql
