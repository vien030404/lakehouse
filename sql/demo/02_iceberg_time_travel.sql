-- ===========================================================================
-- 02. ICEBERG: METADATA, SNAPSHOT VÀ TIME TRAVEL
--
-- Mỗi lần ghi vào bảng Iceberg tạo một snapshot mới; dữ liệu cũ không bị ghi
-- đè, nên đọc lại trạng thái quá khứ chỉ là chọn snapshot.
--
-- Lưu ý: mục 2.2 xoá thật một phần bronze.orders và 2.6 đổi schema tạm thời.
-- Mục 2.5 cho cách rollback về snapshot trước khi xoá.
-- ===========================================================================

-- --- 2.1 Các bảng metadata Iceberg mà Trino cung cấp ---------------------
-- $snapshots : lịch sử các phiên bản của bảng
-- Dùng element_at() thay vì summary['key']: mỗi loại thao tác ghi một bộ key
-- khác nhau vào summary (snapshot của CREATE TABLE hay optimize có thể thiếu
-- 'added-records'). map['key'] báo lỗi "Key not present in map" khi thiếu
-- khoá, còn element_at() trả về NULL.
SELECT
    snapshot_id,
    parent_id,
    operation,
    committed_at,
    element_at(summary, 'added-records')    AS added_records,
    element_at(summary, 'total-records')    AS total_records,
    element_at(summary, 'total-data-files') AS data_files
FROM lakehouse.bronze."orders$snapshots"
ORDER BY committed_at DESC;

-- $history : snapshot nào đang là "current" tại thời điểm nào
SELECT * FROM lakehouse.bronze."orders$history" ORDER BY made_current_at DESC;

-- $partitions : số dòng, số file mỗi partition
--               (cho thấy hidden partitioning day(...) hoạt động)
SELECT
    partition,
    record_count,
    file_count,
    total_size
FROM lakehouse.bronze."orders$partitions"
ORDER BY record_count DESC
LIMIT 10;

-- $files : chi tiết từng file Parquet, kèm thống kê min/max theo cột
SELECT
    file_path,
    record_count,
    file_size_in_bytes
FROM lakehouse.bronze."orders$files"
ORDER BY file_size_in_bytes DESC
LIMIT 10;

-- $manifests / $refs : lớp metadata trung gian và các nhánh/tag
SELECT * FROM lakehouse.bronze."orders$manifests";
SELECT * FROM lakehouse.bronze."orders$refs";


-- --- 2.2 Tạo một thay đổi để có cái mà time travel -----------------------
SELECT count(*) AS truoc_khi_xoa FROM lakehouse.bronze.orders;

DELETE FROM lakehouse.bronze.orders
WHERE order_status = 'unavailable';

SELECT count(*) AS sau_khi_xoa FROM lakehouse.bronze.orders;


-- --- 2.3 Time travel theo SNAPSHOT ID ------------------------------------
-- Lấy snapshot_id ngay trước lần DELETE ở trên rồi thay vào câu dưới.
SELECT snapshot_id, operation, committed_at
FROM lakehouse.bronze."orders$snapshots"
ORDER BY committed_at DESC
LIMIT 3;

-- SELECT count(*) FROM lakehouse.bronze.orders FOR VERSION AS OF <snapshot_id>;


-- --- 2.4 Time travel theo THỜI ĐIỂM --------------------------------------
-- Trạng thái bảng cách đây 10 phút (trước khi DELETE):
SELECT count(*) AS cach_day_10_phut
FROM lakehouse.bronze.orders
FOR TIMESTAMP AS OF current_timestamp - INTERVAL '10' MINUTE;

-- So sánh trực tiếp hiện tại với quá khứ trong một câu:
WITH now_state AS (
    SELECT order_status, count(*) AS n
    FROM lakehouse.bronze.orders
    GROUP BY order_status
),
past_state AS (
    SELECT order_status, count(*) AS n
    FROM lakehouse.bronze.orders FOR TIMESTAMP AS OF current_timestamp - INTERVAL '10' MINUTE
    GROUP BY order_status
)
SELECT
    COALESCE(n.order_status, p.order_status) AS order_status,
    p.n AS truoc,
    n.n AS sau,
    COALESCE(n.n, 0) - COALESCE(p.n, 0)      AS chenh_lech
FROM now_state n
FULL OUTER JOIN past_state p ON n.order_status = p.order_status
ORDER BY chenh_lech;


-- --- 2.5 Quay ngược bảng về snapshot cũ ----------------------------------
-- Thay <snapshot_id> bằng id lấy ở 2.3. Đây là rollback thật của bảng, không phải view.
-- CALL lakehouse.system.rollback_to_snapshot('bronze', 'orders', <snapshot_id>);

-- Sau khi rollback, kiểm tra lại:
-- SELECT count(*) FROM lakehouse.bronze.orders;


-- --- 2.6 Schema evolution: đổi bảng mà không ghi lại dữ liệu -------------
ALTER TABLE lakehouse.bronze.orders ADD COLUMN ingestion_note varchar;
DESCRIBE lakehouse.bronze.orders;

ALTER TABLE lakehouse.bronze.orders RENAME COLUMN ingestion_note TO load_note;
ALTER TABLE lakehouse.bronze.orders DROP COLUMN load_note;

-- Đổi partition spec cũng không cần viết lại dữ liệu cũ (partition evolution):
-- ALTER TABLE lakehouse.bronze.orders
--   SET PROPERTIES partitioning = ARRAY['month(order_purchase_timestamp)'];
