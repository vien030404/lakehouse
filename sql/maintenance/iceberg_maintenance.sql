-- ===========================================================================
-- Bảo trì Iceberg thủ công cho các bảng silver/gold do dbt dựng.
-- (Bảng stream được bảo trì tự động: orders_live trong job Spark Streaming,
-- bronze.*_events và silver_order_facts trong DAG Airflow olist_daily.)
--
--   docker compose run --rm trino-cli --client-tags maintenance \
--       -f /sql/maintenance/iceberg_maintenance.sql
--
-- Thứ tự các stage theo khuyến nghị của Trino và không được đảo:
--
--   1. ANALYZE              cập nhật thống kê cho CBO, để các bước sau lập
--                           kế hoạch tốt hơn
--   2. optimize             gộp file nhỏ + gộp delete file vào data file
--   3. expire_snapshots     bỏ snapshot cũ -> metadata gọn
--                           (MẤT khả năng time travel về trước mốc này)
--   4. remove_orphan_files  xoá file không snapshot nào còn trỏ tới
--
-- Stage 4 phải chạy sau stage 3: nếu xoá file mồ côi trước khi expire, file
-- còn được snapshot cũ tham chiếu có thể bị coi là mồ côi và bị xoá nhầm.
--
-- Chạy theo từng stage cho cả 5 bảng thay vì từng bảng đủ 4 bước: mỗi stage
-- hoàn tất mới sang stage sau, nên nếu optimize lỗi giữa chừng thì chưa bảng
-- nào bị expire_snapshots tác động.
--
-- Retention: '7d' giữ được time travel một tuần. Catalog cho phép tối thiểu
-- 1h (iceberg.expire-snapshots.min-retention) để dọn bảng stream thường xuyên.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- STAGE 1: ANALYZE - thống kê cho CBO
-- ---------------------------------------------------------------------------
ANALYZE lakehouse.silver.silver_orders;
ANALYZE lakehouse.silver.silver_order_facts;
ANALYZE lakehouse.gold.gold_revenue_by_state;
ANALYZE lakehouse.gold.gold_revenue_by_category;
ANALYZE lakehouse.gold.gold_seller_scorecard;


-- ---------------------------------------------------------------------------
-- STAGE 2: optimize - gộp file nhỏ
-- Gộp các file nhỏ hơn 64MB. Ngưỡng càng lớn, mỗi lần chạy càng phải đọc và
-- ghi lại nhiều dữ liệu.
-- ---------------------------------------------------------------------------
ALTER TABLE lakehouse.silver.silver_orders            EXECUTE optimize(file_size_threshold => '64MB');
ALTER TABLE lakehouse.silver.silver_order_facts       EXECUTE optimize(file_size_threshold => '64MB');
ALTER TABLE lakehouse.gold.gold_revenue_by_state      EXECUTE optimize(file_size_threshold => '64MB');
ALTER TABLE lakehouse.gold.gold_revenue_by_category   EXECUTE optimize(file_size_threshold => '64MB');
ALTER TABLE lakehouse.gold.gold_seller_scorecard      EXECUTE optimize(file_size_threshold => '64MB');


-- ---------------------------------------------------------------------------
-- STAGE 3: expire_snapshots - dọn lịch sử
-- Sau stage này không thể time travel về trước mốc retention.
-- ---------------------------------------------------------------------------
ALTER TABLE lakehouse.silver.silver_orders            EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lakehouse.silver.silver_order_facts       EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_revenue_by_state      EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_revenue_by_category   EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_seller_scorecard      EXECUTE expire_snapshots(retention_threshold => '7d');


-- ---------------------------------------------------------------------------
-- STAGE 4: remove_orphan_files - xoá file không còn snapshot nào tham chiếu
-- ---------------------------------------------------------------------------
ALTER TABLE lakehouse.silver.silver_orders            EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE lakehouse.silver.silver_order_facts       EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_revenue_by_state      EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_revenue_by_category   EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE lakehouse.gold.gold_seller_scorecard      EXECUTE remove_orphan_files(retention_threshold => '7d');


-- ---------------------------------------------------------------------------
-- Kiểm tra sau bảo trì
-- ---------------------------------------------------------------------------
SELECT 'silver_orders'          AS bang,
       (SELECT count(*) FROM lakehouse.silver."silver_orders$files")     AS data_files,
       (SELECT count(*) FROM lakehouse.silver."silver_orders$snapshots") AS snapshots
UNION ALL
SELECT 'silver_order_facts',
       (SELECT count(*) FROM lakehouse.silver."silver_order_facts$files"),
       (SELECT count(*) FROM lakehouse.silver."silver_order_facts$snapshots")
UNION ALL
SELECT 'gold_revenue_by_state',
       (SELECT count(*) FROM lakehouse.gold."gold_revenue_by_state$files"),
       (SELECT count(*) FROM lakehouse.gold."gold_revenue_by_state$snapshots")
UNION ALL
SELECT 'gold_revenue_by_category',
       (SELECT count(*) FROM lakehouse.gold."gold_revenue_by_category$files"),
       (SELECT count(*) FROM lakehouse.gold."gold_revenue_by_category$snapshots")
UNION ALL
SELECT 'gold_seller_scorecard',
       (SELECT count(*) FROM lakehouse.gold."gold_seller_scorecard$files"),
       (SELECT count(*) FROM lakehouse.gold."gold_seller_scorecard$snapshots")
ORDER BY 1;
