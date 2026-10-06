-- ===========================================================================
-- 09. FAULT-TOLERANT EXECUTION (FTE)
--
-- Ở chế độ mặc định (pipelined), mất một worker giữa chừng làm hỏng cả query.
-- Với FTE, Trino ghi kết quả trung gian ra exchange spooling (MinIO) và chỉ
-- chạy lại phần bị hỏng.
--
-- Đánh đổi: chậm hơn vì phải ghi/đọc S3 giữa các stage. Phù hợp cho ETL chạy
-- nền hơn là truy vấn tương tác.
--
-- Bật/tắt cho cả cụm qua TRINO_RETRY_POLICY trong .env hoặc:
--   .\scripts\set-retry-policy.ps1 TASK
-- Chỉ cho phiên hiện tại:
--   SET SESSION retry_policy = 'TASK';
-- Dữ liệu spool nằm tạm trên s3://exchange và được xoá khi query kết thúc.
--
-- fault-tolerant-execution-task-memory được hạ từ 5GB (mặc định) xuống 1GB
-- trong coordinator/config.properties: mặc định lớn hơn
-- query.max-memory-per-node=2GB của worker nên scheduler không xếp được task.
--
-- Cấu hình spooling: trino/{coordinator,worker}/exchange-manager.properties
-- (bucket s3://exchange do container minio-init tạo khi khởi động).
-- ===========================================================================

-- --- 9.1 Kiểm tra FTE đang bật hay tắt -----------------------------------
SHOW SESSION LIKE 'retry%';
-- retry_policy        : NONE hay TASK
-- retry_initial_delay / retry_max_delay / retry_attempts
-- task_retry_attempts_per_task


-- --- 9.2 Chạy một truy vấn nặng dưới chế độ FTE --------------------------
-- Với retry-policy=TASK, mỗi stage ghi output ra MinIO trước khi stage sau đọc.
SELECT
    c.mktsegment,
    count(DISTINCT o.orderkey)                        AS orders,
    round(sum(l.extendedprice * (1 - l.discount)), 2) AS revenue
FROM tpch.sf1.lineitem l
JOIN tpch.sf1.orders   o ON l.orderkey = o.orderkey
JOIN tpch.sf1.customer c ON o.custkey  = c.custkey
WHERE o.orderdate >= DATE '1994-01-01'
  AND o.orderdate <  DATE '1995-01-01'
GROUP BY c.mktsegment
ORDER BY revenue DESC;


-- --- 9.3 Thử nghiệm: dừng một worker giữa chừng ---------------------------
--
-- Mở terminal thứ hai và chạy:
--
--     docker stop trinodemo-trino-worker-2
--
-- ngay khi truy vấn ở 9.2 vừa bắt đầu.
--
--   retry-policy=NONE  -> query FAIL:
--       "Encountered too many errors talking to a worker node"
--   retry-policy=TASK  -> query vẫn hoàn thành, kết quả giống hệt.
--
-- Kết quả đo trên cụm này, query GROUP BY 48 triệu dòng:
--
--   08:23:04  query bắt đầu
--   08:24:10  docker stop trinodemo-trino-worker-2
--   08:24:11  node chuyển sang inactive
--             -> query vẫn RUNNING, failedTasks=1
--   08:26:37  docker start trinodemo-trino-worker-2
--             -> runningTasks 0 -> 1 -> 4, tiến trình chạy tiếp
--   08:30:xx  FINISHED, 7.31 phút, 12/12 task, kết quả 48.009.720
--             -> trùng khớp lần chạy không dừng worker (3.66 phút)
--   Sau đó   s3://exchange tự dọn về 0 object
--
-- Quan sát: với cụm 2 worker, task của worker-2 không được giao lại ngay cho
-- worker-1. Số task đang chạy về 0 trong hơn 2 phút cho tới khi worker-2 quay
-- lại, vì với task-memory=1GB và query.max-memory-per-node=2GB, scheduler
-- không còn chỗ trên worker-1 (đang giữ ~2,5 GiB). Query chờ tài nguyên thay
-- vì thất bại.
--
-- FTE đảm bảo query sống sót qua sự cố, không đảm bảo tốc độ khi thiếu node:
-- cụm nhiều worker giao lại task ngay, cụm 2 worker thì chờ node quay lại.
--
-- Bật lại worker:
--     docker start trinodemo-trino-worker-2
--
-- Muốn query đủ lâu để kịp thao tác, dùng tpch.sf1 (~30-60s) hoặc query
-- nhân bản như ở trên (~4-7 phút).


-- --- 9.4 Xem dữ liệu spooling thật sự nằm ở đâu --------------------------
-- MinIO console: http://localhost:9001 (minioadmin / minioadmin123)
--   -> bucket `exchange` -> thư mục theo query id, chỉ tồn tại trong lúc chạy.
-- Hoặc từ dòng lệnh (image minio lấy từ quay.io):
--   docker run --rm --network trinonet --entrypoint sh \
--     quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z -c \
--     "mc alias set m http://minio:9000 minioadmin minioadmin123 && mc ls -r m/exchange"


-- --- 9.5 Các tham số tinh chỉnh -------------------------------------------
SHOW SESSION LIKE 'fault_tolerant%';
-- fault_tolerant_execution_max_task_split_count
-- fault_tolerant_execution_target_task_input_size
-- fault_tolerant_execution_target_task_split_count
--
-- Ngoài ra ở config.properties:
--   query-retry-attempts, task-retry-attempts-per-task
--   exchange.compression-codec (nén dữ liệu spool để đỡ I/O)


-- --- 9.6 FTE theo workload --------------------------------------------------
-- Exchange manager phải được cấu hình ở cấp cụm, còn retry_policy chọn được
-- theo phiên. Trong production, cách phổ biến là tách cụm: một cụm ETL bật
-- FTE, một cụm tương tác tắt FTE, định tuyến qua gateway hoặc connection
-- string của client.


-- --- 9.7 Tắt FTE cho cả cụm ----------------------------------------------
--   .env: TRINO_RETRY_POLICY=NONE
--   docker compose --profile core up -d --force-recreate \
--       trino-coordinator trino-worker-1 trino-worker-2
-- FTE thêm chi phí I/O cho mọi query, kể cả truy vấn tương tác ngắn.
