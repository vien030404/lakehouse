-- ===========================================================================
-- 08. BẢO MẬT: FILE-BASED ACCESS CONTROL, COLUMN MASK, ROW FILTER
--
-- Luật nằm ở trino/security/rules.json, nạp qua:
--     access-control.name=file
--     security.config-file=/etc/trino/security/rules.json
--
-- Môi trường local không bật authentication (HTTP), nên danh tính lấy từ
-- --user của client. Đủ để minh hoạ cơ chế phân quyền; production cần TLS và
-- authentication (password, OAuth2, Kerberos) trước khi ACL có ý nghĩa.
--
-- Cách chạy: mở hai phiên CLI để so sánh cạnh nhau.
--
--   Phiên A (toàn quyền):
--     docker exec -it trinodemo-trino-coordinator trino --user admin
--
--   Phiên B (bị hạn chế):
--     docker exec -it trinodemo-trino-coordinator trino --user analyst
-- ===========================================================================


-- ===========================================================================
-- PHẦN A - chạy bằng --user admin
-- ===========================================================================

SELECT current_user;                     -- 'admin'

-- Thấy hết mọi catalog
SHOW CATALOGS;

-- Đọc được bronze (analyst thì không)
SELECT count(*) FROM lakehouse.bronze.orders;

-- Đọc customer_id nguyên vẹn
SELECT order_id, customer_id
FROM lakehouse.silver.silver_order_facts
LIMIT 5;

-- Thấy đủ mọi bang
SELECT customer_state, count(*) AS n
FROM lakehouse.gold.gold_revenue_by_state
GROUP BY customer_state
ORDER BY n DESC;

-- Ghi được
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;


-- ===========================================================================
-- PHẦN B - chạy bằng --user analyst
-- ===========================================================================

SELECT current_user;                     -- 'analyst'

-- B.1 Catalog bị giấu: faker, jmx không xuất hiện (analyst chỉ thấy 3 catalog)
SHOW CATALOGS;

-- B.2 Bronze bị chặn hoàn toàn
--     -> Query failed: Access Denied: Cannot select from table lakehouse.bronze.orders
SELECT count(*) FROM lakehouse.bronze.orders;

-- B.3 COLUMN MASK: customer_id bị che thành 'abcd****MASKED****'
--     Không phải view hay bảng khác: Trino viết lại biểu thức cột ngay trong
--     kế hoạch truy vấn trên chính bảng đó.
SELECT order_id, customer_id, item_revenue
FROM lakehouse.silver.silver_order_facts
LIMIT 5;

-- GROUP BY / JOIN trên cột đã mask cũng dùng giá trị đã mask, nên không thể
-- tái tạo giá trị gốc qua truy vấn.
SELECT customer_id, count(*) AS n
FROM lakehouse.silver.silver_order_facts
GROUP BY customer_id
LIMIT 5;

-- B.4 Cột bị chặn hẳn (allow: false)
--     -> Access Denied: Cannot select from columns [customer_unique_id]
--     (chỉ chạy được nếu bảng có cột này)
-- SELECT customer_unique_id FROM lakehouse.silver.silver_order_facts LIMIT 1;

-- B.5 ROW FILTER: chỉ thấy bang SP, không thấy bang nào khác
SELECT customer_state, count(*) AS n
FROM lakehouse.gold.gold_revenue_by_state
GROUP BY customer_state;

-- Truy vấn bang khác trả về rỗng, không báo lỗi: filter được AND vào mọi
-- truy vấn trên bảng.
SELECT count(*) AS n_rj
FROM lakehouse.gold.gold_revenue_by_state
WHERE customer_state = 'RJ';

-- B.6 Không được ghi
--     -> Access Denied: Cannot create schema lakehouse.hacked
CREATE SCHEMA lakehouse.hacked;

-- B.7 Không được xem catalog jmx - số liệu vận hành của cụm chỉ dành cho admin
--     (rules.json chỉ mở system, lakehouse, tpch cho analyst)
SHOW SCHEMAS FROM jmx;


-- ===========================================================================
-- PHẦN C - kiểm chứng và vận hành
-- ===========================================================================

-- C.1 Ai đang chạy gì (chạy bằng admin; analyst chỉ thấy query của chính mình)
SELECT query_id, "user", state, substr(query, 1, 60) AS query
FROM system.runtime.queries
ORDER BY created DESC
LIMIT 20;

-- C.2 Sửa luật rồi áp dụng
--     Sửa trino/security/rules.json trên host. Coordinator nạp lại luật sau
--     tối đa 30 giây (security.refresh-period=30s trong
--     trino/coordinator/access-control.properties), không cần restart.

-- C.3 Hướng mở rộng cho môi trường production
--     - TLS + http-server.authentication.type=PASSWORD với file password
--     - hoặc OAuth2 / Kerberos / JWT
--     - access-control.name=opa cho chính sách viết bằng Rego
--     - Apache Ranger nếu cần giao diện quản trị tập trung
