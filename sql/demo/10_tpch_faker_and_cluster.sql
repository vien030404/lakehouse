-- ===========================================================================
-- 10. CATALOG SINH DỮ LIỆU (tpch, faker) VÀ QUAN SÁT CỤM
--
-- tpch và faker chỉ cần một file .properties, không cần hạ tầng riêng.
-- ===========================================================================

-- ===========================================================================
-- 10.1 TPCH - dữ liệu sinh sẵn theo scale factor
-- ===========================================================================
SHOW SCHEMAS FROM tpch;      -- tiny, sf1, sf100, sf1000, sf3000...
SHOW TABLES FROM tpch.sf1;

-- Kích thước thay đổi theo schema, dữ liệu sinh tại chỗ, không tốn đĩa.
SELECT 'tiny' AS sf, count(*) AS lineitem_rows FROM tpch.tiny.lineitem
UNION ALL
SELECT 'sf1', count(*) FROM tpch.sf1.lineitem;

-- TPC-H Q1 rút gọn: bài benchmark kinh điển, dùng để so cấu hình.
SELECT
    returnflag,
    linestatus,
    sum(quantity)                                   AS sum_qty,
    round(sum(extendedprice), 2)                    AS sum_base_price,
    round(sum(extendedprice * (1 - discount)), 2)   AS sum_disc_price,
    round(avg(discount), 4)                         AS avg_disc,
    count(*)                                        AS count_order
FROM tpch.sf1.lineitem
WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;

-- Trên cụm 2 worker này, sf1 mất khoảng 10-30s; sf100 trở lên vượt quá tài nguyên.


-- ===========================================================================
-- 10.2 FAKER - sinh dữ liệu giả theo schema tự khai
-- ===========================================================================
-- Khác tpch: schema do người dùng định nghĩa, cột nào cũng được.
--
-- Cú pháp: `generator` là một biểu thức Datafaker. Tham số bên trong dùng
-- nháy đơn, và vì biểu thức nằm trong chuỗi SQL nên nháy đơn được nhân đôi
-- thành ''. Dùng nháy kép sẽ báo "generator must be a valid Faker expression".
-- Cột kiểu số/thời gian thì không dùng generator mà dùng min / max.
DROP TABLE IF EXISTS faker.default.support_tickets;

CREATE TABLE faker.default.support_tickets (
    ticket_id        uuid,
    customer_id      varchar      NOT NULL WITH (generator = '#{Numerify ''cust_#####''}'),
    channel          varchar      NOT NULL WITH (generator = '#{Options.option ''email'',''chat'',''phone''}'),
    priority         varchar      NOT NULL WITH (generator = '#{Options.option ''low'',''medium'',''high'',''urgent''}'),
    subject          varchar      NOT NULL WITH (generator = '#{Lorem.sentence}'),
    agent_name       varchar      NOT NULL WITH (generator = '#{Name.fullName}'),
    created_at       timestamp(6) NOT NULL WITH (min = '2018-01-01 00:00:00.000000',
                                                 max = '2018-12-31 23:59:59.999999'),
    resolved_minutes integer      NOT NULL WITH (min = '1', max = '2880')
);

-- Mỗi lần SELECT sinh dữ liệu mới. LIMIT quyết định số dòng.
SELECT * FROM faker.default.support_tickets LIMIT 10;

-- Dùng để dựng nhanh một bảng thật trong Iceberg khi cần tập dữ liệu ngoài Olist:
CREATE SCHEMA IF NOT EXISTS lakehouse.demo;
DROP TABLE IF EXISTS lakehouse.demo.support_tickets;

CREATE TABLE lakehouse.demo.support_tickets AS
SELECT * FROM faker.default.support_tickets LIMIT 20000;

SELECT channel, priority, count(*) AS n
FROM lakehouse.demo.support_tickets
GROUP BY channel, priority
ORDER BY n DESC;

-- Ghép dữ liệu giả với dữ liệu thật (Olist) để dựng kịch bản thử nghiệm:
SELECT
    t.priority,
    count(*)                    AS tickets,
    round(avg(t.resolved_minutes), 1) AS avg_minutes
FROM lakehouse.demo.support_tickets t
GROUP BY t.priority
ORDER BY tickets DESC;


-- ===========================================================================
-- 10.3 Quan sát cụm qua catalog `system`
-- ===========================================================================
-- Node nào đang sống
SELECT node_id, http_uri, node_version, coordinator, state
FROM system.runtime.nodes;

-- Query đang chạy / vừa chạy
SELECT
    query_id,
    "user",
    state,
    date_diff('second', created, coalesce("end", current_timestamp)) AS seconds,
    substr(query, 1, 70) AS query
FROM system.runtime.queries
ORDER BY created DESC
LIMIT 20;

-- Task và các stage của một query cụ thể
-- SELECT * FROM system.runtime.tasks WHERE query_id = '<query_id>';

-- Thuộc tính bảng mà connector Iceberg hỗ trợ (partitioning, format, ...)
SELECT * FROM system.metadata.table_properties WHERE catalog_name = 'lakehouse';

-- Toàn bộ session property và giá trị hiện tại
SHOW SESSION;

-- Các connector đang nạp
SELECT * FROM system.metadata.catalogs;

-- Các bảng metadata mà catalog `system` cung cấp
SELECT table_name FROM system.information_schema.tables
WHERE table_schema = 'metadata' ORDER BY 1;

-- Thuộc tính cột mà connector hỗ trợ (vd: generator của faker)
SELECT * FROM system.metadata.column_properties WHERE catalog_name = 'faker';

-- Hàm vô hướng / tổng hợp có sẵn
SHOW FUNCTIONS LIKE 'approx%';


-- ===========================================================================
-- 10.4 JMX - số liệu vận hành JVM và Trino dưới dạng bảng
-- ===========================================================================
-- Memory pool của từng node (số byte còn trống / tối đa / đang giữ)
SELECT node, freebytes, maxbytes, reservedbytes
FROM jmx.current."trino.memory:name=general,type=memorypool";

-- Heap JVM từng node. Cột heapmemoryusage là varchar chứa cả cụm giá trị,
-- nên phải tách ra thay vì SELECT "heapmemoryusage.used".
SELECT node, heapmemoryusage, nonheapmemoryusage
FROM jmx.current."java.lang:type=memory";

-- Query đang chạy / đang xếp hàng (chỉ có ở coordinator)
SELECT node, runningqueries, queuedqueries
FROM jmx.current."trino.execution:name=querymanager";

-- Liệt kê mọi mbean (hơn 300 bảng)
SELECT table_name
FROM jmx.information_schema.tables
WHERE table_schema = 'current'
ORDER BY table_name;
