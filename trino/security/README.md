# Giải thích `rules.json`

`rules.json` không thể chứa chú thích, kể cả dạng `"_comment": "..."`. Trino
parse file bằng Jackson với `FAIL_ON_UNKNOWN_PROPERTIES`, nên một khoá lạ trong
bất kỳ rule nào làm coordinator không khởi động được:

```
UnrecognizedPropertyException: Unrecognized field "_comment"
(class io.trino.plugin.base.security.CatalogTableAccessControlRule)
```

Vì vậy phần giải thích nằm ở file này.

## Nạp từ đâu

`trino/coordinator/access-control.properties`:

```properties
access-control.name=file
security.config-file=/etc/trino/security/rules.json
security.refresh-period=30s
```

Từ Trino 4xx, hai dòng đầu phải nằm ở file riêng này; khai trong
`config.properties` sẽ báo `Configuration property 'access-control.name' was not used`.

Với `security.refresh-period=30s`, thay đổi trong `rules.json` có hiệu lực sau
tối đa 30 giây, không cần khởi động lại coordinator.

## Danh tính người dùng đến từ đâu

Môi trường local không bật authentication (HTTP), nên Trino lấy danh tính
trực tiếp từ tham số `--user` của client:

```bash
docker exec -it trinodemo-trino-coordinator trino --user admin
docker exec -it trinodemo-trino-coordinator trino --user analyst
```

Cấu hình này nhằm minh hoạ cơ chế phân quyền (catalog, column mask, row
filter), không nhằm bảo vệ dữ liệu: client có thể tự khai bất kỳ user nào. Môi
trường production cần TLS và authentication (`PASSWORD`, OAuth2, Kerberos hoặc
JWT) trước, khi đó các luật dưới đây mới có ý nghĩa bảo mật.

## Từng section

### `catalogs`
Ai thấy được catalog nào. `admin` thấy hết; `analyst` chỉ thấy `system`,
`lakehouse`, `tpch` ở chế độ read-only. `faker`, `jmx` bị giấu hẳn khỏi
`SHOW CATALOGS`.

Rule cuối `{"user": ".*", "catalog": ".*", "allow": "none"}` là lưới chặn:
user lạ không có quyền gì.

### `schemas`
Quyết định quyền **sở hữu** schema (tạo/xoá/đổi tên). Không ảnh hưởng SELECT.

### `tables`
Luật được xét từ trên xuống và luật khớp đầu tiên được áp dụng, nên thứ tự
quan trọng:

| # | Đối tượng | Hiệu lực |
|---|---|---|
| 1 | `admin` | mọi quyền, mọi bảng |
| 2 | `analyst` trên `gold.gold_revenue_by_state` | SELECT + **row filter** `customer_state = 'SP'` |
| 3 | `analyst` trên `gold.*` còn lại | SELECT đầy đủ |
| 4 | `analyst` trên `silver.*` | SELECT + **column mask** trên `customer_id`, chặn hẳn `customer_unique_id` |
| 5 | `analyst` trên `bronze.*` | `privileges: []` = cấm |
| 6-7 | `analyst` trên `tpch`, `system` | SELECT |
| 8 | mọi user khác | cấm |

Rule 2 phải đứng trước rule 3; nếu không, `gold.*` khớp trước và row filter
không bao giờ được áp dụng.

**Column mask** là biểu thức SQL, được Trino chèn thẳng vào kế hoạch truy vấn
thay cho cột gốc:

```json
{ "name": "customer_id", "mask": "concat(substr(customer_id, 1, 4), '****MASKED****')" }
```

Vì cột được thay ngay trong kế hoạch truy vấn, mọi `GROUP BY`, `JOIN` hay
subquery trên cột đó cũng dùng giá trị đã mask; không có cách lấy lại giá trị
gốc qua truy vấn.

**Row filter** (`"filter"`) được AND vào mọi truy vấn chạm bảng đó. Người dùng
hỏi bang khác sẽ nhận kết quả rỗng chứ không phải lỗi.

### `functions` và `procedures`
Hai section này có hành vi mặc định dễ gây bất ngờ:

> Nếu **không khai** section, chỉ các hàm/thủ tục trong `system.builtin` chạy được.

Nghĩa là hàm thuộc catalog, kể cả table function như
`lakehouse.system.table_changes(...)`, bị chặn với mọi user không được khai.
Ở đây chỉ `admin` được phép; `analyst` gọi `table_changes` nhận
`Access Denied: Cannot execute function lakehouse.system.table_changes`.

Table procedure (`ALTER TABLE ... EXECUTE optimize / expire_snapshots /
remove_orphan_files`) không được phân quyền riêng trong file-based access
control; quyền thực tế đến từ privileges trên bảng. Vì vậy DAG Airflow chạy
bảo trì bằng user `admin`.

### `system_session_properties` / `catalog_session_properties`
`analyst` chỉ được chỉnh ba session property an toàn. Một số property khác (ví
dụ `resource_overcommit`) cho phép một query vượt hạn mức bộ nhớ của cụm.

### `queries`
`admin` xem và kill được query của mọi người; user khác chỉ chạy được query của
mình (Trino luôn cho phép xem/kill query do chính mình tạo, không cần rule).

### `system_information`
Quyền đọc/ghi thông tin hệ thống qua REST API (`/v1/info`, `/v1/status`).

## Kiểm chứng

`sql/demo/08_security.sql` chạy cùng các câu truy vấn trong hai phiên CLI
(`--user admin` và `--user analyst`) để so sánh trực tiếp.
