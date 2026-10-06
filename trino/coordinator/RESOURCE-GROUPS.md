# Resource groups

`resource-groups.json` không cho phép chú thích (Jackson từ chối mọi khoá lạ,
giống `trino/security/rules.json`), nên phần giải thích nằm ở file này.

## Vì sao cần, khi đã có `query.max-memory`

| | Giới hạn cái gì | Vượt thì sao |
|---|---|---|
| `query.max-memory` | **một** query | query bị **huỷ** |
| resource group | **cả nhóm** query cộng lại | query bị **xếp hàng** |

Rủi ro với một cụm nhỏ thường không phải một query lớn mà là nhiều query vừa
chạy cùng lúc: dbt build, dashboard và bảo trì bảng chồng lên nhau.
`query.max-memory` không ngăn được tình huống đó; resource group thì có.

## Cây nhóm

```
global                      90% RAM cụm, 6 query song song, 50 chờ
├── etl          weight 5   70%, 3 song song   ← dbt (Airflow và chạy tay)
├── bi           weight 3   30%, 2 song song   ← Superset
├── adhoc        weight 2   30%, 2 song song   ← CLI, DBeaver, mặc định
└── maintenance  weight 1   60%, 1 song song   ← bảo trì Iceberg từ Airflow
```

Tổng `softMemoryLimit` vượt 100% là có chủ đích. Đây là ngưỡng mềm: nhóm chỉ
bị ngừng nhận query mới khi *thực tế* đang dùng quá mức, không phải phần giữ
trước. Nếu tổng đúng bằng 100%, lúc cụm rảnh một nhóm vẫn không dùng được
phần tài nguyên đang trống.

`schedulingWeight` chỉ có tác dụng vì nhóm cha khai
`"schedulingPolicy": "weighted_fair"`. Khi cả bốn nhóm cùng có query chờ,
`etl` được chia khe nhiều gấp 5 lần `maintenance`.

`hardConcurrencyLimit: 3` cho `etl` lớn hơn `threads: 2` của dbt, để các model
độc lập chạy song song và vẫn còn chỗ cho một lần chạy tay.

## Chọn nhóm bằng gì

Selector xét **theo thứ tự, trúng cái đầu tiên là dừng**.

```
1. clientTags chứa "maintenance"     → global.maintenance
2. source khớp  .*dbt.*              → global.etl
3. source khớp  .*(superset|python-client).*  → global.bi
4. (không điều kiện)                 → global.adhoc
```

Giá trị `source` mà các client gửi lên (xem trong `system.runtime.queries`):

```
dbt-trino-1.10.4
trino-cli
DBeaver 26.1.0 - Main
DBeaver 26.1.0 - Metadata
```

`clientTags` đứng trước `source` để lệnh bảo trì từ bất kỳ client nào (Airflow,
CLI) đều vào `maintenance` khi gắn thẻ, thay vì rơi vào nhóm theo `source`.

Selector cuối (không điều kiện) là bắt buộc: query không khớp selector nào bị
từ chối ngay:

```
Query was not selected by any resource group
```

## Dùng thử

Gắn thẻ để đẩy query vào nhóm `maintenance`:

```bash
docker compose run --rm trino-cli --client-tags maintenance \
  -f /sql/maintenance/iceberg_maintenance.sql
```

Xem query nào đang ở nhóm nào:

```sql
SELECT query_id, "user", source, resource_group_id, state
FROM system.runtime.queries
ORDER BY created DESC LIMIT 20;
```

`resource_group_id` là mảng: `[global, etl]`.

## Sửa file có cần restart không

Có. Trình quản lý dạng `file` chỉ đọc cấu hình lúc khởi động. Nạp lại không
cần restart đòi hỏi dạng `db` (`resource-groups.configuration-manager=db`),
khi đó `resource-groups.refresh-interval` mới có tác dụng và cấu hình được lưu
trong bảng của một database quan hệ.
