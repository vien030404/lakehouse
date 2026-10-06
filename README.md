# TrinoDemo — Lakehouse pipeline Olist trên Trino + Spark + Iceberg

Pipeline lakehouse chạy bằng Docker Compose trên dữ liệu thương mại điện tử
Olist (Brazil). Hai engine chia việc rõ ràng trên **cùng một lakehouse Iceberg**:

| Engine | Việc | Lý do |
|---|---|---|
| **Spark** | Đưa dữ liệu **vào** lakehouse: nạp CSV lịch sử (batch) và ghi luồng thay đổi từ Redpanda (Structured Streaming) | đọc file/Kafka phân tán, ghi Iceberg theo lô, giữ trạng thái stream qua checkpoint |
| **Trino** | **Mọi thứ phía trên bronze**: dbt dựng silver/gold, view truy vấn tức thì, bảo trì bảng, BI | SQL engine MPP, tối ưu truy vấn (CBO, dynamic filtering), chịu lỗi (FTE), phân quyền, xếp hàng theo resource group |

Airflow chốt dữ liệu stream vào silver chuẩn và gold mỗi ngày, Superset đọc
qua Trino.

---

## Mục lục

1. [Sơ đồ tổng quan](#sơ-đồ-tổng-quan)
2. [Luồng dữ liệu theo tầng](#luồng-dữ-liệu-theo-tầng)
3. [Hành trình một đơn hàng](#hành-trình-một-đơn-hàng)
4. [Spark, Trino, Airflow phối hợp xuyên suốt](#spark-trino-airflow-phối-hợp-xuyên-suốt)
5. [Catalog Iceberg dùng chung — chỗ Spark và Trino gặp nhau](#catalog-iceberg-dùng-chung--chỗ-spark-và-trino-gặp-nhau)
6. [datagen → Redpanda](#datagen--redpanda)
7. [Spark trong pipeline](#spark-trong-pipeline)
8. [Trino trong pipeline](#trino-trong-pipeline)
9. [Airflow — chốt hằng ngày](#airflow--chốt-hằng-ngày)
10. [dbt — silver chuẩn và gold](#dbt--silver-chuẩn-và-gold)
11. [Bảo trì Iceberg](#bảo-trì-iceberg)
12. [Cấu hình tài nguyên](#cấu-hình-tài-nguyên)
13. [Vận hành](#vận-hành)
14. [Kết quả kiểm chứng](#kết-quả-kiểm-chứng)
15. [Cây thư mục](#cây-thư-mục)

---

## Sơ đồ tổng quan

Đường liền `──►` là **dữ liệu**, đường chấm `┄┄►` là **điều khiển / metadata**.

```
 ╔═════════════════════════════ NGUỒN ══════════════════════════════════════════════════════════════╗
 ║                                                                                                   ║
 ║  data/*.csv (9 file Olist)                    datagen  (Faker + phân phối Olist, đồng hồ mô phỏng) ║
 ║        │                                          │  outbox SQLite ─► produce                      ║
 ╚════════┼══════════════════════════════════════════┼════════════════════════════════════════════════╝
          │                                          ▼
          │                               ┌──────────────────────┐
          │                               │ Redpanda  (Kafka API) │  6 topic olist.*, giữ 30 ngày
          │                               └──────────┬───────────┘
          │                                          │ Kafka source, ≤ 20.000 offset / lô
 ╔════════┼══════════════════════════ SPARK STANDALONE ┼══════════════════════════════════════════════╗
 ║        ▼                                          ▼                                                ║
 ║  ┌─────────────────────┐                ┌─────────────────────────┐         ┌──────────────────┐   ║
 ║  │ spark-submit        │                │ spark-streaming         │         │ spark-master     │   ║
 ║  │ DRIVER (batch)      │                │ DRIVER (chạy liên tục)  │◄┄┄┄┄┄┄┄►│ cấp core / RAM   │   ║
 ║  │ land_raw.py         │                │ stream_olist.py         │         └──────────────────┘   ║
 ║  │ ingest_bronze.py    │                │ foreachBatch mỗi 30 s   │                  ┆             ║
 ║  └─────────┬───────────┘                └────────────┬────────────┘                  ┆             ║
 ║            │ giao task                               │ giao task                     ┆             ║
 ║  ┌─────────▼─────────────────────────────────────────▼──────────────┐                ┆             ║
 ║  │ spark-worker-1 (executor 1 core/1 GB) · spark-worker-2 (executor) │◄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘             ║
 ║  └─────────┬───────────────────────────────────────┬────────────────┘                              ║
 ╚════════════┼═══════════════════════════════════════┼═══════════════════════════════════════════════╝
              │ s3a (CSV) + S3FileIO (Parquet)        │ S3FileIO (Parquet) + checkpoint s3a
              ▼                                       ▼
 ╔══════════════════════════════════ LAKEHOUSE ════════════════════════════════════════════════════════╗
 ║  MinIO  s3://raw  s3://lakehouse  s3://checkpoints  s3://exchange                                    ║
 ║     ▲ file Parquet / metadata.json / manifest                                                         ║
 ║     ┆                                                                                                  ║
 ║  iceberg-rest (REST catalog) ┄┄ con trỏ metadata ┄┄► postgres-ops (database iceberg_catalog)           ║
 ║     ▲  commit snapshot từ Spark VÀ Trino — một nguồn sự thật                                           ║
 ╚═════┼══════════════════════════════════════════════════════════════════════════════════════════════════╝
       ┆ REST + S3
 ╔═════┼═════════════════════════════════ TRINO ═════════════════════════════════════════════════════════╗
 ║  ┌──┴───────────────────────────────────┐        ┌──────────────────┐   ┌──────────────────┐          ║
 ║  │ trino-coordinator                    │──────► │ trino-worker-1   │   │ trino-worker-2   │          ║
 ║  │ parse · CBO · lập lịch · resource    │──────► │ đọc/ghi Parquet  │   │ đọc/ghi Parquet  │          ║
 ║  │ group · phân quyền · dynamic filter  │        │ join · spill     │   │ join · spill     │          ║
 ║  └──▲──────────▲──────────▲──────────▲──┘        └────────┬─────────┘   └────────┬─────────┘          ║
 ║     │          │          │          │                    └───── FTE exchange ───┴──► s3://exchange   ║
 ╚═════┼══════════┼══════════┼══════════┼══════════════════════════════════════════════════════════════════╝
       │          │          │          │
   ┌───┴───┐ ┌────┴────┐ ┌───┴────┐ ┌───┴──────┐
   │  dbt  │ │ Airflow │ │Superset│ │trino-cli │
   │ (etl) │ │ (maint.)│ │  (bi)  │ │ (adhoc)  │
   └───▲───┘ └────┬────┘ └────────┘ └──────────┘
       └──────────┘  Airflow chạy dbt (BashOperator) lúc 00:00 VN
```

---

## Luồng dữ liệu theo tầng

```
                      SPARK ghi                                        TRINO ghi (qua dbt / Airflow)
  ─────────────────────────────────────────────────────    ─────────────────────────────────────────────────
  RAW              BRONZE                                   SILVER                              GOLD
  s3://raw/olist   lakehouse.bronze                         lakehouse.silver                    lakehouse.gold

  9 file CSV ────► orders, order_items, payments,    ┐
  (nguyên byte)    reviews, customers, sellers,      │
                   geolocation, products,            ├────► silver_orders ──────┐
                   category_translation              │      silver_customers ───┤
                   [batch, overwrite]                │      silver_payments ────┼──► silver_order_facts ──► gold_revenue_by_state
                                                     │      snap_products (SCD2)│    [incremental MERGE]    gold_revenue_by_category
  Redpanda ──────► customer_events                   │                          │                           gold_seller_scorecard
  (6 topic)        order_events                      │      ▲ chỉ sự kiện       │
                   order_item_events                 ├──────┘ ingest_ts < mốc   │
                   payment_events                    │        (Airflow)         │
                   review_events                     ┘                          │
                   datagen_params                                               │
                   [stream, append]                                             │
         │                                                                      │
         └───────► silver.orders_live  [stream, MERGE theo seq] ──┐             │
                                                                  ▼             ▼
                                          silver.v_orders_now / v_order_facts_now / v_customers_now
                                          = silver chuẩn (đã chốt)  +  live (chưa chốt)   ◄── TRUY VẤN TỨC THÌ

  ops.watermark ◄── Airflow ghi mốc sau mỗi lần chốt ──► Spark đọc để xoá khỏi orders_live đơn đã chốt
```

| Tầng | Bảng | Engine ghi | Kiểu ghi | Partition |
|---|---|---|---|---|
| raw | 9 file CSV | Spark (driver) | chép nguyên byte | — |
| bronze lịch sử | 9 bảng Olist | Spark batch | overwrite trong 1 snapshot | `orders`: `days(order_purchase_timestamp)`, `reviews`: `months(review_creation_date)` |
| bronze stream | 5 bảng `*_events` + `datagen_params` | Spark Streaming | append mỗi lô | `days(ingest_ts)` |
| silver live | `orders_live` | Spark Streaming | MERGE mỗi lô, merge-on-read | — |
| silver chuẩn | `silver_orders`, `silver_customers`, `silver_payments`, `silver_order_facts`, `snap_products` | Trino (dbt) | table / incremental MERGE / snapshot | `month(order_purchase_timestamp)` |
| silver view | `v_customers_now`, `v_orders_now`, `v_order_facts_now` | Trino | view | — |
| gold | 3 bảng | Trino (dbt) | table | — |
| ops | `watermark` | Trino (Airflow) | append | — |

Cột kỹ thuật của mỗi bảng `bronze.*_events` (ngoài các cột nghiệp vụ Olist):

| Cột | Ý nghĩa |
|---|---|
| `event_id` | sha1 tất định — gửi lại vẫn cùng id |
| `event_type`, `seq` | loại sự kiện và thứ tự trong một đơn |
| `event_ts` / `sim_ts` | giờ thật datagen tạo / giờ mô phỏng của sự kiện |
| `ingest_ts` | giờ thật Spark ghi lô — partition và **mốc cắt** của Airflow |
| `kafka_partition`, `kafka_offset`, `batch_id` | vị trí trong Redpanda và lô Spark đã ghi |

### Hai trục thời gian

| Trục | Cột | Đồng hồ | Dùng cho |
|---|---|---|---|
| Nghiệp vụ | `order_purchase_timestamp`, `order_*_date`, `sim_ts` | mô phỏng (`TIME_SCALE=60`: 1 phút thật = 1 giờ mô phỏng, bắt đầu 2018-10-18) | phân tích, gold |
| Kỹ thuật | `event_ts`, `ingest_ts`, `updated_at` | thật | khử trùng, mốc cắt, dọn `orders_live` |

---

## Hành trình một đơn hàng

```
 giờ thật   ─────────────────────────────────────────────────────────────────────────────────────────►

 t0          datagen tạo đơn (sim 2018-11-19 10:02) → ghi outbox + đồng hồ trong 1 transaction
             → produce olist.orders / olist.order_items / olist.customers
 t0+≤30s     Spark lô N:  bronze.order_events  += created (seq 1)        ── append, snapshot có stream_batch_id=N
                          bronze.order_item_events += 2 dòng hàng
                          silver.orders_live   MERGE → đơn ở trạng thái created
             Trino:       SELECT … FROM silver.v_orders_now  → THẤY ĐƠN (p50 ~16 s sau khi gửi)

 t0+~25p     datagen: approved (sim +~21 phút)  →  Spark lô N+k: order_events += approved (seq 2),
                                                    payment_events += thanh toán,  orders_live: approved
 t0+vài giờ  shipped (seq 3) → delivered (seq 4) → review  — mỗi bước một sự kiện, orders_live luôn
             giữ seq cao nhất (sự kiện đến trễ seq thấp hơn bị bỏ qua)

 00:00 VN    Airflow olist_daily: mốc = 00:00 → chờ 5 phút → dbt build --vars cutoff_ts
             silver_orders      ← seq cao nhất của đơn trong order_events (ingest_ts < mốc)
             silver_order_facts ← MERGE dòng hàng + thanh toán + đánh giá
             gold_*             ← chỉ ngày mô phỏng đã đóng
             ops.watermark      += mốc
 sau đó      Spark (chu kỳ bảo trì kế tiếp): DELETE orders_live WHERE updated_at < mốc
             → v_orders_now lấy đơn từ silver_orders; nếu đơn có sự kiện mới sau mốc,
               MERGE chèn lại vào orders_live và view ưu tiên bản live
```

---

## Spark, Trino, Airflow phối hợp xuyên suốt

Ba thành phần không gọi trực tiếp nhau. Chúng phối hợp qua **dữ liệu dùng chung
trong lakehouse** và một vài **quy ước**.

### Một ngày của pipeline

```
 giờ thật (VN)  00:00          00:05                 ~00:10            ~00:15                 suốt ngày
 ───────────────┼──────────────┼─────────────────────┼─────────────────┼──────────────────────┼──────────►
 datagen        │ sinh sự kiện liên tục, đồng hồ mô phỏng chạy (1 ngày thật ≈ 60 ngày mô phỏng)       │
 ───────────────┼──────────────┼─────────────────────┼─────────────────┼──────────────────────┼──────────
 Spark          │ lô 30 s: append bronze.*_events ─► MERGE orders_live ─► (mỗi 20 lô) bảo trì live    │
 Streaming      │     ▲ sự kiện trước 00:00 có ingest_ts < mốc        │ đọc mốc mới ─► DELETE live   │
 ───────────────┼──────────────┼─────────────────────┼─────────────────┼──────────────────────┼──────────
 Airflow        │ resolve_     │ dbt_build ─────────►│ publish_        │ maintain_bronze_     │
 olist_daily    │ cutoff, chờ  │ (gửi SQL tới Trino) │ watermark       │ events / silver_facts│
                │ 5 phút       │                     │                 │                      │
 ───────────────┼──────────────┼─────────────────────┼─────────────────┼──────────────────────┼──────────
 Trino          │              │ etl: CTAS silver,   │ adhoc: INSERT   │ maintenance: optimize│ bi / adhoc:
                │              │ MERGE facts, gold,  │ ops.watermark   │ expire, orphan,      │ dashboard,
                │              │ 31 test             │                 │ ANALYZE (1 query/lúc)│ view *_now
 ───────────────┼──────────────┼─────────────────────┼─────────────────┼──────────────────────┼──────────
 Superset       │ đọc gold (số chốt) và v_*_now (số tức thì) bất cứ lúc nào                            │
```

### Các quy ước nối ba thành phần

| Quy ước | Bên tạo | Bên dùng | Đảm bảo gì |
|---|---|---|---|
| **Catalog Iceberg `lakehouse`** (REST + MinIO) | Spark, Trino | Spark, Trino | một bảng, một lịch sử snapshot, commit nguyên tử; không có bản sao dữ liệu giữa hai engine |
| **Bronze trước, silver sau** trong mỗi lô | Spark | Trino (view) | không bao giờ có trạng thái live mà bronze chưa có |
| **`event_id`** tất định | datagen | Spark (dedupe 10 phút), dbt (`distinct`) | sự kiện gửi lại không bị đếm hai lần |
| **`stream_batch_id`** trong snapshot | Spark | Spark (lô phát lại) | bronze ghi đúng một lần mỗi lô |
| **`seq`** trong mỗi đơn | datagen | Spark (MERGE), dbt (`row_number … order by seq desc`) | trạng thái chỉ tiến, không lùi |
| **`ingest_ts`** | Spark (giờ thật lúc ghi lô) | dbt (`cutoff_filter`), Airflow (optimize partition đã đóng) | ranh giới rõ ràng giữa "đã chốt" và "chưa chốt" |
| **`ops.watermark`** | Airflow (chỉ sau dbt thành công) | dbt lần sau (`prev_cutoff_ts`), Spark (dọn `orders_live`) | live chỉ giữ phần chưa chốt; incremental biết khoảng cần cập nhật |
| **`sim_ts` / ngày mô phỏng đã đóng** | datagen | dbt gold (`closed_sim_days`, `is_final`) | gold không công bố ngày còn đang chạy |
| **Resource group** | Trino | dbt, Airflow, Superset, CLI | ETL, BI, bảo trì không giẫm lên nhau |
| **Ai ghi thì người đó bảo trì** | — | Spark (`orders_live`), Airflow qua Trino (`*_events`, `silver_order_facts`) | không có hai bên cùng viết lại một bảng |

### Dữ liệu đi qua ai, ở dạng nào

| Chặng | Engine | Dạng | Độ trễ |
|---|---|---|---|
| CSV → raw → bronze lịch sử | Spark batch | Parquet trong snapshot overwrite | một lần |
| sự kiện → Redpanda | datagen | JSON, key theo đơn | < 1 s sau khi sinh |
| Redpanda → `bronze.*_events` + `orders_live` | Spark Streaming | Parquet append + MERGE (delete file) | 15–30 s |
| bronze + live → `v_*_now` | Trino (view, lúc đọc) | kết quả truy vấn | ngay khi lô commit |
| bronze → silver chuẩn → gold | Trino (dbt, Airflow kích hoạt) | Parquet, CTAS / MERGE | mỗi ngày lúc 00:00 VN |
| gold / `v_*_now` → dashboard | Trino → Superset | kết quả truy vấn | lúc mở dashboard |

### Khi một thành phần dừng

| Sự cố | Điều xảy ra | Khi chạy lại |
|---|---|---|
| datagen dừng | không có sự kiện mới, đồng hồ mô phỏng đứng yên | đồng hồ chạy tiếp từ chỗ cũ, outbox gửi nốt sự kiện còn kẹt |
| Redpanda dừng | datagen giữ sự kiện trong outbox; Spark lô rỗng | outbox xả hết, Spark đọc tiếp từ offset trong checkpoint |
| Spark Streaming chết giữa lô | snapshot của lô có thể đã commit một phần | lô được phát lại; bảng đã có `stream_batch_id` thì bỏ qua; MERGE theo `seq` không làm lùi trạng thái |
| Spark Streaming treo | watchdog thấy `batchId` đứng yên 10 phút → thoát | Docker dựng lại container, chạy tiếp từ checkpoint |
| Trino dừng | Spark vẫn ghi bronze/live bình thường (không cần Trino); Airflow task lỗi rồi retry; datagen đang chạy không bị ảnh hưởng | Trino đọc thấy ngay mọi snapshot Spark đã ghi; datagen khởi động lại thì chờ Trino để nạp dữ liệu tham chiếu |
| dbt lỗi | `publish_watermark` không chạy → mốc không tiến; Spark không xoá thêm gì khỏi `orders_live` | view vẫn đủ đơn (lấy từ live); lần chốt sau tự gom cả phần bị lỡ |
| Máy tắt qua nửa đêm | không có lần chốt nào | `catchup` chạy bù từng ngày theo thứ tự, mỗi lần với mốc của ngày đó |
| Spark và Trino commit cùng bảng | bên commit sau bị từ chối | Iceberg tự retry trên metadata mới |

### Tính nhất quán của truy vấn tức thì

Tại mọi thời điểm, `v_orders_now` có **đúng một dòng mỗi đơn**:

```
 đơn chưa chốt         → chỉ có trong orders_live           → view lấy từ live
 đơn đã chốt, không đổi → có trong silver_orders, đã xoá khỏi live → view lấy từ silver
 đơn đã chốt, có sự kiện mới sau mốc → có ở cả hai           → view ưu tiên live (NOT EXISTS)
```

Khoảng duy nhất view "chậm" hơn bronze là vài giây trong một lô: bronze đã
commit, MERGE vào `orders_live` chưa xong.

---

## Catalog Iceberg dùng chung — chỗ Spark và Trino gặp nhau

```
   Spark (SparkCatalog "lakehouse")                    Trino (connector iceberg, catalog "lakehouse")
        │  type=rest, io=S3FileIO                            │  iceberg.catalog.type=rest
        │                                                    │
        │ 1. ghi file Parquet + manifest ──► MinIO ◄── 1. ghi file Parquet + manifest
        │ 2. POST commit (snapshot mới) ──► iceberg-rest ◄── 2. POST commit
        │                                    │  kiểm tra "branch main vẫn là snapshot X?"
        │                                    │  đúng → đổi con trỏ metadata trong postgres-ops
        │                                    │  sai  → CommitFailedException → bên đó tự retry
        ▼                                    ▼
   đọc snapshot mới nhất ◄───────── cùng một con trỏ ─────────► đọc snapshot mới nhất
```

* **Một catalog, hai engine.** Hai bên cấu hình catalog tên `lakehouse` trỏ cùng
  `http://iceberg-rest:8181`, warehouse `s3://lakehouse`. Bảng Spark vừa commit
  thì truy vấn Trino kế tiếp thấy ngay, không cần đồng bộ hay refresh.
* **Optimistic concurrency.** Spark append vào `bronze.*_events` trong lúc Trino
  `optimize` cùng bảng: commit đến sau bị từ chối vì snapshot gốc đã đổi, Iceberg
  tự retry trên metadata mới. Append không xoá file nào nên retry luôn thành công.
* **Thống kê dùng chung.** Spark ghi NDV bằng `compute_table_stats` vào file
  Puffin; Trino đọc đúng định dạng đó cho CBO.
* **Lịch sử chung.** `$snapshots` của một bảng có cả snapshot do Spark và do Trino
  ghi; time travel `FOR VERSION AS OF` dùng được trên mọi snapshot.

---

## datagen → Redpanda

`datagen` là một tiến trình Python duy nhất (`datagen/datagen.py`) đóng vai **hệ
thống bán hàng**: sinh đơn mới, đẩy từng đơn qua vòng đời, phát mọi thay đổi
thành sự kiện lên Redpanda. Nó không ghi lakehouse — chỉ đọc Trino một lần lúc
khởi động để lấy phân phối thật.

### Kiến trúc bên trong

```
                     ┌────────────────────────── lúc khởi động ──────────────────────────┐
                     │ Reference: 6 truy vấn Trino (source=datagen) vào lakehouse.bronze  │
                     │  · geo        : (zip, city, state) của 99.441 khách                │
                     │  · buyers     : customer_unique_id → địa chỉ (min_by)              │
                     │  · pairs      : (product, seller, avg price, avg freight, số lần)  │
                     │  · hour_factor: số đơn theo giờ / trung bình                       │
                     │  · installments, review_mix (đơn đúng hạn / trễ hạn)                │
                     └───────────────────────────────┬────────────────────────────────────┘
                                                     ▼
 ┌──────────────────────────────── vòng lặp, mỗi 1 giây thật ─────────────────────────────────────┐
 │  tick(real_dt):                                                                                │
 │   t1 = sim + min(real_dt, 5 s) × TIME_SCALE           ◄ tắt máy 10 tiếng cũng chỉ tiến 5 s thật │
 │   BEGIN (SQLite)                                                                               │
 │    1. sang ngày mô phỏng mới → nạp lại scenario.yml, phát sự kiện params của ngày             │
 │    2. đơn mới: N ~ Poisson(orders_per_day / 86400 × hour_factor[giờ] × khoảng mô phỏng)        │
 │       mỗi đơn → customer_created, order_created, item_added × k → outbox; lịch "approve"      │
 │    3. bước tới hạn: SELECT … FROM pending WHERE due <= t1 ORDER BY due → advance()             │
 │    4. lưu sim = t1                                                                             │
 │   COMMIT                                       ◄ đồng hồ, hàng đợi, outbox đổi cùng một lúc    │
 │   flush_outbox(): produce → chờ ack → DELETE khỏi outbox đúng các tin đã ack                   │
 └────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                     │ Kafka producer: acks=all, enable.idempotence,
                                                     ▼ lz4, linger 100 ms, key = order_id / customer_id
                                          Redpanda 6 topic olist.*
```

Trạng thái bền trong SQLite (`/state/datagen.sqlite`, volume `datagen-state`):

| Bảng | Chứa |
|---|---|
| `clock` | `sim` (giờ mô phỏng hiện tại), `last_day` (ngày mô phỏng đã phát tham số) |
| `pending` | mỗi đơn đang dở một dòng: bước kế tiếp, thời điểm tới hạn (giờ mô phỏng), trạng thái JSON (đơn, `seq`, tổng tiền) |
| `buyers` | người mua do datagen tạo + địa chỉ — để lần mua sau giữ đúng địa chỉ |
| `outbox` | sự kiện đã sinh nhưng chưa được Redpanda xác nhận |

### Vòng đời đơn và thời gian giữa các bước

```
             approve_delay_min                carrier_delay_h          delivery_delay_h      0–24 h
 created ──────────(log-normal)──────► approved ─────────────► shipped ─────────────► delivered ────► review
  seq 1       + payments (1–3 dòng)     seq 2   │                seq 3                   seq 4
                                                ├─ cancel_rate ──────► canceled    (~2 h sau)  ─┐ 50 % có
                                                └─ unavailable_rate ─► unavailable (~24 h sau) ─┘ review điểm thấp
```

* Thời gian chờ mỗi bước là log-normal quanh trung vị của ngày đó, nên có đuôi
  dài như dữ liệu thật.
* Ngày giao dự kiến = ngày đặt + `estimated_delivery_d`; đơn giao **sau** ngày
  này lấy điểm đánh giá từ phân phối của đơn trễ trong Olist (thấp hơn rõ rệt).
* Mỗi lần đổi trạng thái phát **toàn bộ dòng đơn** với `seq` tăng dần — bên nhận
  chỉ cần giữ bản có `seq` lớn nhất.

### Faker và dữ liệu thật

| Cột | Nguồn |
|---|---|
| `order_id`, `customer_id`, `customer_unique_id`, `review_id` | UUID từ bộ sinh ngẫu nhiên |
| `review_comment_title`, `review_comment_message` | **Faker** `pt_BR` (11,7 % / 41,3 % đơn có, như Olist) |
| zip / city / state của khách | một dòng thật từ `bronze.customers` (ba cột luôn khớp nhau) |
| người mua quay lại | `customer_unique_id` cũ — Olist hoặc do datagen tạo — **giữ nguyên địa chỉ** |
| `product_id`, `seller_id` | cặp thật từ `bronze.order_items`, trọng số theo số lần bán |
| `price`, `freight_value` | trung bình của cặp đó × `price_multiplier` × nhiễu ±10 % |
| `payment_type`, `payment_installments` | phân phối thật; đơn nhiều thanh toán thì dòng sau là `voucher` |
| `review_score` | phân phối thật, tách theo đơn đúng hạn / trễ hạn |
| `order_purchase_timestamp` … `review_answer_timestamp` | đồng hồ mô phỏng |

### Tham số động (`scenario.yml`)

Mỗi tham số tính lại theo **ngày mô phỏng**:

```
giá trị = base × weekday[thứ] × (1 + trend_per_30d)^(ngày/30) × exp(AR1) × chiến dịch, kẹp trong [min, max]
AR1_k   = 0,8 × AR1_{k-1} + noise × 0,6 × ε_k      (ε seed theo tên tham số + ngày → tất định)
```

| Tham số | base (Olist thật) | Ý nghĩa |
|---|---|---|
| `orders_per_day` | 222 | số đơn trung bình/ngày; thấp hơn vào T7, CN |
| `repeat_buyer_rate` | 3,1 % | tỉ lệ đơn của người mua cũ |
| `cancel_rate` / `unavailable_rate` | 0,63 % / 0,61 % | tỉ lệ huỷ / hết hàng sau khi duyệt |
| `approve_delay_min` | 21 phút | đặt → duyệt; chậm hơn cuối tuần |
| `carrier_delay_h` | 43 giờ | duyệt → giao cho vận chuyển |
| `delivery_delay_h` | 171 giờ | vận chuyển → khách nhận |
| `estimated_delivery_d` | 23 ngày | ngày giao dự kiến |
| `price_multiplier` | 1,0 | hệ số giá, tăng nhẹ theo thời gian |
| `review_rate` | 99 % | tỉ lệ đơn giao xong có đánh giá |

Phân phối rời rạc (`mixes`): số dòng hàng/đơn, số thanh toán/đơn, loại thanh
toán. Chiến dịch nhân vào tham số hoặc vào một khoá của phân phối
(`payment_type.voucher`, `review_score.1`):

| Chiến dịch | Ngày mô phỏng | Tác động |
|---|---|---|
| `black_friday` | 23–26/11/2018 | đơn × 2, giao vận chuyển chậm × 1,6, voucher × 2,5 |
| `dinh_cong_van_chuyen` | 10–20/12/2018 | giao chậm × 2,2, huỷ × 3, điểm 1 sao × 2,5 |

File được đọc lại mỗi khi sang ngày mô phỏng mới — sửa là áp dụng, không restart.
Bộ tham số thực dùng của từng ngày được phát lên `olist.datagen_params` →
`bronze.datagen_params`, để dashboard đặt cạnh số liệu thực tế.

### Định dạng sự kiện

```jsonc
{
  "event_id":   "3f0c…e1",                      // sha1("order", order_id, "order_shipped")
  "event_type": "order_shipped",
  "event_ts":   "2026-10-05 08:31:12.418",      // giờ thật
  "sim_ts":     "2018-11-19 14:05:00",          // giờ mô phỏng
  "seq":        3,
  "payload":    { "order_id": "…", "order_status": "shipped", "order_purchase_timestamp": "…", … }
}
```

`event_id` tất định từ khoá nghiệp vụ: chết giữa lúc gửi rồi gửi lại thì vẫn ra
đúng id cũ. Key Kafka là `order_id` (hoặc `customer_id`), nên mọi sự kiện của
một đơn vào cùng partition, giữ thứ tự.

| Topic | Bảng đích |
|---|---|
| `olist.customers` (compacted) | `bronze.customer_events` |
| `olist.orders` | `bronze.order_events` + `silver.orders_live` |
| `olist.order_items` | `bronze.order_item_events` |
| `olist.payments` | `bronze.payment_events` |
| `olist.reviews` | `bronze.review_events` |
| `olist.datagen_params` | `bronze.datagen_params` |

---

## Spark trong pipeline

### Cụm Standalone

```
                        ┌───────────────────────────┐
                        │ spark-master :7077 / :8082 │  cluster manager: biết worker nào rảnh,
                        └──────┬──────────────┬─────┘  cấp executor cho ứng dụng — không chạm dữ liệu
               đăng ký executor│              │đăng ký executor
          ┌────────────────────▼───┐    ┌─────▼──────────────────┐
          │ spark-worker-1 :8083   │    │ spark-worker-2 :8084   │  mỗi worker nhường 1 core + 1 GB
          │  └─ executor (1c/1g)   │    │  └─ executor (1c/1g)   │  = vừa đúng một executor
          └────────────▲───────────┘    └────────────▲───────────┘
                       │ task / kết quả               │
          ┌────────────┴───────────┐    ┌─────────────┴──────────┐
          │ spark-streaming        │    │ spark-submit           │
          │ driver, cores.max = 1  │    │ driver, cores.max = 2  │
          │ → giữ 1 executor       │    │ → lấy executor còn rảnh│
          └────────────────────────┘    └────────────────────────┘
```

| Thành phần | Vai |
|---|---|
| `spark-master` | cluster manager — nhận đăng ký worker, cấp executor theo `spark.cores.max` của từng ứng dụng |
| `spark-worker-1/2` | khởi động executor (JVM riêng) — nơi đọc CSV/Kafka và ghi Parquet |
| driver (`spark-submit`, `spark-streaming`) | lập kế hoạch, chia task, nhận kết quả; chạy **client mode** vì Standalone không hỗ trợ cluster mode cho ứng dụng Python |

`spark.driver.host=$(hostname -i)` để executor gọi ngược được về driver qua mạng
Docker (cổng cố định 7078 driver, 7079 block manager).

### Job batch: nạp lịch sử

```
data/*.csv ──land_raw.py──► s3://raw/olist/ ──ingest_bronze.py──► lakehouse.bronze.* (9 bảng)
           (driver, Hadoop FileUtil,          (2 executor đọc song song,
            chép nguyên byte)                  ghi Parquet qua S3FileIO)
```

`ingest_bronze.py`, với từng bảng:

| Bước | Tính năng Spark / Iceberg |
|---|---|
| Tạo bảng nếu chưa có | DDL Iceberg format v2 kiểu `TIMESTAMP_NTZ`; **hidden partitioning** `days()` / `months()`; `WRITE ORDERED BY` để file được sắp, Trino lọc min/max hiệu quả |
| Đọc CSV từ `s3a://raw/olist/` | đọc phân tán; `lineSep` riêng từng file (CRLF cho `reviews` và `category_translation`); `multiLine` cho văn bản đánh giá |
| Ép kiểu | theo schema **của chính bảng đích**; job **dừng** nếu ép kiểu biến giá trị khác NULL thành NULL |
| Ghi | `writeTo(...).overwrite(lit(True))` — thay toàn bộ dòng trong **một** snapshot, giữ bảng, partition spec và lịch sử |
| Thống kê | `CALL lakehouse.system.compute_table_stats(...)` — NDV dạng theta sketch trong file Puffin cho CBO của Trino |
| Báo cáo | số task mỗi executor đọc từ REST API của driver |

### Structured Streaming: Redpanda → lakehouse

```
 Redpanda 6 topic
      │  readStream.format("kafka"), startingOffsets=earliest, maxOffsetsPerTrigger=20000
      ▼
 withWatermark("timestamp", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])
      │
      ▼  trigger(processingTime = 30 s), checkpoint s3a://checkpoints/stream_olist
 ┌──────────────────────────── foreachBatch(batch, batch_id) ────────────────────────────┐
 │ 0. Lô đầu tiên sau khởi động: snapshot nào có stream_batch_id = batch_id? → đã ghi,  │
 │    bỏ qua bảng đó (chống ghi trùng khi Spark phát lại lô)                              │
 │ 1. Tách theo topic, from_json theo schema bảng đích (đọc một lần lúc khởi động)       │
 │ 2. BRONZE: append 6 bảng *_events, snapshot-property stream_batch_id = batch_id        │
 │ 3. SILVER LIVE: MERGE INTO silver.orders_live                                          │
 │       ON order_id  WHEN MATCHED AND s.last_seq > t.last_seq THEN UPDATE                │
 │                    WHEN NOT MATCHED THEN INSERT                                        │
 │ 4. Mỗi 20 lô: dọn theo ops.watermark + rewrite_data_files + rewrite_position_delete_   │
 │    files + expire_snapshots (xem Bảo trì)                                              │
 └────────────────────────────────────────────────────────────────────────────────────────┘
      ▲
 watchdog thread: batchId đứng yên 10 phút trong lúc có lô đang chạy → os._exit(3)
                  → Docker (restart: unless-stopped) dựng lại → chạy tiếp từ checkpoint
```

| Tính năng | Cách dùng trong pipeline |
|---|---|
| Kafka source + checkpoint | offset đã xử lý lưu trên `s3://checkpoints`; container chết thì chạy tiếp đúng chỗ |
| `maxOffsetsPerTrigger` | giới hạn 20.000 sự kiện mỗi lô — bật lại sau thời gian dài không dồn một lô khổng lồ |
| `dropDuplicatesWithinWatermark` | khử trùng `event_id` trong cửa sổ 10 phút; bản gửi lại muộn hơn được dbt khử trùng thêm lần nữa |
| `foreachBatch` | mỗi lô là một DataFrame tĩnh → ghi được nhiều bảng và chạy MERGE Iceberg |
| Snapshot property `stream_batch_id` | ghi bronze **đúng một lần** kể cả khi lô bị phát lại |
| MERGE có điều kiện `seq` | `orders_live` không bao giờ lùi trạng thái khi sự kiện đến trễ |
| Merge-on-read (`write.delete.mode`, `write.update.mode`, `write.merge.mode`) | MERGE mỗi 30 s chỉ ghi delete file nhỏ thay vì viết lại data file |
| `write.metadata.delete-after-commit.enabled` + `previous-versions-max=20` | chặn `metadata.json` tích lại sau hàng nghìn commit |
| Tài nguyên | `spark.cores.max=1`, `spark.sql.shuffle.partitions=2` — luồng chỉ giữ một executor |

`foreachBatch` chạy trên một **phiên Spark nhân bản**: view tạm của lô chỉ thấy
được qua `batch.sparkSession`, nên MERGE được gọi qua đúng phiên đó.

UI: master <http://localhost:8082>, driver streaming <http://localhost:4040>
(tab *Structured Streaming*: tốc độ vào/ra, thời gian từng lô).

---

## Trino trong pipeline

### Ai gửi gì vào Trino

```
                  source / client_tags                  resource group        việc
 dbt (Airflow) ── dbt-trino-* ───────────────────────► global.etl          ── CREATE TABLE AS, MERGE, test
 Airflow tasks ── airflow, tags=[maintenance] ───────► global.maintenance  ── optimize, expire, orphan, ANALYZE
 Airflow tasks ── airflow ───────────────────────────► global.adhoc        ── đọc/ghi ops.watermark
 Superset ─────── superset ──────────────────────────► global.bi           ── dashboard, SQL Lab
 trino-cli ────── trino-cli ─────────────────────────► global.adhoc        ── view *_now, truy vấn tay
```

### Coordinator và worker

```
           client
             │ HTTP :8080
   ┌─────────▼──────────────────────────────────────────────────────────┐
   │ trino-coordinator  (không nhận task: node-scheduler.include-coordinator=false)
   │  1. parse + phân quyền (rules.json)                                  │
   │  2. xếp vào resource group, chờ nếu nhóm đầy                         │
   │  3. lập kế hoạch: CBO đọc thống kê Iceberg → thứ tự join, broadcast  │
   │     hay partitioned; lọc partition + min/max từ manifest            │
   │  4. chia stage → task, giao cho worker; thu dynamic filter          │
   └─────────┬───────────────────────────────────────┬──────────────────┘
             ▼                                       ▼
   ┌───────────────────────┐   exchange   ┌───────────────────────┐
   │ trino-worker-1        │◄────────────►│ trino-worker-2        │
   │ đọc Parquet từ MinIO  │  (FTE: qua   │ đọc Parquet từ MinIO  │
   │ join / agg / ghi file │ s3://exchange)│ join / agg / ghi file │
   │ spill → /data/trino   │              │ spill → /data/trino   │
   └───────────────────────┘              └───────────────────────┘
```

### Catalog

| Catalog | Connector | Dùng cho |
|---|---|---|
| `lakehouse` | Iceberg + REST catalog + S3 (MinIO) | toàn bộ dữ liệu pipeline |
| `tpch` | sinh dữ liệu theo scale factor | đo hiệu năng, demo |
| `faker` | sinh dữ liệu giả theo schema tự khai | demo |
| `jmx` | số liệu JVM/Trino dạng bảng | giám sát cụm |
| `system` | metadata runtime | `system.runtime.queries`, `nodes` |

Catalog `lakehouse` bật `iceberg.extended-statistics.collect-on-write=true`
(Trino tự thu NDV khi chính nó ghi), nén `ZSTD`, `expire-snapshots.min-retention`
và `remove-orphan-files.min-retention` hạ xuống `1h` để bảo trì bảng stream.

### Resource group

```
 global   softMemoryLimit 90%, tối đa 6 query chạy, 50 xếp hàng, weighted_fair
  ├── etl          70% RAM · 3 query · weight 5   ◄ source khớp .*dbt.*
  ├── bi           30% RAM · 2 query · weight 3   ◄ source khớp .*(superset|python-client).*
  ├── adhoc        30% RAM · 2 query · weight 2   ◄ mọi thứ còn lại
  └── maintenance  60% RAM · 1 query · weight 1   ◄ client tag "maintenance"
```

`weighted_fair`: khi nhiều nhóm cùng chờ, nhóm weight cao được chạy trước theo
tỉ lệ. Bảo trì bảng chỉ chạy một query một lúc và nhường dbt/BI — dù Airflow
gửi hai task bảo trì song song, Trino vẫn xếp chúng thành hàng.

### Fault-tolerant execution (FTE)

`retry-policy=TASK` (biến `TRINO_RETRY_POLICY`):

```
 stage A (worker-1, worker-2) ──ghi kết quả trung gian──► s3://exchange ──đọc──► stage B
                     ▲
       worker chết giữa chừng → coordinator chạy lại ĐÚNG task hỏng trên worker còn sống,
       đọc lại input từ exchange — query không phải chạy lại từ đầu
```

| Thuộc tính | Giá trị | Ý nghĩa |
|---|---|---|
| `fault-tolerant-execution-task-memory` | 1GB | RAM ước lượng ban đầu mỗi task, tăng dần khi task OOM rồi retry |
| `fault-tolerant-execution-{min,max}-partition-count` | 2 / 8 | số partition của exchange |
| `fault-tolerant-execution-standard-split-size` | 32MB | kích thước split |
| `exchange.compression-codec` | LZ4 | nén dữ liệu trung gian |

Exchange tự xoá khi query xong. Áp dụng cho mọi query — kể cả MERGE của dbt và
`optimize` của Airflow.

### Spill to disk

Trên worker: `spill-enabled=true`, ghi vào `/data/trino/spill`, nén LZ4, tối đa
4 GB mỗi query mỗi node. Khi bộ nhớ một node vượt `memory-revoking-threshold`
(90 %), Trino đẩy trạng thái của join/aggregation xuống đĩa cho tới còn 50 % rồi
chạy tiếp, thay vì báo lỗi hết bộ nhớ.

### Cost-based optimizer (CBO)

```
 Spark: compute_table_stats ─┐
 Trino: collect-on-write     ├──► thống kê Iceberg (row count, NDV Puffin, min/max, null)
 Airflow: ANALYZE            ─┘                    │
                                                   ▼
                       optimizer.join-reordering-strategy = AUTOMATIC  (tối đa 8 bảng)
                       join-distribution-type        = AUTOMATIC  (broadcast bảng nhỏ / partitioned)
```

Ví dụ trong dbt: `gold_seller_scorecard` join `silver_order_facts` (~120 nghìn
dòng) với `bronze.sellers` (3.095 dòng) và `bronze.products` — CBO đặt bảng nhỏ
làm phía build và broadcast sang hai worker.

### Dynamic filtering

`enable-dynamic-filtering=true`. Khi join bảng lớn với bảng nhỏ, worker thu tập
khoá của phía build (tối đa 10.000 giá trị / 1 MB mỗi driver), coordinator gửi
nó sang phía probe; connector Iceberg dùng nó để **bỏ qua cả file / partition**
có min/max không khớp, đợi tối đa `iceberg.dynamic-filtering.wait-timeout=5s`.
Thấy trong `EXPLAIN ANALYZE` dưới dạng `dynamicFilterAssignments`.

### Tính năng Iceberg dùng qua Trino

| Tính năng | Trong pipeline |
|---|---|
| `MERGE INTO` | dbt incremental `silver_order_facts` (strategy `merge`) |
| `CREATE TABLE AS` | dbt materialization `table` cho silver/gold |
| Lọc partition + min/max | truy vấn `bronze.*_events` theo `ingest_ts`, silver/gold theo tháng |
| `ALTER TABLE … EXECUTE optimize / optimize_manifests / expire_snapshots / remove_orphan_files` | bảo trì từ Airflow |
| `ANALYZE` | cập nhật thống kê `silver_order_facts` |
| Bảng metadata `$snapshots`, `$files`, `$history`, `$partitions` | đếm file/delete file/snapshot khi bảo trì, kiểm chứng |
| Time travel `FOR VERSION / TIMESTAMP AS OF` | đọc lại bảng ở một lần chốt cũ |
| `table_changes(...)` | đọc các dòng được append giữa hai snapshot của bảng `*_events` |

### Phân quyền

`access-control.name=file`, luật trong `trino/security/rules.json`, nạp lại mỗi 30 s:

| User | Thấy gì |
|---|---|
| `admin` | mọi catalog, mọi quyền (dbt, Airflow, Superset dùng user này) |
| `analyst` | chỉ đọc `lakehouse`, `tpch`, `system`; silver bị **column mask** `customer_id` (`29a1****MASKED****`); `gold_revenue_by_state` bị **row filter** chỉ bang `SP`; không gọi được `table_changes`, không thấy `jmx` |

---

## Airflow — chốt hằng ngày

Airflow 3 standalone, LocalExecutor, metadata trong database `airflow` của
`postgres-ops`. dbt cài trong venv riêng của image Airflow. UI <http://localhost:8090>.

```
resolve_cutoff ─► wait_commit_margin ─► dbt_build ─► publish_watermark ─┬─► maintain_bronze_events
                                                                        └─► maintain_silver_facts
```

| Task | Operator | Làm gì |
|---|---|---|
| `resolve_cutoff` | `@task` | mốc = cuối khoảng lịch (00:00 giờ VN, quy về UTC) hoặc `cutoff_ts` trong conf; đọc mốc trước từ `ops.watermark` |
| `wait_commit_margin` | `@task.sensor` (reschedule) | chờ mốc + 5 phút để lô Spark bắt đầu ngay trước mốc kịp commit |
| `dbt_build` | `BashOperator` | `dbt build --vars '{"cutoff_ts": …, "prev_cutoff_ts": …}'` |
| `publish_watermark` | `@task` | chỉ khi dbt thành công: ghi mốc vào `ops.watermark` |
| `maintain_bronze_events` | `@task` | bảo trì 6 bảng `bronze.*_events` |
| `maintain_silver_facts` | `@task` | bảo trì `silver.silver_order_facts` |

* `schedule="0 0 * * *"`, timezone `Asia/Ho_Chi_Minh`, `catchup=True` (tắt máy
  qua nửa đêm thì bật lại chạy bù từng ngày), `max_active_runs=1`, `retries=2`.
* Mỗi sự kiện vào silver chuẩn đúng **một** lần: ở lần chốt đầu tiên có mốc lớn
  hơn `ingest_ts` của nó.
* Airflow chỉ điều phối — mọi SQL chạy trên Trino.

Chạy tay (mốc theo UTC):

```powershell
docker exec trinodemo-airflow airflow dags trigger olist_daily -c '{\"cutoff_ts\": \"2026-10-05 08:00:00\"}'
```

---

## dbt — silver chuẩn và gold

Project `dbt/olist`, adapter `dbt-trino`. Nguồn duy nhất là `lakehouse.bronze`.

### Macro mốc cắt (`macros/stream_cutoff.sql`)

| Macro | Sinh ra |
|---|---|
| `cutoff_filter()` | `ingest_ts < timestamp '<cutoff_ts>'` (không truyền mốc → `true`) |
| `sim_cutoff()` | `max(sim_ts)` của `order_events` trước mốc — "bây giờ" theo giờ mô phỏng |
| `closed_sim_days()` | `order_purchase_timestamp < date_trunc('day', sim_cutoff())` |

### Silver

| Model | Kiểu | Nguồn |
|---|---|---|
| `silver_orders` | table | `bronze.orders` ∪ sự kiện `seq` cao nhất mỗi đơn trong `order_events` |
| `silver_customers` | table | `bronze.customers` ∪ bản mới nhất mỗi khách trong `customer_events` |
| `silver_payments` | table | `bronze.payments` ∪ `payment_events`, gộp 1 dòng/đơn |
| `silver_order_facts` | incremental MERGE, khoá `(order_id, order_item_id)` | dòng hàng ⋈ đơn ⋈ thanh toán ⋈ đánh giá |
| `snap_products` | snapshot SCD2 | `bronze.products` |

Ở chế độ incremental, `silver_order_facts` chọn lại mọi đơn có sự kiện đơn hàng,
thanh toán hoặc đánh giá với `ingest_ts` trong `[prev_cutoff_ts, cutoff_ts)` —
đơn cũ đổi trạng thái hay có đánh giá mới đều được cập nhật.

### Gold

| Model | Nguồn | Lọc |
|---|---|---|
| `gold_revenue_by_state` | `silver_order_facts` ⋈ `silver_customers` | `delivered`, `shipped`, `invoiced` + ngày mô phỏng đã đóng |
| `gold_revenue_by_category` | `silver_order_facts` ⋈ `bronze.products` ⋈ `bronze.category_translation` | như trên |
| `gold_seller_scorecard` | `silver_order_facts` ⋈ `bronze.sellers` ⋈ `bronze.products` | `delivered` + ngày mô phỏng đã đóng |

Hai bảng doanh thu có cột `is_final`: tháng chốt hẳn khi đã qua hết tháng + 30
ngày mô phỏng.

### Test

39 node mỗi lần `dbt build`: 31 test + 7 model + 1 snapshot. Gồm `not_null`,
`unique`, `accepted_values`, `unique_combination_of_columns`,
`assert_no_duplicate_order_id`, và `assert_revenue_reconciles` (tổng doanh thu
silver bằng tổng gold, cùng bộ lọc).

---

## Bảo trì Iceberg

Bên nào ghi bảng thì bên đó bảo trì bảng.

| Bảng | Ai bảo trì | Khi nào | Engine |
|---|---|---|---|
| `silver.orders_live` | Spark Streaming | mỗi 20 lô | Spark procedures |
| `bronze.*_events` | Airflow `maintain_bronze_events` | sau mỗi lần chốt | Trino |
| `silver.silver_order_facts` | Airflow `maintain_silver_facts` | sau mỗi lần chốt | Trino |
| 9 bảng bronze lịch sử | `ingest_bronze.py` | sau mỗi lần nạp | Spark (`compute_table_stats`) |

**`orders_live` (Spark):**

| # | Lệnh | Mục đích |
|---|---|---|
| 1 | `DELETE WHERE updated_at < max(ops.watermark.cutoff_ts)` | bỏ đơn đã chốt vào `silver_orders` |
| 2 | `rewrite_data_files` | gộp file nhỏ |
| 3 | `rewrite_position_delete_files` | gộp delete file |
| 4 | `expire_snapshots` cũ hơn 1 giờ, giữ tối thiểu 5 | bỏ snapshot cũ |

**`bronze.*_events` (Trino):**

| # | Lệnh | Phạm vi |
|---|---|---|
| 1 | `optimize WHERE ingest_ts < '<ngày của mốc> 00:00'` | chỉ partition đã đóng |
| 2 | `optimize_manifests` | gộp manifest |
| 3 | `expire_snapshots(retention_threshold => '1d')` | giữ time travel 1 ngày |
| 4 | `remove_orphan_files(retention_threshold => '1d')` | sau expire |

**`silver_order_facts` (Trino):**

| # | Lệnh | Mục đích |
|---|---|---|
| 1 | `optimize` | gộp file nhỏ, áp delete file vào data file |
| 2 | `optimize_manifests` | gộp manifest |
| 3 | `expire_snapshots(retention_threshold => '7d')` | giữ time travel 7 ngày |
| 4 | `remove_orphan_files(retention_threshold => '7d')` | sau expire |
| 5 | `ANALYZE` | thống kê mới cho CBO |

Bảo trì tay mọi bảng: `docker compose run --rm trino-cli -f /sql/maintenance/iceberg_maintenance.sql`
(`ANALYZE` → `optimize` → `expire_snapshots` → `remove_orphan_files`).

---

## Cấu hình tài nguyên

`mem_limit` / `cpus` là **trần** của container trong compose.

| Container | RAM | CPU | Cấu hình bên trong |
|---|---|---|---|
| `trino-coordinator` | 3 GB | 0,5 | heap `-Xmx2500m`; `query.max-memory-per-node=1GB`; không nhận task |
| `trino-worker-1/2` | 4 GB | 1,0 | heap `-Xmx3g`; `query.max-memory-per-node=2GB`; headroom 0,8 GB; `task.concurrency=2`; spill 4 GB/query |
| cụm Trino | — | — | `query.max-memory=6GB` cho một query trên toàn cụm |
| `spark-master` | 768 MB | 0,3 | daemon 384 MB |
| `spark-worker-1/2` | 2 GB | 1,0 | daemon 384 MB; nhường **1 core / 1 GB** cho executor |
| executor Spark | — | — | `spark.executor.cores=1`, `spark.executor.memory=1g` |
| `spark-submit` | 1,5 GB | 0,5 | driver 768 MB; `cores.max=2`; `shuffle.partitions=8`; AQE bật |
| `spark-streaming` | 1,5 GB | 0,5 | driver 768 MB; `cores.max=1`; `shuffle.partitions=2` |
| `redpanda` | 1,5 GB | 0,5 | `--smp 1` |
| `datagen` | 384 MB | 0,2 | — |
| `airflow` | 2 GB | 0,5 | LocalExecutor, `PARALLELISM=2` |
| `minio` | 1 GB | 0,2 | — |
| `iceberg-rest` | 512 MB | 0,2 | — |
| `postgres-ops` | 1 GB | 0,3 | — |
| `superset` | 1,5 GB | 0,4 | — |

Chia việc theo tài nguyên:

* Spark Streaming giữ **một** executor; job batch (`spark-submit`) lấy executor
  còn lại — hai loại ứng dụng chạy cùng lúc không giành core của nhau.
* Trong Trino, resource group giới hạn số query đồng thời và phần RAM của từng
  nhóm; FTE và spill cho phép query lớn vượt bộ nhớ một node mà không lỗi.

---

## Vận hành

### Bật

```powershell
cd C:\Users\USER\Documents\TrinoDemo

# Lần đầu: hạ tầng + Trino, nạp lịch sử, dựng silver/gold
docker compose --profile core up -d
docker compose run --rm spark-submit jobs/land_raw.py
docker compose run --rm spark-submit jobs/ingest_bronze.py
docker compose run --rm dbt build

# Luồng stream + Airflow
docker compose --profile stream up -d
docker compose run --rm trino-cli -f /sql/streaming/silver_views.sql    # tạo view, một lần

# BI
docker compose --profile full up -d
```

| Profile | Gồm |
|---|---|
| `core` | MinIO, postgres-ops, iceberg-rest, Trino |
| `spark` | hạ tầng + cụm Spark |
| `stream` | hạ tầng + Trino + Spark + Redpanda + datagen + spark-streaming + Airflow |
| `full` | `core` + Superset |
| `tools` | `dbt`, `trino-cli`, `trino-cli-analyst`, `spark-submit` (chạy theo yêu cầu) |

Các service stream đều `restart: unless-stopped`: mở Docker là cả luồng tự chạy
tiếp, đồng hồ mô phỏng nối tiếp, Airflow chạy bù các lần chốt đã lỡ.

### Theo dõi

```powershell
docker logs -f trinodemo-datagen            # giờ mô phỏng, số đơn, số sự kiện
docker logs -f trinodemo-spark-streaming    # từng lô: số sự kiện, thời gian, bảo trì
docker logs -f trinodemo-airflow
```

```sql
-- query đang chạy trên Trino, theo resource group
SELECT query_id, state, resource_group_id, source, substr(query, 1, 80)
FROM system.runtime.queries WHERE state IN ('RUNNING', 'QUEUED');

-- đơn mới nhất, truy vấn tức thì
SELECT order_status, count(*) FROM lakehouse.silver.v_orders_now
WHERE order_purchase_timestamp >= TIMESTAMP '2018-10-18 00:00:00' GROUP BY 1;

-- các lần chốt
SELECT * FROM lakehouse.ops.watermark ORDER BY cutoff_ts;
```

| UI | Địa chỉ | Đăng nhập |
|---|---|---|
| Trino | <http://localhost:8080> | `admin` |
| Airflow | <http://localhost:8090> | không cần |
| Spark master | <http://localhost:8082> | — |
| Spark Streaming driver | <http://localhost:4040> | — |
| MinIO | <http://localhost:9001> | `minioadmin` / `minioadmin123` |
| Superset | <http://localhost:8088> | `admin` / `admin` |

### Biến `.env` của pipeline

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `TIME_SCALE` | 60 | 1 phút thật = 60 phút mô phỏng |
| `SIM_START` | 2018-10-18T00:00:00 | điểm bắt đầu đồng hồ mô phỏng (chỉ dùng lần đầu) |
| `STREAM_TRIGGER` | 30 seconds | chu kỳ lô Spark Streaming |
| `CHECKPOINT_BUCKET` | checkpoints | bucket checkpoint |
| `TRINO_RETRY_POLICY` | TASK | chế độ FTE của Trino |
| `SPARK_VERSION` / `REDPANDA_VERSION` / `AIRFLOW_VERSION` | 3.5.9 / v26.2.3 / 3.3.2 | phiên bản image |

### Tắt

```powershell
docker compose --profile stream --profile full stop    # dừng, giữ dữ liệu
docker compose --profile stream --profile full down    # xoá container, giữ volume
```

---

## Kết quả kiểm chứng

Số liệu từ lần chạy thật.

| Phần | Kết quả |
|---|---|
| Spark: CSV → raw | 9 file (120,3 MB) trong 23,8 s; MD5 trên MinIO khớp file gốc 9/9 |
| Spark: raw → bronze | 9 bảng trong 199,1 s, 68 task chia 2 executor (36 / 32). orders 99.441 · order_items 112.650 · payments 103.886 · reviews 99.224 · customers 99.441 · sellers 3.095 · geolocation 1.000.163 · products 32.951 · category_translation 71 |
| Spark: thống kê cho Trino | `customer_unique_id` NDV ước 96.122, thật 96.096 |
| Spark Streaming: Redpanda → bronze | 17.649 / 17.649 sự kiện, 0 thiếu, 0 thừa, 0 trùng — kể cả sau khi tắt toàn bộ Docker giữa chừng và một lô bị phát lại |
| Spark Streaming: `orders_live` | khớp trạng thái `seq` cao nhất trong bronze: 2.709 / 2.709 đơn |
| Độ trễ stream | datagen gửi → bronze: p50 16,3 s · p95 28,9 s |
| Đồng hồ mô phỏng | tắt cả stack 4,5 phút, bật lại chạy tiếp đúng giờ mô phỏng |
| Trino FTE | giết `worker-2` giữa query 48 triệu dòng: `failedTasks=1`, query **FINISHED**, kết quả y hệt lần không bị giết |
| Trino resource group | dbt → `[global, etl]`, bảo trì Airflow → `[global, maintenance]`, Superset → `[global, bi]`, còn lại → `[global, adhoc]` |
| Trino CBO | `SHOW STATS` trả NDV thật trên bảng do Spark ghi (`order_id` 99.068, `order_status` 8) |
| Trino dynamic filtering | `dynamicFilterAssignments = {customer_id_0 -> #df_325}` khi join `orders` ⋈ `customers` |
| Trino phân quyền | `analyst` thấy `29a1****MASKED****` và chỉ bang SP; `admin` thấy đủ |
| Spark ⇄ Trino commit đồng thời | Spark append gặp `optimize` của Trino trên cùng bảng → `CommitFailedException`, tự retry, lô thành công |
| Airflow: lần chốt mốc 2026-10-05 08:00 UTC | `dbt build` PASS 39/39; 7.002 / 7.002 đơn stream vào `silver_orders`, 0 lệch trạng thái, 0 đơn sau mốc lọt vào; 7.920 / 7.920 dòng hàng vào `silver_order_facts` |
| Airflow: lần chốt mốc 08:49 UTC | 925 đơn cũ đổi trạng thái giữa hai mốc → cả 925 được cập nhật trong `silver_order_facts` |
| Gold | thêm tháng 10–11/2018 với `is_final = false`; tháng 11 có 3.303 đơn = đếm lại trên các ngày mô phỏng đã đóng; 278 dòng của ngày chưa đóng nằm ngoài gold |
| View tức thì | `v_orders_now` 106.856 đơn = số đơn khác nhau trên mọi nguồn; trên 7.812 đơn stream đã ổn định: 0 thiếu, 0 trùng, 0 lệch trạng thái so với `seq` cao nhất trong bronze |
| Spark dọn `orders_live` theo `ops.watermark` | mốc 08:00: 7.586 → 1.634 đơn; mốc 08:49: 2.145 → 881 đơn — chỉ giữ đơn có sự kiện sau mốc, view không mất đơn nào |
| Bảo trì `bronze.order_events` | 941 → 327 file sau khi nén partition đã đóng |
| Bảo trì `silver_order_facts` | 62 file (18 delete file) → 26 file (0 delete file) trong ~1,5 phút; dữ liệu giữ nguyên |

---

## Cây thư mục

```
TrinoDemo/
├── docker-compose.yml               mọi service, chia theo profile
├── .env / .env.example              phiên bản image, mật khẩu, tham số stream
├── data/                            9 file CSV Olist
│
├── spark/
│   ├── Dockerfile                   Spark 3.5.9 + Iceberg + s3a + Kafka connector
│   ├── conf/spark-defaults.conf     master, executor, catalog `lakehouse` (REST), s3a
│   └── jobs/
│       ├── land_raw.py              data/*.csv -> s3://raw/olist/
│       ├── ingest_bronze.py         s3://raw/olist/ -> 9 bảng bronze
│       └── stream_olist.py          Redpanda -> bronze.*_events + silver.orders_live
│
├── trino/
│   ├── coordinator/                 config (CBO, DF, FTE, spill), jvm, exchange-manager,
│   │                                resource-groups.json, access-control
│   ├── worker/                      config (spill, memory revoking), jvm
│   ├── catalog/                     lakehouse, tpch, faker, jmx
│   └── security/rules.json          phân quyền admin / analyst
│
├── datagen/                         datagen.py, scenario.yml, Dockerfile
├── airflow/                         Dockerfile (Airflow + dbt venv), start.sh, dags/olist_daily.py
├── dbt/olist/                       models/{silver,gold}, snapshots, macros, tests
├── iceberg-rest/Dockerfile          REST catalog + driver PostgreSQL
├── superset/                        image + bootstrap kết nối Trino
├── scripts/set-retry-policy.ps1     đổi retry-policy cho cả cụm Trino
└── sql/
    ├── streaming/silver_views.sql   view truy vấn tức thì
    ├── maintenance/iceberg_maintenance.sql
    └── demo/                        10 file SQL minh hoạ tính năng Trino / Iceberg
```
