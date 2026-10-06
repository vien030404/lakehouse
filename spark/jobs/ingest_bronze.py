"""
Spark job: CSV thô trên MinIO -> 9 bảng bronze Iceberg.

    s3a://raw/olist/*.csv  ──Spark (2 executor)──►  lakehouse.bronze.*

Job tự tạo namespace và bảng nếu chưa có, kiểm tra chất lượng ép kiểu, ghi
dữ liệu rồi sinh thống kê cho CBO của Trino. Các tầng phía trên (silver,
gold) do dbt dựng trên Trino từ chính các bảng này.

Chạy (sau land_raw.py):
    docker compose run --rm spark-submit jobs/ingest_bronze.py
    docker compose run --rm spark-submit jobs/ingest_bronze.py --tables orders customers

Ghi đè nhưng không tạo lại bảng:
    writeTo(...).overwrite(lit(True)) thay toàn bộ dòng trong một snapshot
    mới. Schema, partition spec, sort order và lịch sử snapshot được giữ
    nguyên, nên time travel và rollback_to_snapshot vẫn dùng được, và lần
    nạp lại là nguyên tử với người đọc.

Schema đích là nguồn chuẩn:
    CREATE TABLE chỉ chạy khi bảng chưa tồn tại. Dữ liệu luôn được ép theo
    schema đọc từ chính bảng đích, không theo DDL trong file này, nên không
    thể lệch kiểu với bảng mà dbt đang đọc.
"""

import argparse
import json
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RAW = "s3a://raw/olist"
NAMESPACE = "bronze"

# Ánh xạ kiểu Spark -> Iceberg: STRING -> string, INT -> int, DOUBLE -> double,
# TIMESTAMP_NTZ -> timestamp (không múi giờ).
# Cột thời gian của Olist không mang múi giờ nên dùng TIMESTAMP_NTZ; TIMESTAMP
# thường sẽ tạo timestamptz và Trino thấy một kiểu khác.
DDL = {
    "orders": """
        order_id STRING, customer_id STRING, order_status STRING,
        order_purchase_timestamp TIMESTAMP_NTZ, order_approved_at TIMESTAMP_NTZ,
        order_delivered_carrier_date TIMESTAMP_NTZ,
        order_delivered_customer_date TIMESTAMP_NTZ,
        order_estimated_delivery_date TIMESTAMP_NTZ""",
    "order_items": """
        order_id STRING, order_item_id INT, product_id STRING, seller_id STRING,
        shipping_limit_date TIMESTAMP_NTZ, price DOUBLE, freight_value DOUBLE""",
    "payments": """
        order_id STRING, payment_sequential INT, payment_type STRING,
        payment_installments INT, payment_value DOUBLE""",
    "reviews": """
        review_id STRING, order_id STRING, review_score INT,
        review_comment_title STRING, review_comment_message STRING,
        review_creation_date TIMESTAMP_NTZ, review_answer_timestamp TIMESTAMP_NTZ""",
    "customers": """
        customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix INT,
        customer_city STRING, customer_state STRING""",
    "sellers": """
        seller_id STRING, seller_zip_code_prefix INT, seller_city STRING,
        seller_state STRING""",
    "geolocation": """
        geolocation_zip_code_prefix INT, geolocation_lat DOUBLE,
        geolocation_lng DOUBLE, geolocation_city STRING, geolocation_state STRING""",
    "products": """
        product_id STRING, product_category_name STRING, product_name_lenght INT,
        product_description_lenght INT, product_photos_qty INT,
        product_weight_g INT, product_length_cm INT, product_height_cm INT,
        product_width_cm INT""",
    "category_translation": """
        product_category_name STRING, product_category_name_english STRING""",
}

# Hidden partitioning và sort order cho hai bảng lớn có trục thời gian:
# truy vấn lọc theo ngày/tháng được cắt tỉa partition, và file đã sắp theo
# thời gian đặt hàng giúp Trino bỏ qua file nhờ thống kê min/max.
PARTITION = {
    "orders": "days(order_purchase_timestamp)",
    "reviews": "months(review_creation_date)",
}
WRITE_ORDER = {
    "orders": "order_purchase_timestamp",
}

# bảng -> (file CSV, cột bắt buộc khác NULL, ký tự xuống dòng của file)
#
# Ký tự xuống dòng khai riêng từng file vì dataset Olist không đồng nhất:
#   reviews, category_translation : CRLF (\r\n); reviews còn có xuống dòng
#                                   bên trong nội dung đánh giá
#   7 file còn lại                : LF (\n)
# Nếu không khai lineSep, Spark vẫn tách đúng bản ghi nhưng chuẩn hoá "\r\n"
# bên trong nội dung thành "\n", làm văn bản đánh giá khác bản gốc. Khai CRLF
# cho file LF thì ngược lại: cả file bị đọc thành một dòng.
TABLES = {
    "orders": ("olist_orders_dataset.csv", "order_purchase_timestamp", "\n"),
    "order_items": ("olist_order_items_dataset.csv", None, "\n"),
    "payments": ("olist_order_payments_dataset.csv", None, "\n"),
    "reviews": ("olist_order_reviews_dataset.csv", "review_creation_date", "\r\n"),
    "customers": ("olist_customers_dataset.csv", None, "\n"),
    "sellers": ("olist_sellers_dataset.csv", None, "\n"),
    "geolocation": ("olist_geolocation_dataset.csv", None, "\n"),
    "products": ("olist_products_dataset.csv", None, "\n"),
    "category_translation": ("product_category_name_translation.csv", None, "\r\n"),
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_table(spark, table):
    """Tạo bảng nếu chưa có. Trả về True nếu vừa tạo."""
    target = f"lakehouse.{NAMESPACE}.{table}"
    if spark.catalog.tableExists(target):
        return False

    partition = f"PARTITIONED BY ({PARTITION[table]})" if table in PARTITION else ""
    spark.sql(f"""
        CREATE TABLE {target} ({DDL[table]})
        USING iceberg
        {partition}
        TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet')
    """)
    # Iceberg format v2: phiên bản được cả Spark (Iceberg 1.10) lẫn Trino 483
    # hỗ trợ đầy đủ, gồm position delete cho MERGE/UPDATE/DELETE.
    if table in WRITE_ORDER:
        spark.sql(f"ALTER TABLE {target} WRITE ORDERED BY {WRITE_ORDER[table]}")
    return True


def read_raw(spark, fname, line_sep):
    # multiLine + escape: văn bản đánh giá có xuống dòng bên trong dấu nháy;
    # thiếu hai tuỳ chọn này, một bản ghi bị tách thành nhiều dòng.
    # FAILFAST: dòng sai định dạng làm job dừng thay vì bị bỏ qua lặng lẽ.
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("lineSep", line_sep)
        .option("mode", "FAILFAST")
        .csv(f"{RAW}/{fname}")
    )


def check_cast_loss(raw, schema, table):
    """Dừng job nếu ép kiểu biến một ô có giá trị thành NULL.

    Khi ANSI mode tắt, Spark ép chuỗi không hợp lệ (ví dụ "225.0" sang INT)
    thành NULL mà không báo lỗi. Hàm này đếm, với mỗi cột không phải chuỗi,
    số ô khác NULL trong CSV nhưng NULL sau khi ép kiểu. Ô rỗng trong CSV vốn
    đã là NULL nên không bị tính.
    """
    typed = [f for f in schema if not isinstance(f.dataType, StringType)]
    if not typed:
        return
    lost = raw.agg(*[
        F.sum((F.col(f.name).isNotNull() & F.col(f.name).cast(f.dataType).isNull())
              .cast("int")).alias(f.name)
        for f in typed
    ]).first().asDict()
    bad = {c: n for c, n in lost.items() if n}
    if bad:
        raise SystemExit(f"{table}: ép kiểu làm mất giá trị ở {bad}")


def ingest(spark, table):
    fname, not_null, line_sep = TABLES[table]
    target = f"lakehouse.{NAMESPACE}.{table}"

    created = ensure_table(spark, table)
    schema = spark.table(target).schema
    raw = read_raw(spark, fname, line_sep)

    missing = [f.name for f in schema if f.name not in raw.columns]
    if missing:
        raise SystemExit(f"{fname} thiếu cột {missing} so với {target}")

    check_cast_loss(raw, schema, table)

    df = raw.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema])
    if not_null:
        df = df.where(F.col(not_null).isNotNull())

    started = time.time()
    df.writeTo(target).overwrite(F.lit(True))

    # Thống kê NDV cho CBO của Trino. Spark không tự thu NDV khi ghi; thủ tục
    # này ghi theta sketch vào file Puffin gắn với snapshot vừa commit, đúng
    # định dạng Trino đọc, nên Trino có ngay số liệu để chọn thứ tự join.
    spark.sql(f"CALL lakehouse.system.compute_table_stats(table => '{NAMESPACE}.{table}')")

    rows = spark.table(target).count()
    snap = spark.sql(
        f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    log(
        f"  {target:40} {rows:>10,} dòng  {time.time() - started:5.1f}s  "
        f"snapshot {snap.snapshot_id}{'  (bảng MỚI TẠO)' if created else ''}"
    )


def report_executors(spark):
    """In số task và dung lượng đọc của từng executor, lấy từ REST API của
    driver, để thấy công việc được chia cho các worker. Phải gọi trước
    spark.stop() vì UI của driver tắt cùng ứng dụng.
    """
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/executors"
    try:
        executors = json.load(urllib.request.urlopen(url, timeout=10))
    except Exception as exc:  # báo cáo là phụ, không để nó làm hỏng job đã ghi xong
        log(f"  (không đọc được {url}: {exc})")
        return

    log("Executor đã làm việc:")
    for e in sorted(executors, key=lambda x: x["id"]):
        role = "driver  " if e["id"] == "driver" else f"executor {e['id']}"
        log(
            f"  {role:11} {e['hostPort']:22} task xong {e['completedTasks']:>4}  "
            f"đọc {e['totalInputBytes'] / 1048576:7.1f} MB"
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--tables", nargs="*", choices=list(TABLES), default=list(TABLES))
    args = ap.parse_args()

    spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()
    sc = spark.sparkContext
    # Giảm log INFO của Iceberg để log tiến độ dễ đọc
    sc.setLogLevel("WARN")
    log(f"Ứng dụng {sc.applicationId} trên {sc.master}")
    log(f"Nguồn {RAW}/  ->  lakehouse.{NAMESPACE}.*")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{NAMESPACE}")

    started = time.time()
    for t in args.tables:
        ingest(spark, t)
    log(f"XONG {len(args.tables)} bảng sau {time.time() - started:.1f}s")

    report_executors(spark)
    spark.stop()


if __name__ == "__main__":
    main()
