"""
Spark job: chép 9 file CSV Olist từ host lên vùng raw trên MinIO.

    data/*.csv  ──driver──►  s3a://raw/olist/*.csv

Chạy:
    docker compose run --rm spark-submit jobs/land_raw.py

Đây là thao tác chép file, không phải xử lý phân tán: chỉ driver thực hiện,
qua Hadoop FileSystem API, chép nguyên byte mà không đọc nội dung. Thư mục
data/ chỉ được mount vào container spark-submit; worker không cần truy cập
host. Việc đọc và ép kiểu (phần nặng) nằm ở ingest_bronze.py, chạy trên các
executor.

Giữ vùng raw nguyên bản cho phép nạp lại bronze bất kỳ lúc nào mà không cần
nguồn gốc, và tách bước "đưa file vào hệ thống" khỏi bước "biến file thành
bảng".

Bucket `raw` được container minio-init tạo sẵn khi khởi động cụm.
"""

import time

from pyspark.sql import SparkSession

LOCAL_DIR = "/opt/spark/work-dir/data"
RAW = "s3a://raw/olist"

FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    spark = SparkSession.builder.appName("land_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    log(f"Chép {len(FILES)} file: {LOCAL_DIR}/  ->  {RAW}/")
    started = time.time()
    total = 0
    for name in FILES:
        src = Path(f"file://{LOCAL_DIR}/{name}")
        dst = Path(f"{RAW}/{name}")
        src_fs = src.getFileSystem(conf)
        dst_fs = dst.getFileSystem(conf)

        if not src_fs.exists(src):
            raise SystemExit(f"Thiếu {LOCAL_DIR}/{name} - thư mục data/ đã có đủ 9 file chưa?")

        # deleteSource=False, overwrite=True: job idempotent, chạy lại an toàn
        jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src, dst_fs, dst, False, True, conf)

        size = dst_fs.getFileStatus(dst).getLen()
        total += size
        log(f"  {name:42} {size / 1048576:7.1f} MB")

    log(f"XONG {total / 1048576:.1f} MB sau {time.time() - started:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
