"""
Spark Structured Streaming: Redpanda -> bronze (nhật ký thay đổi) + silver (trạng thái mới nhất).

    olist.* (Redpanda) ──mỗi lô──► ① lakehouse.bronze.*_events   append
                                  ② lakehouse.silver.orders_live MERGE

Chạy như một service thường trực (spark-streaming trong docker-compose.yml);
job không tự kết thúc.

Thứ tự ghi trong mỗi lô: bronze trước, silver sau. Nhờ vậy silver không bao
giờ chứa dữ liệu mà bronze chưa có, và mọi thứ ở silver đều dựng lại được từ
bronze.

Đảm bảo không mất, không lặp:
  - Checkpoint trên s3a://checkpoints/ lưu offset Redpanda đã xử lý.
  - Mỗi commit bronze gắn `stream_batch_id` vào thuộc tính snapshot. Nếu
    driver chết sau khi commit nhưng trước khi lưu offset, lô đó được phát
    lại; bảng nào đã có snapshot mang batch_id này thì được bỏ qua.
  - event_id được khử trùng trong cửa sổ 10 phút (producer có thể gửi lại
    sau sự cố).
  - orders_live chỉ cập nhật khi `seq` mới lớn hơn, nên lô phát lại, sự kiện
    đến trễ hay sai thứ tự đều không ghi đè được trạng thái mới hơn.

Hai trục thời gian:
  payload / sim_ts   : thời gian mô phỏng (nghiệp vụ) -> phân tích
  event_ts, ingest_ts: thời gian thật (kỹ thuật) -> ingest_ts là mốc cắt của Airflow
"""

import os
import threading
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, MapType, StringType, StructField, StructType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
TRIGGER = os.environ.get("STREAM_TRIGGER", "30 seconds")
CHECKPOINT = os.environ.get("STREAM_CHECKPOINT", "s3a://checkpoints/stream_olist")
MAX_OFFSETS = os.environ.get("STREAM_MAX_OFFSETS", "20000")
# Chu kỳ bảo trì orders_live (tính theo số lô). MERGE mỗi 30 giây sinh file
# nhỏ và delete file rất nhanh; chu kỳ thưa hơn làm thời gian mỗi lô tăng rõ.
MAINTAIN_EVERY = int(os.environ.get("STREAM_MAINTAIN_EVERY", "20"))
WATCHDOG_S = int(os.environ.get("STREAM_WATCHDOG_SECONDS", "600"))

# topic -> (bảng bronze lịch sử dùng làm schema, bảng sự kiện đích)
TOPICS = {
    "olist.customers": ("customers", "customer_events"),
    "olist.orders": ("orders", "order_events"),
    "olist.order_items": ("order_items", "order_item_events"),
    "olist.payments": ("payments", "payment_events"),
    "olist.reviews": ("reviews", "review_events"),
}
PARAMS_TOPIC = "olist.datagen_params"

# Cột kỹ thuật thêm vào mọi bảng sự kiện
TECH_DDL = """event_id STRING, event_type STRING, seq INT, event_ts TIMESTAMP_NTZ,
    sim_ts TIMESTAMP_NTZ, ingest_ts TIMESTAMP_NTZ, kafka_partition INT,
    kafka_offset BIGINT, batch_id BIGINT"""

# Phong bì JSON chung của mọi sự kiện datagen; payload là map chuỗi và được
# ép kiểu theo schema bảng bronze tương ứng.
ENVELOPE = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("sim_ts", StringType()),
    StructField("seq", IntegerType()),
    StructField("payload", MapType(StringType(), StringType())),
])


def log(msg):
    print(f"[{datetime.now(timezone.utc).replace(tzinfo=None):%H:%M:%S}] {msg}", flush=True)


def ensure_tables(spark):
    """Tạo các bảng đích nếu chưa có. Cột nghiệp vụ của bảng sự kiện sao từ
    bảng bronze lịch sử tương ứng, nên dbt hợp nhất hai nguồn bằng UNION."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    props = "TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')"
    for base, evt in TOPICS.values():
        cols = ", ".join(f"{f.name} {f.dataType.simpleString()}"
                         for f in spark.table(f"lakehouse.bronze.{base}").schema)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS lakehouse.bronze.{evt} ({cols}, {TECH_DDL})
                      USING iceberg PARTITIONED BY (days(ingest_ts)) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS lakehouse.bronze.datagen_params
                  (sim_date DATE, params_json STRING, {TECH_DDL})
                  USING iceberg PARTITIONED BY (days(ingest_ts)) {props}""")

    # orders_live có cùng cột với silver_orders (do dbt dựng) cộng hai cột
    # theo dõi, để view hợp nhất chỉ cần UNION ALL. Merge-on-read: mỗi MERGE
    # chỉ ghi thêm delete file nhỏ thay vì viết lại cả file dữ liệu.
    cols = ", ".join(f"{f.name} {f.dataType.simpleString()}"
                     for f in spark.table("lakehouse.silver.silver_orders").schema)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS lakehouse.silver.orders_live
                  ({cols}, last_seq INT, updated_at TIMESTAMP_NTZ)
                  USING iceberg PARTITIONED BY (days(order_purchase_timestamp))
                  TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                                 'write.merge.mode'='merge-on-read',
                                 'write.update.mode'='merge-on-read',
                                 'write.delete.mode'='merge-on-read')""")

    # Mỗi commit Iceberg ghi một metadata.json mới chứa toàn bộ danh sách
    # snapshot, và mặc định các bản cũ được giữ lại. Với một commit mỗi 30
    # giây trên mỗi bảng, số file và kích thước metadata tăng tuyến tính. Chỉ
    # giữ 20 bản gần nhất; bản cũ hơn bị xoá ngay sau mỗi commit.
    for table in [f"lakehouse.bronze.{evt}" for _, evt in TOPICS.values()] + [
            "lakehouse.bronze.datagen_params", "lakehouse.silver.orders_live"]:
        spark.sql(f"""ALTER TABLE {table} SET TBLPROPERTIES (
                          'write.metadata.delete-after-commit.enabled'='true',
                          'write.metadata.previous-versions-max'='20')""")


def committed(spark, table, batch_id):
    """True nếu bảng đã có snapshot do lô `batch_id` ghi (lô đang được phát lại)."""
    return spark.sql(
        f"SELECT 1 FROM {table}.snapshots WHERE summary['stream_batch_id'] = '{batch_id}' LIMIT 1"
    ).count() > 0


def tech(batch_id, ingest_ts):
    return [
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.seq").alias("seq"),
        F.to_timestamp_ntz(F.col("e.event_ts")).alias("event_ts"),
        F.to_timestamp_ntz(F.col("e.sim_ts")).alias("sim_ts"),
        F.lit(ingest_ts).cast("timestamp_ntz").alias("ingest_ts"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
    ]


def append(df, table, batch_id):
    (df.writeTo(table)
       .option("snapshot-property.stream_batch_id", str(batch_id))
       .append())


def merge_orders_live(orders, ingest_ts, target_cols):
    """Upsert trạng thái mới nhất của từng đơn vào orders_live. Các cột phái
    sinh được tính giống dbt/silver_orders.sql để hai bảng ghép được bằng
    UNION ALL."""
    w = Window.partitionBy("order_id").orderBy(F.col("seq").desc())
    latest = (orders.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn"))

    def days(a, b):  # cùng ngữ nghĩa với date_diff('day', a, b) của Trino: cắt phần lẻ
        return ((F.unix_micros(F.col(b).cast("timestamp")) - F.unix_micros(F.col(a).cast("timestamp")))
                / F.lit(86_400_000_000)).cast("bigint")

    src = latest.select(
        "order_id", "customer_id",
        F.lower(F.trim("order_status")).alias("order_status"),
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
        days("order_purchase_timestamp", "order_delivered_customer_date").alias("delivery_days"),
        days("order_estimated_delivery_date", "order_delivered_customer_date").alias("delivery_delay_days"),
        F.date_trunc("month", "order_purchase_timestamp").cast("timestamp_ntz").alias("order_month"),
        F.col("seq").alias("last_seq"),
        F.lit(ingest_ts).cast("timestamp_ntz").alias("updated_at"),
    )
    src.select(*target_cols).createOrReplaceTempView("orders_src")
    # Trong foreachBatch, mỗi lô chạy trên một bản sao của SparkSession, và
    # view tạm thuộc về phiên đó. MERGE phải được gọi qua đúng phiên của
    # DataFrame; gọi qua session gốc sẽ không thấy view (TABLE_OR_VIEW_NOT_FOUND).
    src.sparkSession.sql("""
        MERGE INTO lakehouse.silver.orders_live t
        USING orders_src s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.last_seq > t.last_seq THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def maintain(spark):
    """Bảo trì orders_live giữa hai lô.

    Bảng này chỉ do job này ghi, nên bảo trì ngay trong job đảm bảo không có
    MERGE nào chạy song song. Nếu nén từ một engine khác trong lúc MERGE diễn
    ra mỗi 30 giây, hai bên sẽ thường xuyên xung đột commit.
    """
    t = "silver.orders_live"
    try:
        # Đơn đã được Airflow chốt vào silver chuẩn (updated_at < mốc) không
        # cần giữ ở bảng live nữa: view hợp nhất lấy chúng từ bảng chuẩn. Nếu
        # đơn có sự kiện mới sau mốc, MERGE sẽ chèn lại đơn với trạng thái đầy
        # đủ mới nhất.
        if spark.catalog.tableExists("lakehouse.ops.watermark"):
            cutoff = spark.sql("SELECT max(cutoff_ts) AS c FROM lakehouse.ops.watermark").first().c
            if cutoff is not None:
                before = spark.table(f"lakehouse.{t}").count()
                spark.sql(f"DELETE FROM lakehouse.{t} WHERE updated_at < TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}'")
                after = spark.table(f"lakehouse.{t}").count()
                log(f"  dọn orders_live theo mốc {cutoff:%Y-%m-%d %H:%M}: {before} -> {after} đơn")
        spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{t}', "
                  "options => map('min-input-files','5'))")
        spark.sql(f"CALL lakehouse.system.rewrite_position_delete_files(table => '{t}')")
        # Thủ tục expire_snapshots chỉ nhận hằng số cho older_than (biểu thức
        # như current_timestamp() - INTERVAL 1 HOUR gây AnalysisException), nên
        # mốc được tính sẵn trong Python.
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{t}', "
                  f"older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}', retain_last => 5)")
        log("  bảo trì orders_live: nén file + gộp delete file + expire snapshot > 1 giờ")
    except Exception as exc:  # lỗi bảo trì không được làm dừng luồng; thử lại ở chu kỳ sau
        log(f"  bảo trì orders_live lỗi, bỏ qua lần này: {exc!r}"[:600])


def make_processor(spark):
    # Schema đọc một lần lúc khởi động: mỗi lời gọi spark.table(...) là một
    # lượt hỏi REST catalog, gọi lại mỗi lô làm tăng đáng kể thời gian lô.
    schemas = {base: spark.table(f"lakehouse.bronze.{base}").schema for base, _ in TOPICS.values()}
    live_cols = [f.name for f in spark.table("lakehouse.silver.orders_live").schema]
    # Chỉ lô đầu tiên sau khi khởi động mới có thể là lô phát lại (Spark phát
    # lại đúng lô cuối chưa được ghi nhận), nên chỉ lô đó cần kiểm tra snapshot.
    first = {"batch": None}

    def process(batch, batch_id):
        if first["batch"] is None:
            first["batch"] = batch_id
        maybe_replay = batch_id == first["batch"]
        started = time.time()
        ingest_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        env = (batch.select("topic", "partition", "offset",
                            F.from_json("value", ENVELOPE).alias("e"))
                    .persist())
        counts = {r["topic"]: r["count"] for r in env.groupBy("topic").count().collect()}
        if not counts:
            env.unpersist()
            return

        # ① bronze: nhật ký thay đổi, append
        orders_df = None
        for topic, (base, evt) in TOPICS.items():
            if not counts.get(topic):
                continue
            table = f"lakehouse.bronze.{evt}"
            schema = schemas[base]
            df = env.where(F.col("topic") == topic).select(
                *[F.col("e.payload").getItem(f.name).cast(f.dataType).alias(f.name) for f in schema],
                *tech(batch_id, ingest_ts),
            )
            if topic == "olist.orders":
                orders_df = df.persist()
            if not (maybe_replay and committed(spark, table, batch_id)):
                append(df, table, batch_id)

        if counts.get(PARAMS_TOPIC) and not (
                maybe_replay and committed(spark, "lakehouse.bronze.datagen_params", batch_id)):
            df = env.where(F.col("topic") == PARAMS_TOPIC).select(
                F.to_date(F.col("e.payload").getItem("sim_date")).alias("sim_date"),
                F.col("e.payload").getItem("params_json").alias("params_json"),
                *tech(batch_id, ingest_ts),
            )
            append(df, "lakehouse.bronze.datagen_params", batch_id)

        # ② silver: trạng thái mới nhất, MERGE có điều kiện theo seq
        if orders_df is not None:
            merge_orders_live(orders_df, ingest_ts, live_cols)
            orders_df.unpersist()

        env.unpersist()
        summary = "  ".join(f"{t.split('.')[-1]}={n}" for t, n in sorted(counts.items()))
        log(f"lô {batch_id}: {summary}  ({time.time() - started:.1f}s)")

        if batch_id and batch_id % MAINTAIN_EVERY == 0:
            maintain(spark)

    return process


def main():
    spark = SparkSession.builder.appName("stream_olist").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    log(f"Đọc {BOOTSTRAP}  topic olist.*  mỗi {TRIGGER}  checkpoint {CHECKPOINT}")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", r"olist\..*")
        .option("startingOffsets", "earliest")
        # Giới hạn kích thước lô: sau thời gian dừng dài, backlog được tiêu
        # qua nhiều lô vừa phải thay vì một lô khổng lồ.
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .select("topic", "partition", "offset", "timestamp",
                F.col("value").cast("string").alias("value"))
        .withColumn("event_id", F.get_json_object("value", "$.event_id"))
        # Khử trùng sự kiện gửi lại trong cửa sổ 10 phút. Bản gửi lại muộn hơn
        # vẫn vào bronze nhưng dbt khử trùng lần nữa theo event_id, còn
        # orders_live được bảo vệ bởi điều kiện seq.
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    query = (
        stream.writeStream.queryName("stream_olist")
        .foreachBatch(make_processor(spark))
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    start_watchdog(query)
    query.awaitTermination()


def start_watchdog(query, limit_s=WATCHDOG_S):
    """Phát hiện luồng bị treo và thoát tiến trình để Docker khởi động lại.

    Treo được định nghĩa là: đang có lô chạy nhưng số thứ tự lô (batchId)
    không đổi trong `limit_s` giây. Luồng treo không ném exception nên chính
    sách `restart: unless-stopped` không tự kích hoạt; watchdog thoát với mã
    lỗi, Docker dựng lại container và luồng chạy tiếp từ checkpoint (phát lại
    an toàn nhờ stream_batch_id và điều kiện seq).

    Không dùng riêng cờ isTriggerActive: khi mỗi lô mất lâu hơn chu kỳ
    trigger, lô sau bắt đầu ngay khi lô trước xong nên cờ luôn bật dù luồng
    vẫn khoẻ. batchId thay đổi mới là bằng chứng có tiến triển.
    """
    def watch():
        last_id, since = None, time.time()
        while True:
            time.sleep(15)
            try:
                progress = query.lastProgress
                batch_id = progress["batchId"] if progress else None
                active = query.status.get("isTriggerActive", False)
            except Exception:
                continue
            if batch_id != last_id or not active:
                last_id, since = batch_id, time.time()
            elif time.time() - since > limit_s:
                log(f"WATCHDOG: lô sau {last_id} chạy quá {limit_s} giây không xong - coi là treo, thoát")
                os._exit(3)

    threading.Thread(target=watch, name="watchdog", daemon=True).start()


if __name__ == "__main__":
    main()
