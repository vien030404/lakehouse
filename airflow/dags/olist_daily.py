"""
olist_daily - chốt dữ liệu stream hằng ngày vào silver chuẩn và gold.

    resolve_cutoff ─► wait_commit_margin ─► dbt_build ─► publish_watermark ─┬─► maintain_bronze_events
                                                                            └─► maintain_silver_facts

Chạy lúc 00:00 giờ Việt Nam. Mốc cắt là cuối khoảng lịch (00:00 hôm nay),
quy về UTC vì ingest_ts được lưu theo UTC.

Hai trục thời gian:
  Mốc cắt áp lên ingest_ts - thời điểm thật Spark ghi sự kiện vào bronze -
  nên mỗi sự kiện vào silver/gold đúng một lần: ở lần chốt đầu tiên có mốc
  lớn hơn ingest_ts của nó. Gold nhóm theo thời gian nghiệp vụ (mô phỏng)
  và chỉ công bố các ngày mô phỏng đã đóng.

Bỏ lỡ lịch chạy:
  catchup=True - khi Airflow chạy lại sau thời gian dừng, các ngày bị lỡ được
  chạy bù tuần tự, mỗi lần với mốc của ngày đó. Sự kiện ghi sau một mốc tự
  rơi vào lần chốt kế tiếp.

Chạy tay (cutoff_ts theo UTC; bỏ trống thì dùng thời điểm hiện tại trừ 10 phút):
  airflow dags trigger olist_daily -c '{"cutoff_ts": "2026-10-05 08:00:00"}'
"""

from datetime import datetime, timedelta, timezone

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

TRINO = dict(host="trino-coordinator", port=8080, user="admin", source="airflow")
FMT = "%Y-%m-%d %H:%M:%S"
EVENT_TABLES = ["customer_events", "order_events", "order_item_events",
                "payment_events", "review_events", "datagen_params"]


def trino_run(statements, maintenance=False):
    import trino

    # client_tags=maintenance -> resource group global.maintenance: một query
    # mỗi lúc, ưu tiên thấp nhất, không lấn tài nguyên của ETL và dashboard
    conn = trino.dbapi.connect(**TRINO, client_tags=["maintenance"] if maintenance else None)
    cur = conn.cursor()
    results = []
    for sql in statements:
        cur.execute(sql)
        results.append(cur.fetchall())
    conn.close()
    return results


@dag(
    dag_id="olist_daily",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 10, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["olist", "gold"],
)
def olist_daily():

    @task
    def resolve_cutoff():
        """Mốc lần này + mốc lần trước (đọc từ ops.watermark)."""
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}
        if conf.get("cutoff_ts"):
            cutoff = conf["cutoff_ts"]
        elif ctx.get("data_interval_end"):
            cutoff = ctx["data_interval_end"].in_timezone("UTC").strftime(FMT)
        else:  # chạy tay không có data interval
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime(FMT)

        prev = trino_run([
            "CREATE SCHEMA IF NOT EXISTS lakehouse.ops",
            """CREATE TABLE IF NOT EXISTS lakehouse.ops.watermark (
                   cutoff_ts timestamp(6), sim_cutoff_ts timestamp(6),
                   dag_run_id varchar, finished_at timestamp(6) with time zone)""",
            f"SELECT CAST(max(cutoff_ts) AS varchar) FROM lakehouse.ops.watermark "
            f"WHERE cutoff_ts < TIMESTAMP '{cutoff}'",
        ])[-1][0][0]
        prev = prev[:19] if prev else None
        print(f"mốc lần này = {cutoff} UTC, mốc lần trước = {prev}")
        # khoá trùng tên biến dbt, truyền thẳng qua --vars
        return {"cutoff_ts": cutoff, "prev_cutoff_ts": prev}

    @task.sensor(poke_interval=60, timeout=2 * 3600, mode="reschedule")
    def wait_commit_margin(cut: dict):
        """Chờ tới mốc + 5 phút: một lô Spark bắt đầu ngay trước mốc (ingest_ts
        < mốc) có thể chưa commit xong lúc DAG khởi động. Chế độ reschedule
        nhả worker slot giữa các lần kiểm tra."""
        cutoff = datetime.strptime(cut["cutoff_ts"], FMT).replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= cutoff + timedelta(minutes=5)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/dbt/olist && /home/airflow/dbt-venv/bin/dbt build "
            "--vars '{{ ti.xcom_pull(task_ids=\"resolve_cutoff\") | tojson }}'"
        ),
        env={"TRINO_HOST": "trino-coordinator", "TRINO_PORT": "8080", "TRINO_USER": "admin",
             "TRINO_CATALOG": "lakehouse", "DBT_PROFILES_DIR": "/opt/dbt/olist",
             # Project dbt được bind mount dùng chung với container dbt chạy
             # tay (root); Airflow chạy bằng uid 50000 nên ghi artifact ra /tmp
             "DBT_TARGET_PATH": "/tmp/dbt/target", "DBT_LOG_PATH": "/tmp/dbt/logs"},
        append_env=True,
    )

    @task
    def publish_watermark(cut: dict):
        """Ghi mốc vào ops.watermark - chỉ chạy khi dbt build thành công.

        Mốc là hợp đồng giữa các thành phần: lần chốt sau đọc nó làm
        prev_cutoff_ts, Spark Streaming đọc nó để xoá khỏi orders_live các
        đơn đã có trong silver chuẩn."""
        ctx = get_current_context()
        c = cut["cutoff_ts"]
        trino_run([f"""
            INSERT INTO lakehouse.ops.watermark
            SELECT TIMESTAMP '{c}',
                   (SELECT max(sim_ts) FROM lakehouse.bronze.order_events
                    WHERE ingest_ts < TIMESTAMP '{c}'),
                   '{ctx["dag_run"].run_id}', current_timestamp(6)"""])

    @task
    def maintain_bronze_events(cut: dict):
        """Bảo trì 6 bảng bronze.*_events qua Trino.

        Các bảng này chỉ được append, nên nén từ bên ngoài vẫn an toàn khi
        Spark đang ghi: append không xoá file nào, và nếu hai commit chạm
        nhau thì Iceberg tự retry. Chỉ nén các partition đã đóng (trước ngày
        của mốc) để không đụng partition đang nhận dữ liệu."""
        day = cut["cutoff_ts"][:10]
        for t in EVENT_TABLES:
            full = f"lakehouse.bronze.{t}"
            trino_run([
                f"ALTER TABLE {full} EXECUTE optimize WHERE ingest_ts < TIMESTAMP '{day} 00:00:00'",
                f"ALTER TABLE {full} EXECUTE optimize_manifests",
                f"ALTER TABLE {full} EXECUTE expire_snapshots(retention_threshold => '1d')",
                f"ALTER TABLE {full} EXECUTE remove_orphan_files(retention_threshold => '1d')",
            ], maintenance=True)
            snaps = trino_run([f'SELECT count(*) FROM lakehouse.bronze."{t}$snapshots"'])[0][0][0]
            files = trino_run([f'SELECT count(*) FROM lakehouse.bronze."{t}$files"'])[0][0][0]
            print(f"{t}: còn {snaps} snapshot, {files} file")

    @task
    def maintain_silver_facts():
        """Bảo trì silver.silver_order_facts qua Trino.

        Đây là model incremental: mỗi lần chốt dbt MERGE vào bảng, để lại một
        snapshot và các delete file. Các model silver/gold khác là `table`,
        được dựng lại toàn bộ mỗi lần nên không tích luỹ file nhỏ.

        Task chạy sau dbt_build trong cùng DAG (max_active_runs=1), nên không
        có MERGE nào chạy song song lúc nén. Giữ time travel 7 ngày để so sánh
        silver giữa các lần chốt."""
        t = "lakehouse.silver.silver_order_facts"
        stats = f'SELECT count(*), count_if(content <> 0) FROM lakehouse.silver."silver_order_facts$files"'
        before = trino_run([stats])[0][0]
        trino_run([
            f"ALTER TABLE {t} EXECUTE optimize",          # gộp file nhỏ + áp delete file vào data file
            f"ALTER TABLE {t} EXECUTE optimize_manifests",
            f"ALTER TABLE {t} EXECUTE expire_snapshots(retention_threshold => '7d')",
            f"ALTER TABLE {t} EXECUTE remove_orphan_files(retention_threshold => '7d')",
            f"ANALYZE {t}",                                # NDV mới cho CBO khi dựng gold lần sau
        ], maintenance=True)
        after = trino_run([stats])[0][0]
        snaps = trino_run(['SELECT count(*) FROM lakehouse.silver."silver_order_facts$snapshots"'])[0][0][0]
        print(f"silver_order_facts: {before[0]} file ({before[1]} delete) -> "
              f"{after[0]} file ({after[1]} delete), còn {snaps} snapshot")

    cut = resolve_cutoff()
    published = wait_commit_margin(cut) >> dbt_build >> publish_watermark(cut)
    # Hai nhánh bảo trì chạy song song phía Airflow; resource group
    # `maintenance` chỉ cho một query mỗi lúc nên chúng xếp hàng ở Trino.
    published >> [maintain_bronze_events(cut), maintain_silver_facts()]


olist_daily()
