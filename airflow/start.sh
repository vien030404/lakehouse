#!/usr/bin/env bash
# Entrypoint của container Airflow: đảm bảo database metadata `airflow` tồn
# tại trên postgres-ops (Postgres chỉ tự tạo database khai trong POSTGRES_DB),
# rồi chạy Airflow standalone (api-server, scheduler, dag-processor, triggerer).
set -euo pipefail

python - <<'EOF'
import os
import psycopg2

conn = psycopg2.connect(host="postgres-ops", dbname="postgres",
                        user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"])
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone() is None:
    cur.execute("CREATE DATABASE airflow")
    print("Đã tạo database airflow trên postgres-ops", flush=True)
conn.close()
EOF

exec airflow standalone
