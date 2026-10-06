#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Entrypoint Superset: migrate metadata DB, tạo admin, khởi tạo quyền, tạo
# kết nối Trino, rồi chạy gunicorn. Mọi bước đều idempotent nên container
# khởi động lại an toàn.
# ---------------------------------------------------------------------------
set -euo pipefail

ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin}"
WORKERS="${SERVER_WORKER_AMOUNT:-1}"
THREADS="${SERVER_THREADS_AMOUNT:-8}"
TIMEOUT="${GUNICORN_TIMEOUT:-300}"

echo "[entrypoint] nâng cấp metadata DB"
superset db upgrade

echo "[entrypoint] tạo tài khoản admin (bỏ qua nếu đã có)"
superset fab create-admin \
  --username "${ADMIN_USER}" \
  --password "${ADMIN_PASSWORD}" \
  --firstname Trino --lastname Demo \
  --email admin@example.com 2>/dev/null || true

echo "[entrypoint] khởi tạo role và permission"
superset init

echo "[entrypoint] tạo sẵn kết nối tới Trino"
python /app/bootstrap_trino_db.py

echo "[entrypoint] khởi động web server (${WORKERS} worker x ${THREADS} thread)"
exec gunicorn \
  --bind "0.0.0.0:8088" \
  --workers "${WORKERS}" \
  --worker-class gthread \
  --threads "${THREADS}" \
  --timeout "${TIMEOUT}" \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  "superset.app:create_app()"
