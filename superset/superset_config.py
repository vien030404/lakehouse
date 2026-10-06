"""
===========================================================================
Cấu hình Superset.

Metadata DB mặc định là SQLite trong volume `superset-home`: đủ cho một người
dùng local và không cần thêm service. Có thể chuyển sang PostgreSQL qua biến
SUPERSET_METADATA_URI, ví dụ database `superset` trên postgres-ops:
    postgresql+psycopg2://ops:ops123@postgres-ops:5432/superset
===========================================================================
"""

import os

# --- Bảo mật ---------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME_IN_DOT_ENV")

# --- Metadata DB -----------------------------------------------------------
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_URI", "sqlite:////app/superset_home/superset.db"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

# --- Timeout và giới hạn số dòng ------------------------------------------
# Timeout rộng để truy vấn phân tích dài trên cụm Trino nhỏ không bị SQL Lab
# cắt ngang; giới hạn số dòng bảo vệ trình duyệt và web server.
SUPERSET_WEBSERVER_TIMEOUT = int(os.environ.get("GUNICORN_TIMEOUT", 300))
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600
SUPERSET_WEBSERVER_PORT = 8088

ROW_LIMIT = 50_000
VIZ_ROW_LIMIT = 10_000
SAMPLES_ROW_LIMIT = 1_000
SQL_MAX_ROW = 100_000
DISPLAY_MAX_ROW = 10_000

# --- Cache trong tiến trình, truy vấn đồng bộ ------------------------------
# Truy vấn bất đồng bộ và alert/report cần Redis + Celery worker; với một
# người dùng local, chế độ đồng bộ và SimpleCache là đủ.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 86400}
EXPLORE_FORM_DATA_CACHE_CONFIG = FILTER_STATE_CACHE_CONFIG

# --- Feature flag ----------------------------------------------------------
FEATURE_FLAGS = {
    # Jinja trong SQL Lab: truy vấn tham số hoá
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DRILL_TO_DETAIL": True,
    "DRILL_BY": True,
    "ALERT_REPORTS": False,        # cần Celery
    "EMBEDDED_SUPERSET": False,
}

# --- Chạy trên localhost / sau reverse proxy, không ép HTTPS --------------
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log"]

# --- DML từ SQL Lab ---------------------------------------------------------
# Mỗi database trong Superset còn công tắc "Allow DML" riêng;
# bootstrap_trino_db.py bật sẵn để chạy được CTAS/MERGE trong SQL Lab.
PREVENT_UNSAFE_DB_CONNECTIONS = False

LOG_LEVEL = "INFO"
