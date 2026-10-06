# ===========================================================================
# Image Superset kèm driver Trino (SQLAlchemy dialect `trino://`) và script
# khởi tạo kết nối tới cụm Trino.
# ===========================================================================
ARG SUPERSET_VERSION=5.0.0
FROM apache/superset:${SUPERSET_VERSION}

USER root

# Superset chạy trong virtualenv /app/.venv, còn `pip` của image trỏ tới Python
# hệ thống; cài bằng `pip` khiến Superset không thấy dialect
# (NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:trino).
# Vì vậy driver được cài thẳng vào venv bằng uv (có sẵn trong image).
RUN uv pip install --python /app/.venv/bin/python --no-cache       "trino[sqlalchemy]>=0.336"

COPY --chown=superset:superset superset_config.py     /app/pythonpath/superset_config.py
COPY --chown=superset:superset entrypoint-superset.sh /app/entrypoint-superset.sh
COPY --chown=superset:superset bootstrap_trino_db.py  /app/bootstrap_trino_db.py

RUN chmod +x /app/entrypoint-superset.sh

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py \
    PYTHONPATH=/app/pythonpath
