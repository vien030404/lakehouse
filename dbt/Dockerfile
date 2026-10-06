# ===========================================================================
# Image dbt chạy theo yêu cầu (không chạy nền), dùng để build tay ngoài lịch
# Airflow:
#
#   docker compose run --rm dbt build
#   docker compose run --rm dbt run --select +gold_revenue_by_state
#
# Chỉ gồm python slim + dbt-core + dbt-trino, cùng phiên bản với venv dbt
# trong image Airflow, nên kết quả chạy tay và chạy theo lịch giống nhau.
# ===========================================================================
FROM python:3.12-slim

ARG DBT_CORE_VERSION=">=1.10,<1.11"
ARG DBT_TRINO_VERSION="==1.10.4"

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir \
      "dbt-core${DBT_CORE_VERSION}" \
      "dbt-trino${DBT_TRINO_VERSION}"

# Project dbt được bind mount từ host; target/ và logs/ ghi vào đây.
WORKDIR /dbt/olist

# Cố định project dir và profiles dir để lệnh gọi chỉ cần
#   docker compose run --rm dbt <lệnh>
ENV DBT_PROJECT_DIR=/dbt/olist \
    DBT_PROFILES_DIR=/dbt/olist \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["dbt"]
CMD ["--help"]
