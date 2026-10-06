# ===========================================================================
# Airflow 3 standalone + dbt-trino + client Trino
#
# dbt chạy ngay trong container Airflow qua BashOperator thay vì
# DockerOperator: DockerOperator cần mount docker.sock (tương đương toàn quyền
# trên Docker host) và phải truyền đường dẫn bind mount theo hệ điều hành
# host, khó mang đi nơi khác.
#
# dbt được cài trong một venv riêng vì dbt-core và Airflow có lịch sử xung
# đột phụ thuộc (jinja2, protobuf, ...). Hai môi trường tách biệt thì nâng
# cấp bên này không làm hỏng bên kia. Phiên bản dbt khớp với image
# trinodemo/dbt dùng để chạy tay.
# ===========================================================================
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

# Ghim lại apache-airflow để pip không tự nâng/hạ phiên bản khi cài thêm gói
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "trino==0.340.0"

RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core>=1.10,<1.11" "dbt-trino==1.10.4"
