# ===========================================================================
# Iceberg REST catalog với backend PostgreSQL
#
# Image apache/iceberg-rest-fixture chỉ đóng gói driver SQLite. Image này bổ
# sung driver PostgreSQL để catalog lưu con trỏ metadata trong postgres-ops.
#
# Vì sao không dùng SQLite: SQLite chỉ cho một writer tại một thời điểm.
# dbt chạy nhiều thread và Spark Streaming commit liên tục, nên các commit
# Iceberg đồng thời sẽ gặp lỗi khoá:
#     [SQLITE_BUSY] The database file is locked
#     ICEBERG_COMMIT_ERROR: Cannot determine whether the commit was successful
# PostgreSQL xử lý commit đồng thời bằng transaction thông thường.
#
# Vì sao phải đổi ENTRYPOINT: image gốc chạy `java -jar ...`, và với `-jar`
# JVM bỏ qua mọi classpath bên ngoài, nên driver thêm vào sẽ không được nạp.
# Entrypoint bên dưới dùng `-cp` và gọi thẳng main class.
# ===========================================================================
ARG ICEBERG_REST_VERSION=1.10.1
FROM apache/iceberg-rest-fixture:${ICEBERG_REST_VERSION}

ARG PG_DRIVER_VERSION=42.7.13

USER root

ADD --chown=iceberg:iceberg \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_DRIVER_VERSION}/postgresql-${PG_DRIVER_VERSION}.jar \
    /usr/lib/iceberg-rest/postgresql.jar

RUN chmod 644 /usr/lib/iceberg-rest/postgresql.jar

USER iceberg
WORKDIR /usr/lib/iceberg-rest

ENTRYPOINT ["java", \
            "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", \
            "org.apache.iceberg.rest.RESTCatalogServer"]
