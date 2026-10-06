# ===========================================================================
# Spark 3.5 + Iceberg + S3A + Kafka - một image dùng chung cho master,
# worker, spark-submit và spark-streaming.
#
# Image apache/spark không kèm Iceberg, S3 hay Kafka. Các jar cần thiết được
# đóng gói sẵn để container không phải tải dependency lúc chạy:
#
#   iceberg-spark-runtime   bảng Iceberg, MERGE INTO, CALL <procedure>
#   iceberg-aws-bundle      S3FileIO (AWS SDK v2): Iceberg đọc/ghi Parquet trên MinIO
#   hadoop-aws              hệ thống file s3a:// cho file thường (CSV, checkpoint)
#   aws-java-sdk-bundle     AWS SDK v1 mà hadoop-aws phụ thuộc
#
# Cần cả hai đường S3: S3FileIO chỉ phục vụ bảng Iceberg, còn CSV thô ở
# s3://raw và checkpoint của Structured Streaming đi qua Hadoop FileSystem
# (s3a://).
#
# Ràng buộc phiên bản:
#   hadoop-aws phải khớp Hadoop đi kèm Spark: image 3.5.9 có
#     hadoop-client-api-3.3.4.jar  ->  hadoop-aws 3.3.4
#   aws-java-sdk-bundle 1.12.262 là phiên bản hadoop-aws 3.3.4 khai trong pom
#   iceberg-spark-runtime-3.5_2.12: "3.5" = Spark minor, "2.12" = Scala
#   ICEBERG_VERSION khớp với iceberg-rest (ICEBERG_REST_VERSION trong .env)
# ===========================================================================
ARG SPARK_VERSION=3.5.9
FROM apache/spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu
# ARG khai trước FROM hết hiệu lực sau FROM; khai lại để dùng trong ADD
ARG SPARK_VERSION

ARG ICEBERG_VERSION=1.10.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

USER root

ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar \
    /opt/spark/jars/

# --- Kafka connector cho Structured Streaming -------------------------------
# Redpanda tương thích giao thức Kafka nên dùng thẳng connector Kafka của
# Spark. Phiên bản kafka-clients và commons-pool2 lấy theo spark-parent 3.5.9
# pom (kafka.version=3.4.1, commons-pool2.version=2.11.1).
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark --chmod=644 \
    ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
    /opt/spark/jars/

# Quyền file đặt ngay trong ADD (--chmod) thay vì một lệnh `RUN chmod` riêng:
# RUN chmod trên /opt/spark/jars khiến Docker chép lại cả thư mục sang layer
# mới (khoảng 390 MB thừa).

USER spark
WORKDIR /opt/spark/work-dir
