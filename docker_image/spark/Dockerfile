# # FROM docker.io/bitnami/spark:3.3.2

# # USER root

# # # Install prerequisites
# # RUN apt-get update && apt-get install -y curl

# # RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
# #     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
# #     # && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
# #     && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
# #     && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
# #     && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
# #     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
# #     && mv s3-2.18.41.jar /opt/bitnami/spark/jars \
# #     && mv aws-java-sdk-1.12.367.jar /opt/bitnami/spark/jars \
# #     # && mv aws-java-sdk-bundle-1.11.1026.jar /opt/bitnami/spark/jars \
# #     && mv delta-core_2.12-2.3.0.jar /opt/bitnami/spark/jars \
# #     && mv delta-storage-2.3.0.jar /opt/bitnami/spark/jars \
# #     && mv mysql-connector-java-8.0.19.jar /opt/bitnami/spark/jars \
# #     && mv hadoop-aws-3.3.2.jar /opt/bitnami/spark/jars
# FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

# USER root

# # -------------------------------------------------
# # 1. Install Python 3.9.16 (align with Dagster)
# # -------------------------------------------------
# RUN apt-get update && apt-get install -y \
#     python3.9 \
#     python3.9-distutils \
#     python3-pip \
#     curl \
#  && ln -sf /usr/bin/python3.9 /usr/bin/python \
#  && ln -sf /usr/bin/python3.9 /usr/bin/python3 \
#  && python3 --version \
#  && rm -rf /var/lib/apt/lists/*

# RUN python3 -m pip install --upgrade pip

# # -------------------------------------------------
# # 2. Force Spark to use Python 3.9
# # -------------------------------------------------
# ENV PYSPARK_PYTHON=python3
# ENV PYSPARK_DRIVER_PYTHON=python3

# # -------------------------------------------------
# # 3. Spark 3.4.1 compatible JARs
# # -------------------------------------------------
# RUN curl -fLo /opt/spark/jars/s3-2.18.41.jar \
#         https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
#  && curl -fLo /opt/spark/jars/aws-java-sdk-1.12.367.jar \
#         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
#  && curl -fLo /opt/spark/jars/delta-core_2.12-2.4.0.jar \
#         https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
#  && curl -fLo /opt/spark/jars/delta-storage-2.4.0.jar \
#         https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar \
#  && curl -fLo /opt/spark/jars/mysql-connector-java-8.0.19.jar \
#         https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
#  && curl -fLo /opt/spark/jars/hadoop-aws-3.3.4.jar \
#         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# # -------------------------------------------------
# # 4. (Optional) Python libs for PySpark jobs
# # -------------------------------------------------
# # Uncomment if your jobs need them
# # RUN pip install \
# #     pandas \
# #     pyarrow \
# #     boto3 \
# #     s3fs

# # -------------------------------------------------
# # 5. Working directory
# # -------------------------------------------------
# WORKDIR /opt/spark

# Sử dụng OpenJDK 11 (phù hợp với Spark 3.3.2)
# FROM eclipse-temurin:11-jre-focal

# USER root

# # 1. Cài đặt các công cụ cơ bản và Python
# RUN apt-get update && apt-get install -y curl python3 python3-pip procps && \
#     rm -rf /var/lib/apt/lists/*

# # 2. Cấu hình biến môi trường
# ENV SPARK_VERSION=3.3.2
# ENV HADOOP_VERSION=3
# ENV SPARK_HOME=/opt/spark
# ENV PATH=$SPARK_HOME/bin:$PATH

# # 3. Tải và cài đặt Spark 3.3.2
# # Sử dụng link archive vì bản 3.3.2 không còn là bản mới nhất trên mirror chính
# RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
#     tar -xf spark.tgz && \
#     mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
#     rm spark.tgz

# # 4. Tải các thư viện JAR (S3, Delta, MySQL) trực tiếp vào thư mục jars của Spark
# WORKDIR $SPARK_HOME/jars

# RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
#     && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar

# # 5. Thiết lập thư mục làm việc và quyền hạn
# WORKDIR $SPARK_HOME
# EXPOSE 4040 7077 8080

# # Chạy Spark Shell mặc định (hoặc bạn có thể đổi thành tail -f để giữ container chạy)
# CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]

# Sử dụng Python 3.9 làm gốc
FROM python:3.9-slim-bullseye

USER root

# 1. Cài đặt Java (Spark cần Java) và các công cụ cần thiết
RUN apt-get update && apt-get install -y openjdk-11-jre-headless curl procps && \
       rm -rf /var/lib/apt/lists/*
# 2. Cấu hình biến môi trường Spark
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Khai báo rõ ràng phiên bản Python cho Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Tải và cài đặt Spark 3.3.2
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
       tar -xf spark.tgz && \
       mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
       rm spark.tgz

# 4. Tải các thư viện JAR (S3, Delta, MySQL)

WORKDIR $SPARK_HOME/jars
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar

WORKDIR $SPARK_HOME