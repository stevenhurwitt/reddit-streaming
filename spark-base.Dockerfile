FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.3.1
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get install -y curl && \
    # curl https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz -o spark.tgz && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV JAVA_HOME /usr/local/openjdk-8
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_UI_PORT 4040
# ENV SPARK_PUBLIC_DNS localhost
ENV PYSPARK_PYTHON python3
ENV JAVA_HOME /usr/local/openjdk-8

# -- Runtime

WORKDIR ${SPARK_HOME}
