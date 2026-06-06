# =============================================================================
# Custom Spark 3.5 image with:
#   - Apache Iceberg runtime (iceberg-spark-runtime-3.5_2.12)
#   - Hadoop AWS connector
#   - AWS Java SDK bundle
#   - Python 3.10 + PySpark dependencies
# =============================================================================

FROM apache/spark:3.5.1-python3

USER root

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Download required JARs into Spark's jars directory
# ---------------------------------------------------------------------------
ENV ICEBERG_VERSION=1.5.2
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV SPARK_JARS_DIR=/opt/spark/jars

# Iceberg Spark runtime (for Spark 3.5 + Scala 2.12)
RUN wget -q -O ${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# Hadoop AWS
RUN wget -q -O ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

# AWS Java SDK bundle
RUN wget -q -O ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ---------------------------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------------------------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# App directory
# ---------------------------------------------------------------------------
WORKDIR /opt/app

# Allow running as non-root in Spark
RUN chmod -R 777 /opt/spark/work-dir 2>/dev/null || true

USER 1001
