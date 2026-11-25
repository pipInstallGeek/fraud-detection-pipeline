# =============================================================================
# Spark Dockerfile - Production Optimized
# =============================================================================
# Multi-stage build for better layer caching
# Proper metadata and labels

# -----------------------------------------------------------------------------
# Stage 1: Download JARs
# -----------------------------------------------------------------------------
FROM alpine:3.19 AS jar-downloader

RUN apk add --no-cache curl

WORKDIR /jars

# Download all required JARs in one layer
RUN curl -sLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -sLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# -----------------------------------------------------------------------------
# Stage 2: Production image
# -----------------------------------------------------------------------------
FROM apache/spark:3.5.0

# Labels for container metadata
LABEL maintainer="Marouane Bouziane"
LABEL description="Fraud Detection Lakehouse Spark Ingestion"
LABEL version="0.1.0"

USER root

# Copy JARs from downloader stage
COPY --from=jar-downloader /jars/*.jar /opt/spark/jars/

# Set working directory
WORKDIR /app

# Install Python dependencies for ML/MLOps
RUN pip install --no-cache-dir \
    boto3 \
    "numpy<2" \
    "sqlalchemy<2.1.0" \
    skl2onnx \
    mlflow==2.10.0

# Copy Python scripts
COPY *.py ./

# Copy Great Expectations suite
COPY expectations/ ./expectations/

# Environment variables
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[*]", "/app/streaming_job.py"]
