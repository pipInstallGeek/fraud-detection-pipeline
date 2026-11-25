# =============================================================================
# MLflow Tracking Server - Dockerfile
# =============================================================================
# Lightweight MLflow server with S3 (MinIO) artifact storage support

FROM python:3.11-slim

LABEL maintainer="Marouane Bouziane"
LABEL description="MLflow Tracking Server for Fraud Detection Pipeline"
LABEL version="1.0.0"

# Install MLflow with S3 support
RUN pip install --no-cache-dir \
    mlflow==2.10.0 \
    "sqlalchemy<2.1.0" \
    boto3 \
    psycopg2-binary

# Create directory for SQLite backend store
RUN mkdir -p /mlflow

WORKDIR /mlflow

# Expose MLflow UI port
EXPOSE 5000

# Default command (can be overridden in docker-compose)
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
