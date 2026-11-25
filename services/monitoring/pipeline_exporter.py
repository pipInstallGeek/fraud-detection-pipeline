"""
Fraud Detection Pipeline - Telemetry Exporter
Exposes real-time data lakehouse pipeline health, layer counts, CDC state,
and ML metrics for Prometheus scraping on port 8008.
"""

import logging
import os
import time
from typing import Dict, Tuple

from minio import Minio
import psycopg2
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import requests
from prometheus_client import Gauge, Info, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline-exporter")

# Environment configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")
DEBEZIUM_CONNECTOR = os.getenv("DEBEZIUM_CONNECTOR", "postgres-connector")

MLFLOW_URL = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8008"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "10"))

# Prometheus Metrics Definitions
PIPELINE_INFO = Info("fraud_pipeline", "Fraud detection pipeline metadata")
SERVICE_HEALTH = Gauge(
    "fraud_pipeline_service_health",
    "Health status of core pipeline services (1=UP, 0=DOWN)",
    ["service"],
)
DEBEZIUM_UP = Gauge(
    "fraud_debezium_connector_up",
    "Status of Debezium PostgreSQL CDC connector (1=RUNNING, 0=OTHER)",
)
POSTGRES_TRANSACTIONS = Gauge(
    "fraud_postgres_transactions_total",
    "Total transaction rows committed in PostgreSQL source table",
)
LAKEHOUSE_OBJECTS = Gauge(
    "fraud_lakehouse_objects_count",
    "Number of parquet objects/files stored in Lakehouse tiers",
    ["layer"],
)
LAKEHOUSE_RECORDS = Gauge(
    "fraud_lakehouse_records_count",
    "Total record (row) count stored in Lakehouse tiers",
    ["layer"],
)
MLFLOW_LATEST_AUC = Gauge(
    "fraud_mlflow_latest_model_auc",
    "AUC of the most recent finished ML model run in MLflow",
)
LAST_SCRAPE_TIMESTAMP = Gauge(
    "fraud_pipeline_exporter_last_scrape_timestamp",
    "Unix timestamp of the last telemetry collection cycle",
)

_PARQUET_ROW_CACHE: Dict[Tuple[str, int], int] = {}


def get_postgres_count() -> int:
    """Fetch count of records from PostgreSQL source transactions table."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions;")
            count = cur.fetchone()[0]
        conn.close()
        SERVICE_HEALTH.labels(service="postgres").set(1)
        return int(count)
    except Exception as exc:
        logger.warning("PostgreSQL check failed: %s", exc)
        SERVICE_HEALTH.labels(service="postgres").set(0)
        return -1


def check_debezium() -> int:
    """Check Debezium connector state via REST API."""
    url = f"{DEBEZIUM_URL}/connectors/{DEBEZIUM_CONNECTOR}/status"
    try:
        resp = requests.get(url, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            connector_state = data.get("connector", {}).get("state", "")
            tasks = data.get("tasks", [])
            tasks_running = all(t.get("state") == "RUNNING" for t in tasks) if tasks else False
            is_healthy = 1 if (connector_state == "RUNNING" and tasks_running) else 0
            SERVICE_HEALTH.labels(service="debezium").set(is_healthy)
            return is_healthy
        else:
            SERVICE_HEALTH.labels(service="debezium").set(0)
            return 0
    except Exception as exc:
        logger.warning("Debezium check failed: %s", exc)
        SERVICE_HEALTH.labels(service="debezium").set(0)
        return 0


def get_minio_stats() -> Tuple[Dict[str, int], Dict[str, int]]:
    """Count parquet objects AND actual record rows across Bronze, Silver, and Gold tiers."""
    object_counts = {"bronze": 0, "silver": 0, "gold": 0}
    record_counts = {"bronze": 0, "silver": 0, "gold": 0}
    try:
        s3_fs = pafs.S3FileSystem(
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            endpoint_override=f"http://{MINIO_ENDPOINT}",
            scheme="http",
        )
        layer_prefixes = {
            "bronze": f"{MINIO_BUCKET}/bronze/transactions",
            "silver": f"{MINIO_BUCKET}/silver/transactions",
            "gold": f"{MINIO_BUCKET}/gold/features",
        }
        for layer, prefix in layer_prefixes.items():
            try:
                selector = pafs.FileSelector(prefix, recursive=True)
                entries = [e for e in s3_fs.get_file_info(selector) if e.is_file and e.path.endswith(".parquet")]
            except Exception:
                entries = []

            object_counts[layer] = len(entries)
            total_rows = 0
            for e in entries:
                cache_key = (e.path, e.size)
                if cache_key not in _PARQUET_ROW_CACHE:
                    try:
                        _PARQUET_ROW_CACHE[cache_key] = pq.read_metadata(e.path, filesystem=s3_fs).num_rows
                    except Exception:
                        _PARQUET_ROW_CACHE[cache_key] = 0
                total_rows += _PARQUET_ROW_CACHE[cache_key]
            record_counts[layer] = total_rows

        SERVICE_HEALTH.labels(service="minio").set(1)
        return object_counts, record_counts
    except Exception as exc:
        logger.warning("MinIO stats check failed: %s", exc)
        SERVICE_HEALTH.labels(service="minio").set(0)
        return object_counts, record_counts


def get_latest_mlflow_auc() -> float:
    """Retrieve AUC of latest completed fraud detection model run from MLflow."""
    try:
        # Search experiment
        exp_url = f"{MLFLOW_URL}/api/2.0/mlflow/experiments/get-by-name?experiment_name=fraud-detection"
        exp_resp = requests.get(exp_url, timeout=3)
        if exp_resp.status_code != 200:
            SERVICE_HEALTH.labels(service="mlflow").set(0)
            return 0.0

        exp_data = exp_resp.json()
        exp_id = exp_data.get("experiment", {}).get("experiment_id")
        if not exp_id:
            SERVICE_HEALTH.labels(service="mlflow").set(1)
            return 0.0

        SERVICE_HEALTH.labels(service="mlflow").set(1)
        search_url = f"{MLFLOW_URL}/api/2.0/mlflow/runs/search"
        payload = {
            "experiment_ids": [exp_id],
            "filter": "attributes.status = 'FINISHED'",
            "order_by": ["attributes.start_time DESC"],
            "max_results": 1,
        }
        runs_resp = requests.post(search_url, json=payload, timeout=3)
        if runs_resp.status_code == 200:
            runs_data = runs_resp.json()
            runs = runs_data.get("runs", [])
            if runs:
                metrics = runs[0].get("data", {}).get("metrics", [])
                for m in metrics:
                    if m.get("key") == "auc":
                        return float(m.get("value", 0.0))
        return 0.0
    except Exception as exc:
        logger.warning("MLflow check failed: %s", exc)
        SERVICE_HEALTH.labels(service="mlflow").set(0)
        return 0.0


def collect_metrics() -> None:
    """Run one collection iteration across all components."""
    # 1. PostgreSQL
    pg_count = get_postgres_count()
    if pg_count >= 0:
        POSTGRES_TRANSACTIONS.set(pg_count)

    # 2. Debezium CDC
    deb_up = check_debezium()
    DEBEZIUM_UP.set(deb_up)

    # 3. MinIO Lakehouse (both object files and actual rows)
    minio_obj_counts, minio_rec_counts = get_minio_stats()
    for layer, count in minio_obj_counts.items():
        LAKEHOUSE_OBJECTS.labels(layer=layer).set(count)
    for layer, count in minio_rec_counts.items():
        LAKEHOUSE_RECORDS.labels(layer=layer).set(count)

    # 4. MLflow
    auc = get_latest_mlflow_auc()
    if auc > 0.0:
        MLFLOW_LATEST_AUC.set(auc)

    LAST_SCRAPE_TIMESTAMP.set(time.time())



def main() -> None:
    logger.info("Starting Fraud Pipeline Telemetry Exporter on port %s", EXPORTER_PORT)
    PIPELINE_INFO.info({
        "version": "1.0.0",
        "lakehouse": "MinIO S3",
        "streaming": "Spark Structured Streaming",
        "cdc": "Debezium PostgreSQL",
    })
    start_http_server(EXPORTER_PORT)
    logger.info("Prometheus HTTP server started. Entering collection loop...")

    while True:
        try:
            collect_metrics()
        except Exception as exc:
            logger.error("Unexpected error in collect loop: %s", exc)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
