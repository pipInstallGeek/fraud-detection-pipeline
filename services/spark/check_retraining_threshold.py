"""
Check whether the model should be retrained based on Gold row growth.

This script compares the current Gold row count with the latest successful
MLflow training run's recorded dataset size. It prints a single JSON object so
Airflow can branch on the result.
"""

import json
import os
from typing import Optional, Tuple

import mlflow
from mlflow import MlflowClient
from pyspark.sql import SparkSession


MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "fraud-detection-onnx")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://lakehouse/gold/features")
RETRAINING_MIN_NEW_ROWS = int(os.getenv("RETRAINING_MIN_NEW_ROWS", "10000"))


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Fraud-Retraining-Threshold-Check")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def get_latest_finished_training_total_records(
    client: MlflowClient,
) -> Tuple[Optional[int], Optional[str]]:
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if experiment is None:
        return None, None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=20,
    )

    for run in runs:
        total_records = run.data.metrics.get("total_records")
        if total_records is not None:
            return int(total_records), run.info.run_id

    return None, None


def main() -> int:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASS

    spark = build_spark_session()
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    try:
        total_records = spark.read.parquet(GOLD_PATH).count()
        previous_total_records, previous_run_id = get_latest_finished_training_total_records(client)

        if previous_total_records is None:
            result = {
                "should_retrain": True,
                "reason": "no_previous_training_run",
                "threshold": RETRAINING_MIN_NEW_ROWS,
                "current_total_records": int(total_records),
                "previous_total_records": None,
                "new_rows_since_last_train": int(total_records),
                "previous_run_id": None,
            }
        else:
            new_rows = max(int(total_records) - int(previous_total_records), 0)
            result = {
                "should_retrain": new_rows >= RETRAINING_MIN_NEW_ROWS,
                "reason": "new_rows_threshold" if new_rows >= RETRAINING_MIN_NEW_ROWS else "threshold_not_met",
                "threshold": RETRAINING_MIN_NEW_ROWS,
                "current_total_records": int(total_records),
                "previous_total_records": int(previous_total_records),
                "new_rows_since_last_train": int(new_rows),
                "previous_run_id": previous_run_id,
            }

        print(json.dumps(result))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
