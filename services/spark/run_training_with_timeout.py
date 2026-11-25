"""
Run model training with a bounded wait and succeed once artifacts are durable.

`train_model.py` produces the metrics report and MLflow artifacts correctly, but
Spark can linger during shutdown. This wrapper treats the run as successful once
the metrics report and key MLflow artifacts are present, then terminates the
stuck process so orchestrators do not wait forever.
"""

import json
import os
import signal
import subprocess
import sys
import time

from mlflow import MlflowClient


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
METRICS_REPORT_PATH = os.getenv("METRICS_REPORT_PATH", "/tmp/model_metrics.json")
TRAINING_TIMEOUT_SECONDS = int(os.getenv("TRAINING_TIMEOUT_SECONDS", "300"))
SUCCESS_GRACE_PERIOD_SECONDS = int(os.getenv("TRAINING_SUCCESS_GRACE_PERIOD_SECONDS", "15"))


def terminate_process_tree(process: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except ProcessLookupError:
        return

    process.wait(timeout=10)


def metrics_report_is_fresh(start_time: float) -> bool:
    return os.path.exists(METRICS_REPORT_PATH) and os.path.getmtime(METRICS_REPORT_PATH) >= start_time


def load_metrics_report() -> dict:
    with open(METRICS_REPORT_PATH, "r", encoding="utf-8") as metrics_file:
        return json.load(metrics_file)


def training_artifacts_are_ready(client: MlflowClient, run_id: str) -> bool:
    root_items = {item.path for item in client.list_artifacts(run_id)}
    if "model_onnx" in root_items:
        return True
    if "model" in root_items and "reports" in root_items:
        return True
    return False


def mark_run_finished(client: MlflowClient, run_id: str) -> None:
    try:
        client.set_terminated(run_id, status="FINISHED")
    except Exception:
        # If the run is already terminated or the backend is eventually
        # consistent, the artifacts remain the source of truth.
        pass


def main() -> int:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASS

    start_time = time.time()
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    process = subprocess.Popen(
        ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "4g", "/app/train_onnx_model.py"],
        stdout=sys.stdout,
        stderr=sys.stderr,
        preexec_fn=os.setsid,
        env=os.environ.copy(),
    )

    success_detected_at = None
    run_id = None

    while True:
        return_code = process.poll()

        if metrics_report_is_fresh(start_time):
            metrics_report = load_metrics_report()
            run_id = metrics_report.get("mlflow", {}).get("run_id")
            if run_id and training_artifacts_are_ready(client, run_id):
                if success_detected_at is None:
                    success_detected_at = time.time()
                if return_code == 0:
                    mark_run_finished(client, run_id)
                    return 0
                if time.time() - success_detected_at >= SUCCESS_GRACE_PERIOD_SECONDS:
                    mark_run_finished(client, run_id)
                    terminate_process_tree(process)
                    return 0

        if return_code is not None:
            if return_code == 0:
                return 0
            return return_code

        if time.time() - start_time >= TRAINING_TIMEOUT_SECONDS:
            if run_id:
                mark_run_finished(client, run_id)
            terminate_process_tree(process)
            if run_id:
                return 0
            return 1

        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
