import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


RETRAINING_MIN_NEW_ROWS = os.getenv("RETRAINING_MIN_NEW_ROWS", "10000")


def evaluate_retraining_threshold(**context) -> str:
    cmd = [
        "docker",
        "exec",
        "-e",
        f"RETRAINING_MIN_NEW_ROWS={RETRAINING_MIN_NEW_ROWS}",
        "-e",
        "MLFLOW_EXPERIMENT_NAME=fraud-detection-onnx",
        "spark-processor",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[*]",
        "/app/check_retraining_threshold.py",
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)

    output_lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not output_lines:
        raise ValueError("Threshold check returned no output")

    threshold_result = json.loads(output_lines[-1])
    context["ti"].xcom_push(key="retraining_context", value=threshold_result)

    return "trigger_model_training" if threshold_result["should_retrain"] else "skip_model_training"


with DAG(
    "silver_incremental_processing",
    default_args=default_args,
    description="Runs Silver/Gold transformations and retrains after N new Gold rows",
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "spark", "incremental", "training"],
) as dag:
    run_silver_spark_job = BashOperator(
        task_id="trigger_transform_silver",
        bash_command='docker exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g /app/transform_silver.py',
    )

    run_gold_spark_job = BashOperator(
        task_id="trigger_transform_gold",
        bash_command='docker exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g /app/transform_gold.py',
    )

    validate_gold_features = BashOperator(
        task_id="validate_gold_features",
        bash_command='docker exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" /app/validate_gold.py',
    )

    evaluate_training_gate = BranchPythonOperator(
        task_id="evaluate_training_gate",
        python_callable=evaluate_retraining_threshold,
    )

    trigger_model_training = BashOperator(
        task_id="trigger_model_training",
        bash_command='docker exec '
        '-e TRAINING_TRIGGER_REASON="new_rows_threshold" '
        '-e TRAINING_NEW_ROWS_SINCE_LAST="{{ ti.xcom_pull(task_ids=\'evaluate_training_gate\', key=\'retraining_context\')[\'new_rows_since_last_train\'] }}" '
        '-e TRAINING_PREVIOUS_TOTAL_RECORDS="{{ ti.xcom_pull(task_ids=\'evaluate_training_gate\', key=\'retraining_context\')[\'previous_total_records\'] }}" '
        '-e RETRAINING_MIN_NEW_ROWS="{{ ti.xcom_pull(task_ids=\'evaluate_training_gate\', key=\'retraining_context\')[\'threshold\'] }}" '
        'spark-processor python3 /app/run_training_with_timeout.py',
    )

    skip_model_training = EmptyOperator(task_id="skip_model_training")

    training_gate_complete = EmptyOperator(
        task_id="training_gate_complete",
        trigger_rule="none_failed_min_one_success",
    )

    run_silver_spark_job >> run_gold_spark_job >> validate_gold_features >> evaluate_training_gate
    evaluate_training_gate >> trigger_model_training >> training_gate_complete
    evaluate_training_gate >> skip_model_training >> training_gate_complete
