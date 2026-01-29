from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'silver_incremental_processing',
    default_args=default_args,
    description='Runs Silver transformation (Incremental Batch) every 2 minutes',
    schedule_interval='*/2 * * * *',  # Run every 2 minutes
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['silver', 'spark', 'incremental'],
) as dag:

    run_silver_spark_job = BashOperator(
        task_id='trigger_transform_silver',
        bash_command='docker exec lakehouse-ingestion /opt/spark/bin/spark-submit --master "local[*]" /app/transform_silver.py',
    )

    run_gold_spark_job = BashOperator(
        task_id='trigger_transform_gold',
        bash_command='docker exec lakehouse-ingestion /opt/spark/bin/spark-submit --master "local[*]" /app/transform_gold.py',
    )

    # Pipeline: Silver -> Gold
    run_silver_spark_job >> run_gold_spark_job
