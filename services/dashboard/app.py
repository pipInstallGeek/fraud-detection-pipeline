import os
from typing import Optional

import mlflow
import mlflow.spark
import pandas as pd
import plotly.express as px
import streamlit as st
from mlflow import MlflowClient
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from streamlit_autorefresh import st_autorefresh


MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "fraud-detection")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://lakehouse/gold/features")
DEFAULT_LIMIT = int(os.getenv("DASHBOARD_DEFAULT_LIMIT", "200"))
DEFAULT_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "15"))


def configure_mlflow_auth() -> None:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASS
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Fraud-Monitoring-Dashboard")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3.access.key", MINIO_USER)
    hadoop_conf.set("fs.s3.secret.key", MINIO_PASS)
    hadoop_conf.set("fs.s3.path.style.access", "true")
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


@st.cache_resource(show_spinner=False)
def get_mlflow_client() -> MlflowClient:
    configure_mlflow_auth()
    return MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)


@st.cache_data(ttl=10, show_spinner=False)
def get_gold_transaction_count() -> int:
    try:
        spark = get_spark()
        return int(spark.read.parquet(GOLD_PATH).count())
    except Exception:
        return 0


def get_latest_model_run() -> Optional[dict]:
    client = get_mlflow_client()
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if experiment is None:
        return None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=20,
    )

    for run in runs:
        try:
            artifact_paths = {item.path for item in client.list_artifacts(run.info.run_id, "model")}
        except Exception:
            continue

        if "model/MLmodel" in artifact_paths:
            return {
                "run_id": run.info.run_id,
                "start_time": run.info.start_time,
                "metrics": run.data.metrics,
                "params": run.data.params,
            }

    return None


@st.cache_resource(show_spinner=False)
def load_model(run_id: str):
    configure_mlflow_auth()
    return mlflow.spark.load_model(f"runs:/{run_id}/model")


@st.cache_data(ttl=10, show_spinner=False)
def load_recent_predictions(run_id: str, limit: Optional[int], threshold: float = 0.50) -> pd.DataFrame:
    spark = get_spark()
    model = load_model(run_id)

    gold_df = (
        spark.read.parquet(GOLD_PATH)
        .select(
            "id",
            "step",
            "type",
            "amount",
            "avg_amount_24h",
            "count_24h",
            "old_balance_orig",
            "new_balance_orig",
            "old_balance_dest",
            "new_balance_dest",
            "orig_balance_err",
            "dest_balance_err",
            "is_empty_balance",
            "is_fraud",
            "ingested_at",
        )
        .orderBy(col("id").desc())
    )
    if limit is not None:
        gold_df = gold_df.limit(limit)

    scored_df = (
        model.transform(gold_df)
        .withColumn("fraud_probability", vector_to_array("probability")[1])
        .withColumn(
            "predicted_fraud",
            when(col("fraud_probability") >= threshold, 1).otherwise(0),
        )
        .select(
            col("id"),
            col("step"),
            col("type"),
            col("amount"),
            col("avg_amount_24h"),
            col("count_24h"),
            col("ingested_at"),
            col("is_fraud").cast("int").alias("actual_fraud"),
            col("predicted_fraud"),
            col("fraud_probability"),
        )
        .withColumn(
            "outcome",
            when((col("actual_fraud") == 1) & (col("predicted_fraud") == 1), "True Positive")
            .when((col("actual_fraud") == 0) & (col("predicted_fraud") == 0), "True Negative")
            .when((col("actual_fraud") == 0) & (col("predicted_fraud") == 1), "False Positive")
            .otherwise("False Negative"),
        )
        .withColumn("is_correct", col("actual_fraud") == col("predicted_fraud"))
        .orderBy(col("id").desc())
    )

    pdf = scored_df.toPandas()
    if pdf.empty:
        return pdf

    pdf["ingested_at"] = pd.to_datetime(pdf["ingested_at"])
    pdf["fraud_probability"] = pdf["fraud_probability"].astype(float)
    pdf["amount"] = pdf["amount"].astype(float)
    pdf["avg_amount_24h"] = pdf["avg_amount_24h"].astype(float)
    pdf["count_24h"] = pdf["count_24h"].astype(int)
    return pdf


def render_metric_cards(df: pd.DataFrame) -> None:
    total = len(df)
    actual_fraud = int(df["actual_fraud"].sum())
    predicted_fraud = int(df["predicted_fraud"].sum())
    accuracy = float(df["is_correct"].mean()) if total else 0.0

    tp = int(((df["actual_fraud"] == 1) & (df["predicted_fraud"] == 1)).sum())
    fp = int(((df["actual_fraud"] == 0) & (df["predicted_fraud"] == 1)).sum())
    fn = int(((df["actual_fraud"] == 1) & (df["predicted_fraud"] == 0)).sum())
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0

    cols = st.columns(5)
    cols[0].metric("Transactions Shown", f"{total}")
    cols[1].metric("Actual Frauds", f"{actual_fraud}")
    cols[2].metric("Predicted Frauds", f"{predicted_fraud}")
    cols[3].metric("Accuracy", f"{accuracy:.1%}")
    cols[4].metric("Precision / Recall", f"{precision:.1%} / {recall:.1%}")


def render_charts(df: pd.DataFrame) -> None:
    trend_df = df.sort_values("id", ascending=True)

    fig_amount = px.scatter(
        trend_df,
        x="id",
        y="amount",
        color="outcome",
        size="fraud_probability",
        hover_data=["type", "actual_fraud", "predicted_fraud", "fraud_probability", "count_24h"],
        title="Recent Transactions Scored By The Model",
        color_discrete_map={
            "True Positive": "#c0392b",
            "True Negative": "#1f77b4",
            "False Positive": "#f39c12",
            "False Negative": "#8e44ad",
        },
    )
    fig_amount.update_layout(height=420, legend_title_text="Prediction Outcome")

    outcome_counts = df["outcome"].value_counts().rename_axis("outcome").reset_index(name="count")
    fig_outcomes = px.bar(
        outcome_counts,
        x="outcome",
        y="count",
        color="outcome",
        title="Prediction Outcomes In The Current Window",
        color_discrete_map={
            "True Positive": "#c0392b",
            "True Negative": "#1f77b4",
            "False Positive": "#f39c12",
            "False Negative": "#8e44ad",
        },
    )
    fig_outcomes.update_layout(height=420, showlegend=False)

    left, right = st.columns(2)
    left.plotly_chart(fig_amount, use_container_width=True)
    right.plotly_chart(fig_outcomes, use_container_width=True)


def render_table(df: pd.DataFrame) -> None:
    display_df = df[
        [
            "id",
            "step",
            "type",
            "amount",
            "avg_amount_24h",
            "count_24h",
            "fraud_probability",
            "predicted_fraud",
            "actual_fraud",
            "outcome",
            "ingested_at",
        ]
    ].copy()
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "fraud_probability": st.column_config.ProgressColumn(
                "Fraud Probability",
                min_value=0.0,
                max_value=1.0,
                format="%.3f",
            ),
            "predicted_fraud": st.column_config.NumberColumn("Predicted Fraud"),
            "actual_fraud": st.column_config.NumberColumn("Actual Fraud"),
            "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
            "avg_amount_24h": st.column_config.NumberColumn("Avg Amount 24h", format="%.2f"),
        },
    )


def main() -> None:
    st.set_page_config(
        page_title="Fraud Detection Dashboard",
        page_icon="chart_with_upwards_trend",
        layout="wide",
    )
    st.title("Fraud Detection Monitoring Dashboard")
    st.caption(
        "Live view of the latest scored Gold transactions. Each row is enriched with rolling features, "
        "scored by the latest trained model, and compared against the real fraud label."
    )

    with st.sidebar:
        st.header("Controls")
        refresh_seconds = st.slider("Auto-refresh (seconds)", min_value=5, max_value=60, value=DEFAULT_REFRESH_SECONDS)
        fraud_threshold = st.slider(
            "Fraud Decision Threshold",
            min_value=0.05,
            max_value=0.95,
            value=0.30,
            step=0.05,
            help="Transactions with fraud probability >= this threshold are flagged as fraud. Lower thresholds increase Recall (catch more fraud); higher thresholds increase Precision (fewer false alarms)."
        )
        total_gold_transactions = get_gold_transaction_count()
        show_all_transactions = st.checkbox("Show all transactions", value=False)
        if show_all_transactions:
            st.caption(f"Scoring all {total_gold_transactions:,} Gold transactions.")
            row_limit = None
        else:
            row_limit = st.slider("Transactions to score", min_value=50, max_value=5000, value=DEFAULT_LIMIT, step=50)
            st.caption(f"Gold transactions available: {total_gold_transactions:,}")
        st_autorefresh(interval=refresh_seconds * 1000, key="dashboard_refresh")
        st.button("Refresh Now", use_container_width=True)

    latest_run = get_latest_model_run()
    if latest_run is None:
        st.error("No finished MLflow run with a logged model artifact was found yet.")
        st.stop()

    st.info(
        f"Using MLflow run `{latest_run['run_id']}` with AUC "
        f"{latest_run['metrics'].get('auc', 0.0):.4f} and PR AUC {latest_run['metrics'].get('auc_pr', 0.0):.4f}."
    )

    try:
        recent_predictions = load_recent_predictions(latest_run["run_id"], row_limit, fraud_threshold)
    except Exception as exc:
        st.error(f"Failed to load model predictions: {exc}")
        st.stop()

    if recent_predictions.empty:
        st.warning("Gold layer is empty, so there are no transactions to score yet.")
        st.stop()

    render_metric_cards(recent_predictions)
    render_charts(recent_predictions)

    st.subheader("Latest Scored Transactions")
    render_table(recent_predictions)


if __name__ == "__main__":
    main()
