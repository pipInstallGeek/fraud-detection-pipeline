"""
Production Model Training & ONNX Export Pipeline
================================================
Trains a Random Forest classifier on Gold features with chronological split,
evaluates against the current production Champion via MLflow Model Registry,
and promotes to production only if safety floors and beat checks pass.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import numpy as np
import os
import shutil
import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
)
import skl2onnx
from skl2onnx.common.data_types import FloatTensorType
import mlflow
from mlflow import MlflowClient

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GOLD_PATH = "s3a://lakehouse/gold/features"
OUTPUT_ONNX_PATH = os.getenv("OUTPUT_ONNX_PATH", "/tmp/fraud_model.onnx")
PROD_MODEL_DIR = os.getenv("PROD_MODEL_DIR", "/models_prod")
MODEL_REGISTRY_NAME = "fraud-detection-onnx"

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASS

FEATURE_NAMES = [
    "step",
    "type_code",
    "amount",
    "old_balance_orig",
    "new_balance_orig",
    "old_balance_dest",
    "new_balance_dest",
    "orig_balance_err",
    "dest_balance_err",
    "is_empty_balance",
]

print("Starting Model Training & ONNX Export Pipeline...")

# 1. Initialize Spark Session
spark = (
    SparkSession.builder.appName("Train-ONNX-Fraud-Model")
    .config("spark.driver.memory", "4g")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 2. Load Gold Features from MinIO
print(f"Loading Gold dataset from: {GOLD_PATH}")
df = spark.read.parquet(GOLD_PATH).cache()
total_records = df.count()
print(f"Loaded {total_records:,} Gold records into memory cache.")

# Encode 'type' numerically: PAYMENT=0, TRANSFER=1, CASH_OUT=2, DEBIT=3, CASH_IN=4
df = df.withColumn(
    "type_code",
    when(col("type") == "PAYMENT", 0.0)
    .when(col("type") == "TRANSFER", 1.0)
    .when(col("type") == "CASH_OUT", 2.0)
    .when(col("type") == "DEBIT", 3.0)
    .when(col("type") == "CASH_IN", 4.0)
    .otherwise(0.0),
)

df_prepared = df.select(
    col("step").cast("float"),
    col("type_code").cast("float"),
    col("amount").cast("float"),
    col("old_balance_orig").cast("float"),
    col("new_balance_orig").cast("float"),
    col("old_balance_dest").cast("float"),
    col("new_balance_dest").cast("float"),
    col("orig_balance_err").cast("float"),
    col("dest_balance_err").cast("float"),
    col("is_empty_balance").cast("float"),
    col("is_fraud").cast("int"),
)

# 3. Chronological Time Split (Past -> Future)
quantiles = df_prepared.approxQuantile("step", [0.80], 0.01)
SPLIT_STEP = float(quantiles[0]) if quantiles else 200.0

print(f"Splitting chronologically at step {SPLIT_STEP:.1f} (80/20 train/test)...")
train_spark = df_prepared.filter(col("step") <= SPLIT_STEP)
test_spark = df_prepared.filter(col("step") > SPLIT_STEP)

if test_spark.count() == 0 or test_spark.filter(col("is_fraud") == 1).count() == 0:
    print("Chronological split left test set without fraud. Falling back to stratified 80/20 split.")
    train_spark, test_spark = df_prepared.randomSplit([0.80, 0.20], seed=42)

train_fraud = train_spark.filter(col("is_fraud") == 1)
train_non_fraud = train_spark.filter(col("is_fraud") == 0).sample(withReplacement=False, fraction=0.15, seed=42)
train_balanced_spark = train_fraud.union(train_non_fraud)

print("Converting training and testing datasets to NumPy...")
train_pd = train_balanced_spark.toPandas()
test_pd = test_spark.toPandas()

X_train = train_pd[FEATURE_NAMES].values.astype(np.float32)
y_train = train_pd["is_fraud"].values.astype(np.int64)

X_test = test_pd[FEATURE_NAMES].values.astype(np.float32)
y_test = test_pd["is_fraud"].values.astype(np.int64)

train_fraud_count = int(np.sum(y_train == 1))
train_non_fraud_count = int(np.sum(y_train == 0))
test_fraud_count = int(np.sum(y_test == 1))
test_non_fraud_count = int(np.sum(y_test == 0))

print(f"Training Set: {len(y_train):,} rows ({train_fraud_count:,} Fraud, {train_non_fraud_count:,} Non-Fraud)")
print(f"Test Set: {len(y_test):,} rows ({test_fraud_count:,} Fraud, {test_non_fraud_count:,} Non-Fraud)")

# 4. Train Model with Class Weighting
print("Training Balanced Random Forest Classifier...")
rf_model = RandomForestClassifier(
    n_estimators=60,
    max_depth=12,
    min_samples_leaf=3,
    class_weight="balanced_subsample",
    random_state=42,
    n_jobs=-1,
)
rf_model.fit(X_train, y_train)

# 5. Evaluate on Unseen Future Data
print("Evaluating model on test set...")
y_probs = rf_model.predict_proba(X_test)[:, 1]
THRESHOLD = 0.50
y_preds = (y_probs >= THRESHOLD).astype(np.int64)

roc_auc = float(roc_auc_score(y_test, y_probs))
pr_auc = float(average_precision_score(y_test, y_probs))
precision = float(precision_score(y_test, y_preds, zero_division=0))
recall = float(recall_score(y_test, y_preds, zero_division=0))
f1 = float(f1_score(y_test, y_preds, zero_division=0))
tn, fp, fn, tp = confusion_matrix(y_test, y_preds).ravel()

print("\n" + "=" * 55)
print("EVALUATION RESULTS (TEST SET)")
print("=" * 55)
print(f"  PR-AUC (Precision-Recall AUC): {pr_auc:.4f}")
print(f"  ROC-AUC:                       {roc_auc:.4f}")
print(f"  Fraud Precision:               {precision:.4f}")
print(f"  Fraud Recall:                  {recall:.4f}")
print(f"  Fraud F1-Score:                {f1:.4f}")
print(f"  True Positives (Caught Fraud): {tp:,} / {test_fraud_count:,}")
print(f"  False Positives (False Alarm): {fp:,}")
print(f"  False Negatives (Missed Fraud):{fn:,}")
print("=" * 55 + "\n")

# 6. Convert to ONNX Format
print("Converting trained model to ONNX...")
initial_type = [("float_input", FloatTensorType([None, len(FEATURE_NAMES)]))]
onnx_model = skl2onnx.convert_sklearn(
    rf_model,
    initial_types=initial_type,
    target_opset=15,
    options={id(rf_model): {"zipmap": False}},
)

with open(OUTPUT_ONNX_PATH, "wb") as f:
    f.write(onnx_model.SerializeToString())

file_size_mb = os.path.getsize(OUTPUT_ONNX_PATH) / (1024 * 1024)
print(f"ONNX model generated at: {OUTPUT_ONNX_PATH} ({file_size_mb:.2f} MB)")

# 7. Champion vs. Challenger Promotion Gate
try:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("fraud-detection-onnx")
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    with mlflow.start_run(run_name="challenger_rf_onnx") as run:
        trigger_reason = os.getenv("TRAINING_TRIGGER_REASON", "manual")
        mlflow.log_params({
            "trigger_reason": trigger_reason,
            "n_estimators": 60,
            "max_depth": 12,
            "split_step": float(SPLIT_STEP),
            "threshold": THRESHOLD,
            "class_weight": "balanced_subsample",
            "features": ",".join(FEATURE_NAMES),
        })

        metrics_dict = {
            "total_records": int(total_records),
            "train_records": int(len(y_train)),
            "test_records": int(len(y_test)),
            "pr_auc": pr_auc,
            "roc_auc": roc_auc,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "tp": int(tp),
            "fp": int(fp),
            "fn": int(fn),
            "tn": int(tn),
        }

        if os.getenv("TRAINING_NEW_ROWS_SINCE_LAST"):
            try:
                metrics_dict["new_rows_since_last_train"] = float(os.getenv("TRAINING_NEW_ROWS_SINCE_LAST"))
            except ValueError:
                pass

        mlflow.log_metrics(metrics_dict)
        mlflow.set_tag("framework", "scikit-learn / onnx")
        mlflow.set_tag("serving_target", "lakehouse-ingestion (.NET 10)")

        # Persist metrics report for timeout supervisor and MLflow artifact store
        report_data = {
            "mlflow": {
                "run_id": run.info.run_id,
                "experiment_name": "fraud-detection-onnx",
            },
            "metrics": metrics_dict,
            "total_records": int(total_records),
        }
        with open("/tmp/model_metrics.json", "w", encoding="utf-8") as rf:
            json.dump(report_data, rf, indent=2)

        mlflow.log_artifact("/tmp/model_metrics.json", artifact_path="reports")
        mlflow.log_artifact(OUTPUT_ONNX_PATH, artifact_path="model_onnx")

        # Register version in MLflow Model Registry
        model_uri = f"runs:/{run.info.run_id}/model_onnx"
        model_version = mlflow.register_model(model_uri=model_uri, name=MODEL_REGISTRY_NAME)
        print(f"Registered Challenger as Version {model_version.version} in MLflow Model Registry.")

        # Find current Champion in Production stage
        current_champion = None
        champion_metrics = {}
        try:
            for mv in client.search_model_versions(f"name='{MODEL_REGISTRY_NAME}'"):
                if mv.current_stage == "Production":
                    current_champion = mv
                    break
        except Exception as e:
            print(f"Notice: Could not search model versions: {e}")

        if current_champion:
            try:
                champ_run = client.get_run(current_champion.run_id)
                champion_metrics = champ_run.data.metrics
                print(f"Current Champion: Version {current_champion.version} (Run ID: {current_champion.run_id})")
                print(f"  Champion PR-AUC: {champion_metrics.get('pr_auc', 0.0):.4f} | Recall: {champion_metrics.get('recall', 0.0):.4f}")
            except Exception as e:
                print(f"Could not load Champion metrics: {e}")
        else:
            print("No active Champion found in Production. Evaluating Challenger against baseline safety floors.")

        # Gating Rules
        rejection_reasons = []

        # Floor Gate 1: Recall must be >= 0.90 (90% of fraud must be caught)
        if recall < 0.90:
            rejection_reasons.append(f"Recall ({recall:.4f}) is below minimum floor (0.90)")

        # Floor Gate 2: PR-AUC must be >= 0.85
        if pr_auc < 0.85:
            rejection_reasons.append(f"PR-AUC ({pr_auc:.4f}) is below minimum floor (0.85)")

        # Floor Gate 3: Precision must be >= 0.40
        if precision < 0.40:
            rejection_reasons.append(f"Precision ({precision:.4f}) is below minimum floor (0.40)")

        # Floor Gate 4: Degenerate model check
        if tp == 0:
            rejection_reasons.append("Degenerate model: Zero True Positives detected.")

        # Challenger vs. Champion Beat Checks
        if current_champion and champion_metrics and not rejection_reasons:
            champ_pr_auc = champion_metrics.get("pr_auc", 0.0)
            champ_recall = champion_metrics.get("recall", 0.0)

            if pr_auc < champ_pr_auc:
                rejection_reasons.append(f"Challenger PR-AUC ({pr_auc:.4f}) is lower than Champion ({champ_pr_auc:.4f})")
            elif recall < (champ_recall - 0.02):
                rejection_reasons.append(f"Challenger Recall ({recall:.4f}) regressed > 2% vs Champion ({champ_recall:.4f})")

        # Decision & Action
        if not rejection_reasons:
            print("\n" + "=" * 60)
            print("GATE PASSED: CHALLENGER PROMOTED TO PRODUCTION CHAMPION!")
            print("=" * 60)

            client.transition_model_version_stage(
                name=MODEL_REGISTRY_NAME,
                version=model_version.version,
                stage="Production",
                archive_existing_versions=True,
            )
            client.set_tag(run.info.run_id, "promotion_status", "PROMOTED_CHAMPION")

            # Deploy to production model directory for zero-downtime hot-reloading
            if os.path.exists(PROD_MODEL_DIR):
                target_path = os.path.join(PROD_MODEL_DIR, "fraud_model.onnx")
                shutil.copyfile(OUTPUT_ONNX_PATH, target_path)
                print(f"Published promoted model binary to: {target_path}")
        else:
            print("\n" + "=" * 60)
            print("GATE FAILED: CHALLENGER REJECTED (CHAMPION RETAINED)")
            print("=" * 60)
            for reason in rejection_reasons:
                print(f"  [REJECTION] {reason}")

            client.transition_model_version_stage(
                name=MODEL_REGISTRY_NAME,
                version=model_version.version,
                stage="Staging",
            )
            client.set_tag(run.info.run_id, "promotion_status", "REJECTED")
            client.set_tag(run.info.run_id, "rejection_reasons", "; ".join(rejection_reasons))

except Exception as e:
    print(f"WARNING: MLflow Model Registry operation skipped: {e}")

spark.stop()
print("Execution finished successfully.")
