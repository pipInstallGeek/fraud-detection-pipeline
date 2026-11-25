"""
Fraud Model Training with MLflow Tracking
==========================================
Trains a Random Forest classifier to detect fraud based on Gold features.
Logs experiments to MLflow and registers the model in the Model Registry.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark
import json
import os
import sys

# =============================================================================
# Configuration
# =============================================================================
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
REGISTER_MODEL_IN_MLFLOW = os.getenv("MLFLOW_REGISTER_MODEL", "false").lower() == "true"
SAVE_LEGACY_MODEL = os.getenv("SAVE_LEGACY_MODEL", "false").lower() == "true"

GOLD_PATH = "s3a://lakehouse/gold/features"
MODEL_PATH = "s3a://lakehouse/models/fraud_rf"
METRICS_REPORT_PATH = "/tmp/model_metrics.json"

# MLflow configuration
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASS

# Hyperparameters
NUM_TREES = 35
MAX_DEPTH = 8
TRAIN_SPLIT = 0.8
SEED = 42
TRAINING_TRIGGER_REASON = os.getenv("TRAINING_TRIGGER_REASON", "manual")
TRAINING_NEW_ROWS_SINCE_LAST = os.getenv("TRAINING_NEW_ROWS_SINCE_LAST", "0")
TRAINING_PREVIOUS_TOTAL_RECORDS = os.getenv("TRAINING_PREVIOUS_TOTAL_RECORDS", "")
RETRAINING_MIN_NEW_ROWS = os.getenv("RETRAINING_MIN_NEW_ROWS", "")

# =============================================================================
# Spark Session
# =============================================================================
spark = SparkSession.builder \
    .appName("Fraud-Model-Training") \
    .config("spark.hadoop.fs.s3.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3.path.style.access", "true") \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.endpoint", "http://minio:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.access.key", MINIO_USER)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.secret.key", MINIO_PASS)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("Starting Model Training with MLflow Tracking...")

# =============================================================================
# MLflow Setup
# =============================================================================
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("fraud-detection")

# =============================================================================
# 1. Load Data
# =============================================================================
try:
    df = spark.read.parquet(GOLD_PATH)
    total_records = df.count()
    print(f"Loaded {total_records:,} records from Gold layer.")
except Exception as e:
    print(f"Error loading Gold data: {e}")
    spark.stop()
    exit(1)

# =============================================================================
# 2. Prepare Data
# =============================================================================
df = df.dropna()
df = df.withColumn("is_fraud", col("is_fraud").cast("double"))

label_col = "is_fraud"
feature_cols = [
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
]

# Convert 'type' to index
type_indexer = StringIndexer(inputCol="type", outputCol="type_index")
feature_cols_with_type = feature_cols + ["type_index"]

assembler = VectorAssembler(inputCols=feature_cols_with_type, outputCol="features")

# =============================================================================
# 3. Split Data
# =============================================================================
train_data, test_data = df.randomSplit([TRAIN_SPLIT, 1 - TRAIN_SPLIT], seed=SEED)
train_count = train_data.count()
test_count = test_data.count()
print(f"Training: {train_count:,} records | Testing: {test_count:,} records")

if train_count == 0 or test_count == 0:
    print("Training or test split is empty. Cannot train model.")
    spark.stop()
    exit(1)

# Calculate class balance weight on train_data
fraud_count = train_data.filter(col(label_col) == 1.0).count()
non_fraud_count = train_data.filter(col(label_col) == 0.0).count()
fraud_weight = float(non_fraud_count) / max(float(fraud_count), 1.0)
print(f"Class Weighting: Fraud={fraud_count:,}, Non-Fraud={non_fraud_count:,}, Multiplier={fraud_weight:.1f}x")

train_data = train_data.withColumn(
    "class_weight",
    when(col(label_col) == 1.0, lit(fraud_weight)).otherwise(lit(1.0))
)

# =============================================================================
# 4. Train Model with MLflow Tracking
# =============================================================================
with mlflow.start_run(run_name="random_forest_training") as run:
    
    # Log parameters
    mlflow.log_param("num_trees", NUM_TREES)
    mlflow.log_param("max_depth", MAX_DEPTH)
    mlflow.log_param("fraud_weight", round(fraud_weight, 2))
    mlflow.log_param("train_split", TRAIN_SPLIT)
    mlflow.log_param("seed", SEED)
    mlflow.log_param("feature_cols", feature_cols_with_type)
    mlflow.log_param("label_col", label_col)
    mlflow.log_param("training_trigger_reason", TRAINING_TRIGGER_REASON)
    mlflow.log_param("training_new_rows_since_last", TRAINING_NEW_ROWS_SINCE_LAST)
    mlflow.log_param("training_previous_total_records", TRAINING_PREVIOUS_TOTAL_RECORDS)
    mlflow.log_param("retraining_min_new_rows", RETRAINING_MIN_NEW_ROWS)
    
    # Log dataset info
    mlflow.log_param("gold_path", GOLD_PATH)
    mlflow.log_metric("total_records", total_records)
    mlflow.log_metric("train_records", train_count)
    mlflow.log_metric("test_records", test_count)
    
    # Build and train pipeline with class_weight
    rf = RandomForestClassifier(
        labelCol=label_col,
        featuresCol="features",
        weightCol="class_weight",
        numTrees=NUM_TREES,
        maxDepth=MAX_DEPTH,
        seed=SEED
    )
    pipeline = Pipeline(stages=[type_indexer, assembler, rf])
    
    print("Training Weighted Random Forest...")
    model = pipeline.fit(train_data)
    
    # Evaluate
    predictions = model.transform(test_data)
    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        metricName="areaUnderROC"
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        metricName="areaUnderPR"
    )
    multiclass_evaluator = MulticlassClassificationEvaluator(labelCol=label_col)
    auc = roc_evaluator.evaluate(predictions)
    pr_auc = pr_evaluator.evaluate(predictions)

    accuracy = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "accuracy"})
    f1_score = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "f1"})
    precision = multiclass_evaluator.evaluate(
        predictions, {multiclass_evaluator.metricName: "weightedPrecision"}
    )
    recall = multiclass_evaluator.evaluate(
        predictions, {multiclass_evaluator.metricName: "weightedRecall"}
    )

    confusion_row = predictions.select(
        spark_sum(((col(label_col) == 1.0) & (col("prediction") == 1.0)).cast("int")).alias("tp"),
        spark_sum(((col(label_col) == 0.0) & (col("prediction") == 0.0)).cast("int")).alias("tn"),
        spark_sum(((col(label_col) == 0.0) & (col("prediction") == 1.0)).cast("int")).alias("fp"),
        spark_sum(((col(label_col) == 1.0) & (col("prediction") == 0.0)).cast("int")).alias("fn"),
    ).first()

    tp = int(confusion_row["tp"] or 0)
    tn = int(confusion_row["tn"] or 0)
    fp = int(confusion_row["fp"] or 0)
    fn = int(confusion_row["fn"] or 0)

    fraud_count = int(df.filter(col(label_col) == 1.0).count())
    non_fraud_count = int(total_records - fraud_count)
    fraud_rate = fraud_count / total_records if total_records > 0 else 0
    precision_fraud = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall_fraud = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1_fraud = (
        2 * precision_fraud * recall_fraud / (precision_fraud + recall_fraud)
        if (precision_fraud + recall_fraud) > 0
        else 0.0
    )

    metrics = {
        "dataset": {
            "total_records": int(total_records),
            "train_records": int(train_count),
            "test_records": int(test_count),
            "fraud_count": fraud_count,
            "non_fraud_count": non_fraud_count,
            "fraud_rate": fraud_rate,
        },
        "model_metrics": {
            "auc_roc": auc,
            "auc_pr": pr_auc,
            "accuracy": accuracy,
            "weighted_precision": precision,
            "weighted_recall": recall,
            "weighted_f1": f1_score,
            "fraud_precision": precision_fraud,
            "fraud_recall": recall_fraud,
            "fraud_f1": f1_fraud,
        },
        "confusion_matrix": {
            "true_positive": tp,
            "true_negative": tn,
            "false_positive": fp,
            "false_negative": fn,
        },
        "mlflow": {
            "tracking_uri": MLFLOW_TRACKING_URI,
            "run_id": run.info.run_id,
        },
    }

    with open(METRICS_REPORT_PATH, "w", encoding="utf-8") as metrics_file:
        json.dump(metrics, metrics_file, indent=2)

    # Log metrics
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("auc_pr", pr_auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("weighted_precision", precision)
    mlflow.log_metric("weighted_recall", recall)
    mlflow.log_metric("weighted_f1", f1_score)
    mlflow.log_metric("fraud_precision", precision_fraud)
    mlflow.log_metric("fraud_recall", recall_fraud)
    mlflow.log_metric("fraud_f1", f1_fraud)

    fraud_rate = fraud_count / total_records if total_records > 0 else 0
    mlflow.log_metric("fraud_rate", fraud_rate)
    mlflow.log_metric("fraud_count", fraud_count)
    
    print("=" * 50)
    print(f"Model Trained Successfully.")
    print(f"Test AUC: {auc:.4f}")
    print(f"Test PR AUC: {pr_auc:.4f}")
    print(f"Fraud Precision: {precision_fraud:.4f}")
    print(f"Fraud Recall: {recall_fraud:.4f}")
    print(f"Fraud F1: {f1_fraud:.4f}")
    print(f"Fraud Rate: {fraud_rate:.4%} ({fraud_count:,} / {total_records:,})")
    print(f"MLflow Run ID: {run.info.run_id}")
    print(f"Metrics report: {METRICS_REPORT_PATH}")
    print("=" * 50)
    
    mlflow.log_artifact(METRICS_REPORT_PATH, artifact_path="reports")

    # Persist the metrics report before optional registry work so the run
    # always contains the evaluation output even if model registration stalls.
    print("Logging model to MLflow...")
    log_model_kwargs = {"artifact_path": "model"}
    if REGISTER_MODEL_IN_MLFLOW:
        log_model_kwargs["registered_model_name"] = "fraud-detection-rf"
    mlflow.spark.log_model(model, **log_model_kwargs)
    
    if SAVE_LEGACY_MODEL:
        print(f"Saving model to {MODEL_PATH}...")
        model.write().overwrite().save(MODEL_PATH)
    else:
        print("Skipping legacy model save. Set SAVE_LEGACY_MODEL=true to enable it.")
    
    print(f"Training complete. View results at: {MLFLOW_TRACKING_URI}")
    mlflow.end_run(status="FINISHED")
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)

spark.stop()
