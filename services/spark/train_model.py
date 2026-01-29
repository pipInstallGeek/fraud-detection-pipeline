"""
Fraud Model Training
====================
Trains a Random Forest classifier to detect fraud based on Gold features.
Saves the trained model to MinIO for serving.
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import os

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

GOLD_PATH = "s3a://lakehouse/gold/features"
MODEL_PATH = "s3a://lakehouse/models/fraud_rf"

spark = SparkSession.builder \
    .appName("Fraud-Model-Training") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🚀 Starting Model Training...")

# 1. Load Data
try:
    df = spark.read.parquet(GOLD_PATH)
    print(f"Loaded {df.count()} records from Gold.")
except Exception as e:
    print(f"Error loading Gold data: {e}")
    spark.stop()
    exit(1)

# 2. Prepare Data
# - Drop rows with nulls (if any)
# - Index 'type' column (categorical) -> numerical
df = df.dropna()

label_col = "is_fraud"
feature_cols = ["amount", "avg_amount_24h", "count_24h"]

# Convert 'type' to index
type_indexer = StringIndexer(inputCol="type", outputCol="type_index")
feature_cols.append("type_index")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 3. Split Data
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training on {train_data.count()} records, Testing on {test_data.count()}")

# 4. Train Model
rf = RandomForestClassifier(labelCol=label_col, featuresCol="features", numTrees=20)
pipeline = Pipeline(stages=[type_indexer, assembler, rf])

print("Training Random Forest...")
model = pipeline.fit(train_data)

# 5. Evaluate
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol=label_col, metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

print("=" * 40)
print(f"✅ Model Trained successfully!")
print(f"📊 Test AUC: {auc:.4f}")
print("=" * 40)

# 6. Save Model
print(f"Saving model to {MODEL_PATH}...")
model.write().overwrite().save(MODEL_PATH)

spark.stop()
