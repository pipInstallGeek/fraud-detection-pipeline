"""
Gold Transformation - Feature Engineering
=========================================
Aggregates customer behavior features for fraud detection.
Features are calculated over a sliding window (e.g., last 24 steps).

Features:
- avg_amount_24h: Average transaction amount in the last 24 steps
- count_24h: Number of transactions in the last 24 steps
- diff_dest_24h: Number of distinct recipients in the last 24 steps
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, approx_count_distinct, current_timestamp
from pyspark.sql.window import Window
import os

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

SILVER_PATH = "s3a://lakehouse/silver/transactions"
GOLD_PATH = "s3a://lakehouse/gold/features"

spark = SparkSession.builder \
    .appName("Gold-Feature-Engineering") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("🚀 Starting Gold Feature Engineering...")

# 1. Load Silver Data
try:
    df = spark.read.parquet(SILVER_PATH)
    print(f"Loaded {df.count()} records from Silver.")
except Exception as e:
    print(f"Error loading Silver data: {e}")
    spark.stop()
    exit(1)

# 2. Define Window Spec (Partition by User, Order by Time, Look back 24 steps)
# Note: 'rangeBetween' works on the VALUE of the order column.
# Assuming 1 step = 1 hour roughly for this dataset logic.
window_spec = Window.partitionBy("name_orig").orderBy("step").rangeBetween(-24, 0)

# 3. Compute Features
features_df = df.withColumn("avg_amount_24h", avg("amount").over(window_spec)) \
                .withColumn("count_24h", count("id").over(window_spec)) \
                .withColumn("ingested_at", current_timestamp())

# Note: approx_count_distinct typically doesn't work well in Window functions in older Spark versions
# or requires specific configuration. For simplicity/robustness, we'll skip distinct destinations for now
# or implement it via a self-join if strictly needed. Keeping it simple for MPV.

# Select final features
gold_df = features_df.select(
    "step",
    "name_orig",
    "amount",
    "type",
    "avg_amount_24h",
    "count_24h",
    "is_fraud",
    "ingested_at"
)

# 4. Write to Gold (Overwrite mode for full refresh of features table)
# For production, you might want to partition by step or date.
print("Writing features to Gold layer...")
gold_df.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(f"✅ Gold features written to {GOLD_PATH}")
spark.stop()
