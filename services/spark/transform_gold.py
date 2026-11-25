"""
Gold Transformation - Feature Engineering
=========================================
Aggregates customer behavior features for fraud detection.
Features are calculated over a sliding window (e.g., last 24 steps).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, when
from pyspark.sql.window import Window
import os

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

SILVER_PATH = "s3a://lakehouse/silver/transactions"
GOLD_PATH = "s3a://lakehouse/gold/features"

spark = SparkSession.builder \
    .appName("Gold-Feature-Engineering") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.parquet.enable.dictionary", "false") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Starting Gold Feature Engineering...")

# 1. Load Silver Data
try:
    df = spark.read.parquet(SILVER_PATH)
    print(f"Loaded {df.count():,} records from Silver.")
except Exception as e:
    print(f"Error loading Silver data: {e}")
    spark.stop()
    exit(1)

# 2. Define Window Spec (Partition by User, Order by Time, Look back 24 steps)
window_spec = Window.partitionBy("name_orig").orderBy("step").rangeBetween(-24, 0)

# 3. Compute Features (Velocity + Balance Discrepancies)
features_df = df.withColumn("avg_amount_24h", avg("amount").over(window_spec)) \
                .withColumn("count_24h", count("id").over(window_spec)) \
                .withColumn("orig_balance_err", col("new_balance_orig") + col("amount") - col("old_balance_orig")) \
                .withColumn("dest_balance_err", col("old_balance_dest") + col("amount") - col("new_balance_dest")) \
                .withColumn("is_empty_balance", when((col("new_balance_orig") == 0) & (col("amount") > 0), 1.0).otherwise(0.0)) \
                .withColumn("ingested_at", current_timestamp())

# Select final features
gold_df = features_df.select(
    "id",
    "step",
    "name_orig",
    "amount",
    "type",
    "old_balance_orig",
    "new_balance_orig",
    "old_balance_dest",
    "new_balance_dest",
    "orig_balance_err",
    "dest_balance_err",
    "is_empty_balance",
    "avg_amount_24h",
    "count_24h",
    "is_fraud",
    "ingested_at"
)

# 4. Write to Gold
print("Writing features to Gold layer...")
gold_df.coalesce(4).write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(f"Gold features written to {GOLD_PATH}")
spark.stop()
