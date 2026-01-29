"""
Silver Transformation - NATIVE INCREMENTAL
==========================================
Uses Spark Structured Streaming with trigger(availableNow=True).
This natively handles S3 checkpointing and ensures only new data is processed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, IntegerType, BooleanType
import os

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BRONZE_URL = "s3a://lakehouse/bronze/transactions"
SILVER_URL = "s3a://lakehouse/silver/transactions"
# Important: Native Spark checkpoints are folders, not files
CHECKPOINT_URL = "s3a://lakehouse/checkpoints/silver_native"

spark = SparkSession.builder \
    .appName("Silver-Transformation-Native") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def decode_amount(col_name):
    return expr(f"conv(hex(unbase64({col_name})), 16, 10) / 100").cast(DoubleType())

bronze_schema = StructType([
    StructField("id", IntegerType()),
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", StringType()),
    StructField("name_orig", StringType()),
    StructField("old_balance_orig", StringType()),
    StructField("new_balance_orig", StringType()),
    StructField("name_dest", StringType()),
    StructField("old_balance_dest", StringType()),
    StructField("new_balance_dest", StringType()),
    StructField("is_fraud", IntegerType()),
    StructField("is_flagged_fraud", IntegerType()),
])

print("Starting Silver Transformation (Incremental Batch)...")

# 1. READ (Stream)
bronze_df = spark.readStream \
    .schema(bronze_schema) \
    .parquet(BRONZE_URL)

# 2. TRANSFORM
silver_df = bronze_df \
    .withColumn("amount", decode_amount("amount")) \
    .withColumn("old_balance_orig", decode_amount("old_balance_orig")) \
    .withColumn("new_balance_orig", decode_amount("new_balance_orig")) \
    .withColumn("old_balance_dest", decode_amount("old_balance_dest")) \
    .withColumn("new_balance_dest", decode_amount("new_balance_dest")) \
    .withColumn("is_fraud", col("is_fraud").cast(BooleanType())) \
    .withColumn("is_flagged_fraud", col("is_flagged_fraud").cast(BooleanType())) \
    .withColumn("ingested_at", current_timestamp()) \
    .dropDuplicates(["id"])  # Deduplicate within the batch

# 3. WRITE (Stream with Trigger)

query = silver_df.coalesce(1).writeStream \
    .format("parquet") \
    .option("path", SILVER_URL) \
    .option("checkpointLocation", CHECKPOINT_URL) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()

print(f"Streaming query started. Checkpoint: {CHECKPOINT_URL}")
query.awaitTermination()
print("Transformation complete. Spark session stopping.")
