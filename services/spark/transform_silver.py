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
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def decode_amount(col_name):
    return expr(f"conv(hex(unbase64({col_name})), 16, 10) / 100").cast(DoubleType())

bronze_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("Step", IntegerType()),
    StructField("Type", StringType()),
    StructField("Amount", StringType()),
    StructField("NameOrig", StringType()),
    StructField("OldBalanceOrig", StringType()),
    StructField("NewBalanceOrig", StringType()),
    StructField("NameDest", StringType()),
    StructField("OldBalanceDest", StringType()),
    StructField("NewBalanceDest", StringType()),
    StructField("IsFraud", IntegerType()),
    StructField("IsFlaggedFraud", IntegerType()),
])


print("Starting Silver Transformation (Incremental Batch)...")

# 1. READ (Stream)
bronze_df = spark.readStream \
    .schema(bronze_schema) \
    .parquet(BRONZE_URL)

# 2. TRANSFORM
silver_df = bronze_df \
    .withColumnRenamed("Id", "id") \
    .withColumnRenamed("Step", "step") \
    .withColumnRenamed("Type", "type") \
    .withColumn("amount", decode_amount("Amount")) \
    .withColumnRenamed("NameOrig", "name_orig") \
    .withColumn("old_balance_orig", decode_amount("OldBalanceOrig")) \
    .withColumn("new_balance_orig", decode_amount("NewBalanceOrig")) \
    .withColumnRenamed("NameDest", "name_dest") \
    .withColumn("old_balance_dest", decode_amount("OldBalanceDest")) \
    .withColumn("new_balance_dest", decode_amount("NewBalanceDest")) \
    .withColumn("is_fraud", col("IsFraud").cast(BooleanType())) \
    .withColumn("is_flagged_fraud", col("IsFlaggedFraud").cast(BooleanType())) \
    .withColumn("ingested_at", current_timestamp()) \
    .dropDuplicates(["id"])

# 3. WRITE (Stream with Trigger)

query = silver_df.writeStream \
    .format("parquet") \
    .option("path", SILVER_URL) \
    .option("checkpointLocation", CHECKPOINT_URL) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()

print(f"Streaming query started. Checkpoint: {CHECKPOINT_URL}")
query.awaitTermination()
print("Transformation complete. Spark session stopping.")
