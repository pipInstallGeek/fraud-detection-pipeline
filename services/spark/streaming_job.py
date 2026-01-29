from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, unbase64, current_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, IntegerType, BooleanType
import os
import time
import boto3
from botocore.exceptions import ClientError


MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_ENDPOINT = "http://minio:9000"
BUCKET_NAME = "lakehouse"


def ensure_bucket_exists():
    """Create the lakehouse bucket in MinIO if it doesn't exist, handling retries."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
    )

    while True:
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' exists.")
            return
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                try:
                    print(f"Bucket '{BUCKET_NAME}' does not exist. Creating...")
                    s3.create_bucket(Bucket=BUCKET_NAME)
                    print(f"Bucket '{BUCKET_NAME}' created successfully.")
                    return
                except Exception as create_error:
                    print(f"Failed to create bucket: {create_error}. Retrying...")
            elif error_code == "403":
                print(f"Access denied to bucket '{BUCKET_NAME}'. Check credentials.")
                raise e
            else:
                print(f"Error checking bucket: {e}. Retrying...")
        except Exception as e:
            print(f"MinIO not reachable yet: {e}. Retrying in 5s...")
        
        time.sleep(5)


# Ensure bucket exists before starting Spark
ensure_bucket_exists()


spark = SparkSession.builder \
    .appName("Lakehouse-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


schema = StructType([
    StructField("after", StructType([
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
    ]))
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "cdc.public.transactions") \
    .option("startingOffsets", "earliest") \
    .load()


# --- BRONZE: Raw JSON ---
bronze_df = df.selectExpr("CAST(value AS STRING) as json_msg") \
    .select(from_json(col("json_msg"), schema).alias("data")) \
    .select("data.after.*") \
    .filter(col("step").isNotNull())


# --- Write Bronze ---
print("Starting Bronze Ingestion (Kafka -> MinIO Raw)...")
query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze") \
    .outputMode("append") \
    .start()

print("Ingestion running. Source: cdc.public.transactions")
query.awaitTermination()