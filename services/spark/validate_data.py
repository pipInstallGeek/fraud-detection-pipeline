"""
Data Consistency Validator
==========================
Compares Bronze and Silver layers to find missing or inconsistent records.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
import os

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BRONZE_PATH = "s3a://lakehouse/bronze/transactions"
SILVER_PATH = "s3a://lakehouse/silver/transactions"

spark = SparkSession.builder \
    .appName("Data-Consistency-Validator") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ============================================================================
# Helper: Write to file
# ============================================================================
REPORT_FILE = "/tmp/validation_output.txt"

def log(message):
    print(message)
    with open(REPORT_FILE, "a") as f:
        f.write(message + "\n")

# Clear file
with open(REPORT_FILE, "w") as f:
    f.write("")

log("=" * 60)
log("DATA CONSISTENCY REPORT")
log("=" * 60)

# ============================================================================
# Step 1: Load both layers
# ============================================================================
log("\n📂 Loading data...")

try:
    bronze_df = spark.read.parquet(BRONZE_PATH)
    bronze_count = bronze_df.count()
    bronze_unique = bronze_df.select("id").distinct().count()
    log(f"   Bronze: {bronze_count:,} total rows, {bronze_unique:,} unique IDs")
except Exception as e:
    log(f"   ❌ Bronze: Error - {e}")
    bronze_df = None
    bronze_count = 0
    bronze_unique = 0

try:
    silver_df = spark.read.parquet(SILVER_PATH)
    silver_count = silver_df.count()
    silver_unique = silver_df.select("id").distinct().count()
    log(f"   Silver: {silver_count:,} total rows, {silver_unique:,} unique IDs")
except Exception as e:
    log(f"   ⚠️  Silver: No data yet (run Silver transformation first)")
    silver_df = None
    silver_count = 0
    silver_unique = 0

# ============================================================================
# Step 2: Find missing records
# ============================================================================
log("\n🔍 Checking for missing records...")

if bronze_df and silver_df:
    # IDs in Bronze but NOT in Silver
    bronze_ids = bronze_df.select("id").distinct()
    silver_ids = silver_df.select("id").distinct()
    
    missing_in_silver = bronze_ids.join(silver_ids, on="id", how="left_anti")
    missing_count = missing_in_silver.count()
    
    if missing_count > 0:
        log(f"   ⚠️  {missing_count:,} records in Bronze but NOT in Silver")
        log(f"   → Run Silver transformation to sync")
    else:
        log(f"   ✅ All Bronze records exist in Silver!")
    
    # Check for duplicates in Silver
    silver_dups = silver_df.groupBy("id").count().filter(col("count") > 1)
    dup_count = silver_dups.count()
    
    if dup_count > 0:
        log(f"\n   ⚠️  {dup_count:,} duplicate IDs found in Silver")
    else:
        log(f"   ✅ No duplicates in Silver")

# ============================================================================
# Step 3: Summary
# ============================================================================
log("\n" + "=" * 60)
log("SUMMARY")
log("=" * 60)

coverage = (silver_unique / bronze_unique * 100) if bronze_unique > 0 else 0

log(f"""
   Bronze Layer:     {bronze_unique:,} unique records
   Silver Layer:     {silver_unique:,} unique records
   Coverage:         {coverage:.1f}%
   Missing Records:  {bronze_unique - silver_unique:,}
""")

if coverage >= 100:
    log("   ✅ STATUS: FULLY SYNCED")
elif coverage >= 90:
    log("   ⚠️  STATUS: MOSTLY SYNCED (run Silver transformation)")
else:
    log("   ❌ STATUS: OUT OF SYNC (run Silver transformation)")

log("=" * 60)
spark.stop()
