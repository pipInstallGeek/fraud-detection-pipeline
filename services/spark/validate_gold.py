"""
Gold Layer Data Validation with Great Expectations
===================================================
Validates Gold features before ML training using Great Expectations.
Ensures data quality gates are passed before model training proceeds.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import os
import sys
from typing import Dict, List, Tuple

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

GOLD_PATH = "s3a://lakehouse/gold/features"
EXPECTATIONS_PATH = "/app/expectations/gold_features_suite.json"

spark = SparkSession.builder \
    .appName("Gold-Data-Validation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


def load_expectations(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_table_row_count(df, min_value: int) -> Tuple[bool, str]:
    count = df.count()
    passed = count >= min_value
    msg = f"Row count: {count:,} (min: {min_value})"
    return passed, msg


def validate_column_exists(df, column: str) -> Tuple[bool, str]:
    passed = column in df.columns
    msg = f"Column '{column}' exists: {passed}"
    return passed, msg


def validate_column_not_null(df, column: str) -> Tuple[bool, str]:
    if column not in df.columns:
        return False, f"Column '{column}' does not exist"
    null_count = df.filter(col(column).isNull()).count()
    total = df.count()
    passed = null_count == 0
    msg = f"Column '{column}' nulls: {null_count}/{total}"
    return passed, msg


def validate_column_between(df, column: str, min_value=None, max_value=None) -> Tuple[bool, str]:
    if column not in df.columns:
        return False, f"Column '{column}' does not exist"

    conditions = []
    if min_value is not None:
        conditions.append(col(column) < min_value)
    if max_value is not None:
        conditions.append(col(column) > max_value)

    if not conditions:
        return True, f"Column '{column}': no bounds specified"

    from functools import reduce
    combined = reduce(lambda a, b: a | b, conditions)
    violations = df.filter(combined).count()

    passed = violations == 0
    bounds = f"[{min_value}, {max_value}]"
    msg = f"Column '{column}' in range {bounds}: {violations} violations"
    return passed, msg


def validate_column_in_set(df, column: str, value_set: List) -> Tuple[bool, str]:
    if column not in df.columns:
        return False, f"Column '{column}' does not exist"

    normalized_values = value_set
    column_type = dict(df.dtypes).get(column)
    if column_type == "boolean":
        normalized_values = [
            bool(value) if value in (0, 1, True, False) else value
            for value in value_set
        ]

    violations = df.filter(~col(column).isin(normalized_values)).count()
    passed = violations == 0
    msg = f"Column '{column}' in set {normalized_values}: {violations} violations"
    return passed, msg


def run_validation(df, expectations: Dict) -> Tuple[bool, List[str]]:
    results = []
    all_passed = True

    for exp in expectations.get("expectations", []):
        exp_type = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})

        passed = False
        msg = f"Unknown expectation: {exp_type}"

        if exp_type == "expect_table_row_count_to_be_between":
            passed, msg = validate_table_row_count(df, kwargs.get("min_value", 0))
        elif exp_type == "expect_column_to_exist":
            passed, msg = validate_column_exists(df, kwargs["column"])
        elif exp_type == "expect_column_values_to_not_be_null":
            passed, msg = validate_column_not_null(df, kwargs["column"])
        elif exp_type == "expect_column_values_to_be_between":
            passed, msg = validate_column_between(
                df,
                kwargs["column"],
                kwargs.get("min_value"),
                kwargs.get("max_value")
            )
        elif exp_type == "expect_column_values_to_be_in_set":
            passed, msg = validate_column_in_set(df, kwargs["column"], kwargs["value_set"])

        status = "[PASS]" if passed else "[FAIL]"
        results.append(f"  {status} {msg}")

        if not passed:
            all_passed = False

    return all_passed, results


print("=" * 60)
print("GOLD LAYER DATA VALIDATION")
print("=" * 60)

try:
    print(f"\nLoading Gold features from: {GOLD_PATH}")
    df = spark.read.parquet(GOLD_PATH)
    print(f"   Loaded {df.count():,} records")
except Exception as e:
    print(f"\nERROR: Failed to load Gold data: {e}")
    spark.stop()
    sys.exit(1)

try:
    print(f"\nLoading expectations from: {EXPECTATIONS_PATH}")
    expectations = load_expectations(EXPECTATIONS_PATH)
    suite_name = expectations.get("expectation_suite_name", "unknown")
    print(f"   Suite: {suite_name}")
except Exception as e:
    print(f"\nERROR: Failed to load expectations: {e}")
    spark.stop()
    sys.exit(1)

print("\nRunning validation checks...")
all_passed, results = run_validation(df, expectations)

for result in results:
    print(result)

print("\n" + "=" * 60)
if all_passed:
    print("VALIDATION PASSED - Data is ready for training.")
    print("=" * 60)
    spark.stop()
    sys.exit(0)
else:
    print("VALIDATION FAILED - Data quality check did not pass.")
    print("=" * 60)
    spark.stop()
    sys.exit(1)
