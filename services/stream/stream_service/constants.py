"""Stream Service - Constants and configuration from environment variables."""

import os

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Debezium
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")

# Stream Settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
ROWS_PER_SECOND = int(os.getenv("ROWS_PER_SECOND", "20"))
CSV_PATH = os.getenv("CSV_PATH", "/app/data/PS_20174392719_1491204439457_log.csv")

# Config paths
CONNECTORS_CONFIG_PATH = os.getenv("CONNECTORS_CONFIG_PATH", "/app/config/connectors.json")
