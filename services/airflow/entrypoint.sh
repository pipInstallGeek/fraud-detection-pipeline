#!/bin/bash
set -e

# Add airflow user's bin to PATH
export PATH="/home/airflow/.local/bin:$PATH"

AIRFLOW_WEB_USERNAME="${AIRFLOW_USERNAME:-admin}"
AIRFLOW_WEB_PASSWORD="${AIRFLOW_PASSWORD:-admin}"

echo "Waiting for database connection..."
sleep 5

# Initialize the database (creates tables if they don't exist)
echo "Initializing Airflow database..."
airflow db migrate

# Create/update the web UI user (predictable credentials from .env / compose)
echo "Ensuring Airflow web user exists..."
# Delete first to guarantee the password matches (dev-only convenience).
airflow users delete --username "${AIRFLOW_WEB_USERNAME}" 2>/dev/null || true
airflow users create \
    --username "${AIRFLOW_WEB_USERNAME}" \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email "${AIRFLOW_WEB_USERNAME}@example.com" \
    --password "${AIRFLOW_WEB_PASSWORD}" \
    2>/dev/null || true

# Start Airflow webserver and scheduler (avoid `standalone` auto-user creation)
echo "Starting Airflow..."
airflow scheduler &
airflow triggerer &
exec airflow webserver
