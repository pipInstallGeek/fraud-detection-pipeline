#!/bin/bash
set -e

# Add airflow user's bin to PATH
export PATH="/home/airflow/.local/bin:$PATH"

echo "Waiting for database connection..."
sleep 5

# Initialize the database (creates tables if they don't exist)
echo "Initializing Airflow database..."
airflow db init

# Create airflow user if it doesn't exist
echo "Creating Airflow user..."
airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname User \
    --role Admin \
    --email airflow@example.com \
    --password airflow \
    2>/dev/null || echo "Airflow user already exists"

# Start Airflow webserver and scheduler
echo "Starting Airflow..."
exec airflow standalone
