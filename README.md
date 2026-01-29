# 🔍 Fraud Detection Pipeline

> Real-time fraud detection lakehouse using **Kafka**, **Debezium CDC**, **Spark Streaming**, and the **Medallion Architecture**.

[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](docker-compose.yml)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-black?logo=apachekafka)](https://kafka.apache.org/)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Quick Start](#quick-start)
- [Services & Ports](#services--ports)
- [Commands](#commands)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project implements a **production-ready fraud detection pipeline** that:

- 🚀 **Streams transactions in real-time** from PostgreSQL via Change Data Capture (CDC)
- 📊 **Processes data through medallion layers** (Bronze → Silver → Gold)
- 🤖 **Trains ML models** using Spark MLlib for fraud classification
- ☁️ **Stores everything in a lakehouse** on S3-compatible MinIO storage

### Tech Stack

| Layer | Technology |
|-------|------------|
| **Source Database** | PostgreSQL 15 |
| **Change Data Capture** | Debezium 2.4 |
| **Event Streaming** | Apache Kafka 7.5 |
| **Data Processing** | Apache Spark 3.5 |
| **Storage** | MinIO (S3-compatible) |
| **Orchestration** | Apache Airflow |
| **Containerization** | Docker Compose |

---

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌───────────────┐    ┌────────────────┐
│  Stream Service │───▶│  PostgreSQL  │───▶│   Debezium    │───▶│     Kafka      │
│  (PaySim data)  │    │ (transactions)│    │    (CDC)      │    │ (CDC events)   │
└─────────────────┘    └──────────────┘    └───────────────┘    └───────┬────────┘
                                                                        │
           ┌────────────────────────────────────────────────────────────┘
           ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              MinIO Lakehouse                                     │
│                                                                                  │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐  │
│   │    BRONZE    │────▶│    SILVER    │────▶│     GOLD     │────▶│   MODEL    │  │
│   │  Raw Events  │     │   Cleaned    │     │   Features   │     │  (MLlib)   │  │
│   └──────────────┘     └──────────────┘     └──────────────┘     └────────────┘  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## How It Works

### 1️⃣ Real-Time Transaction Simulation

The **Stream Service** simulates real-world transaction flow using the [PaySim dataset](https://www.kaggle.com/datasets/ealaxi/paysim1) (6M+ synthetic transactions):

- Reads transactions from CSV and inserts into PostgreSQL
- Configurable streaming rate (`BATCH_SIZE`, `ROWS_PER_SECOND`)
- Runs continuously until dataset is exhausted

### 2️⃣ Change Data Capture (CDC)

**Debezium** monitors PostgreSQL's Write-Ahead Log (WAL) and publishes every INSERT/UPDATE/DELETE to Kafka topic `cdc.public.transactions`.

### 3️⃣ Medallion Architecture

| Layer | Description | Format |
|-------|-------------|--------|
| **Bronze** | Raw CDC events from Kafka | Parquet |
| **Silver** | Decoded amounts, deduped, timestamped | Parquet |
| **Gold** | Fraud features (24h rolling stats) | Parquet |

### 4️⃣ ML Model Training

Spark MLlib trains a **Random Forest classifier** on Gold features:
- Features: `amount`, `avg_amount_24h`, `count_24h`, `type`
- Metric: Area Under ROC (AUC)
- Output: Model saved to `s3a://lakehouse/models/fraud_rf`

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Kaggle account (for dataset download)

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Kaggle credentials and passwords
```

### 2. Start the Pipeline

```bash
docker compose up -d --build
```

### 3. Monitor Progress

```bash
# Check all services are healthy
docker compose ps

# Watch transaction streaming
docker compose logs -f stream-service

# Watch Spark ingestion
docker compose logs -f lakehouse-ingestion
```

The pipeline will automatically:
- ✅ Download the PaySim dataset
- ✅ Register the Debezium CDC connector
- ✅ Stream transactions to PostgreSQL
- ✅ Ingest CDC events into Bronze layer

---

## Services & Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8082 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Kafka UI** | http://localhost:8085 | — |
| **Schema Registry** | http://localhost:8081 | — |
| **Debezium API** | http://localhost:8083 | — |
| **PostgreSQL** | localhost:5432 | from `.env` |

---

## Commands

### Logs

```bash
docker compose logs -f stream-service      # Transaction streaming
docker compose logs -f lakehouse-ingestion # Spark Bronze ingestion
docker compose logs -f airflow             # Airflow scheduler
```

### Manual Spark Jobs

```bash
# Run Silver transformation
docker compose exec lakehouse-ingestion spark-submit transform_silver.py

# Run Gold feature engineering
docker compose exec lakehouse-ingestion spark-submit transform_gold.py

# Train ML model
docker compose exec lakehouse-ingestion spark-submit train_model.py
```

### Debugging

```bash
# Check Debezium connector status
curl -s http://localhost:8083/connectors/postgres-connector/status | jq

# Consume Kafka messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.transactions --from-beginning --max-messages 5
```

### Lifecycle

```bash
docker compose down      # Stop (keep data)
docker compose down -v   # Stop and delete all data
docker compose build --no-cache  # Rebuild images
```

---

## Project Structure

```
fraud-detection-pipeline/
├── docker-compose.yml        # Service definitions
├── docker-compose.prod.yml   # Production overrides
├── .env.example              # Environment template
│
├── services/
│   ├── stream/               # Transaction streaming
│   │   └── stream_service/   # Python package
│   │
│   ├── spark/                # Spark jobs
│   │   ├── streaming_job.py  # Bronze ingestion
│   │   ├── transform_silver.py
│   │   ├── transform_gold.py
│   │   └── train_model.py
│   │
│   └── airflow/              # Workflow orchestration
│       ├── dags/
│       └── entrypoint.sh
│
└── infra/
    └── postgres/init/        # DB initialization scripts
```

---

## Troubleshooting

### Services not starting

```bash
docker compose ps                    # Check health status
docker compose logs postgres         # Database logs
docker compose logs kafka            # Kafka broker logs
```

### Debezium not capturing changes

```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status

# Restart stream-service to re-register
docker compose restart stream-service
```

### No data in MinIO

```bash
# Verify Kafka has messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.transactions --from-beginning --max-messages 1

# Check Spark logs
docker compose logs lakehouse-ingestion
```

---