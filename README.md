# Real-Time Fraud Detection Lakehouse & Microservice Pipeline

A production-grade, distributed fraud detection system combining an ultra-low latency **C# .NET 10 microservice** with embedded **ONNX machine learning inference**, an S3-compatible **MinIO Lakehouse (Medallion Architecture)**, **Debezium Change Data Capture (CDC)**, **Apache Kafka**, **Apache Spark**, **Apache Airflow**, and **MLflow**.

[![.NET 10](https://img.shields.io/badge/.NET-10.0-purple.svg?logo=dotnet)](https://dotnet.microsoft.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![ONNX Runtime](https://img.shields.io/badge/ONNX%20Runtime-1.32-blue.svg)](https://onnxruntime.ai/)
[![MinIO](https://img.shields.io/badge/MinIO-Lakehouse-C72C48.svg?logo=minio)](https://min.io/)
[![MLflow](https://img.shields.io/badge/MLflow-2.10-0194E2.svg?logo=mlflow)](https://mlflow.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose%20Profiles-2496ED.svg?logo=docker)](https://www.docker.com/)

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Key Features](#key-features)
- [Service Topology & Ports](#service-topology--ports)
- [Docker Compose Profiles](#docker-compose-profiles)
- [Quick Start](#quick-start)
- [Verification & Testing](#verification--testing)
- [Project Layout](#project-layout)
- [Operations & Maintenance](#operations--maintenance)

---

## System Architecture

```text
┌──────────────────────┐
│  Transaction Stream  │ (PaySim 6M+ dataset simulation)
└──────────┬───────────┘
           │ SQL INSERTs
           ▼
┌──────────────────────┐
│    PostgreSQL 15     │ (WAL level: logical)
└──────────┬───────────┘
           │ CDC via Debezium 2.4
           ▼
┌────────────────────────────────────────────────────────────────────────┐
│                          Apache Kafka 7.5                              │
│                                                                        │
│   Incoming: cdc.public.transactions   │   Fault-Tolerant: *.dlq        │
└──────────┬───────────────────────────────────────────▲─────────────────┘
           │                                           │
           │ Real-Time Polling                         │ Poison Pill Routing
           ▼                                           │ (Diagnostic Headers)
┌──────────────────────────────────────────────────────┴─────────────────┐
│              .NET 10 Real-Time Ingestion Microservice                  │
│                                                                        │
│   ├── Embedded ONNX Inference (<0.2ms latency, 10-feature vector)      │
│   ├── Resilient Dead Letter Queue (DLQ) & Strategic Offset Commits     │
│   └── System.Threading.Channels Asynchronous Micro-Batcher             │
└──────────┬─────────────────────────────────────────────────────────────┘
           │ Buffered Parquet Batches (5,000 rows)
           ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        MinIO S3 Lakehouse                              │
│                                                                        │
│   ├── Bronze: Raw micro-batches from CDC                               │
│   ├── Silver: Cleaned, deduplicated historical transactions            │
│   └── Gold:   Enriched sliding-window behavioral feature store         │
└──────────┬─────────────────────────────────────────────────────────────┘
           │ Scheduled Transformations & Validation
           ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     Orchestration & MLOps                              │
│                                                                        │
│   ├── Airflow 2.8: Orchestrates Silver/Gold ETL & Retraining Gates     │
│   ├── Great Expectations: 10-point data quality & schema contract      │
│   ├── PySpark 3.5: Feature engineering & Random Forest training        │
│   ├── MLflow 2.10: Experiment tracking & ONNX model registry           │
│   └── Streamlit: Live operational & fraud analytics dashboard          │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Key Features

### 1. Ultra-Fast Embedded ONNX Inference
- Directly embedded inside a high-throughput C# .NET 10 worker via `Microsoft.ML.OnnxRuntime`.
- Sub-millisecond scoring latency ($< 0.2\text{ ms}$) without HTTP or RPC microservice hops.
- Input feature vector (10 features) matches the PySpark training contract:
  - Base features: `step`, `type_code`, `amount`, `old_balance_orig`, `new_balance_orig`, `old_balance_dest`, `new_balance_dest`.
  - Engineered domain features: `orig_balance_err`, `dest_balance_err`, `is_empty_balance`.

### 2. Dead Letter Queue (DLQ) & Strategic Offset Management
- Clean Architecture separation: Domain interface in `Core`, Kafka transport implementation in `Infrastructure`.
- Catches unparseable payloads or malformed schema events ("poison pills").
- Routes failed messages to `cdc.public.transactions.dlq` enriched with audit headers (`x-error-reason`, `x-original-topic`, `x-original-offset`, `x-failed-at`).
- Commits offsets immediately on DLQ forwarding or in periodic micro-batches (every 1,000 messages) to eliminate partition blocking and redundant network round-trips.

### 3. Medallion Lakehouse Architecture
- **Bronze Layer (`lakehouse/bronze/transactions/`):** Raw Parquet files written by the .NET channel flusher.
- **Silver Layer (`lakehouse/silver/transactions/`):** Cleaned, typed, deduplicated historical data.
- **Gold Layer (`lakehouse/gold/features/`):** 24-step sliding window behavioral aggregates (velocity, balance discrepancies, drain flags).

### 4. Automated MLOps & Quality Gates
- **Airflow DAG (`silver_incremental_processing`):** Runs continuous pipeline updates and evaluates whether accumulated new Gold records exceed the retraining threshold.
- **Great Expectations:** Enforces null checks, schema conformance, range validation, and row-count gates before triggering training.
- **MLflow Tracking:** Logs PR-AUC, ROC-AUC, Precision, Recall, confusion matrices, and registers production ONNX binaries.

---

## Service Topology & Ports

| Service | Container Name | Port | Description |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | `postgres` | `5432` | Source transactional database |
| **Kafka Broker** | `kafka` | `9092` | Event streaming engine |
| **Zookeeper** | `zookeeper` | `2181` | Kafka cluster coordination |
| **Debezium** | `debezium` | `8083` | Change Data Capture (CDC) connector |
| **Consumer Service** | `lakehouse-ingestion` | *Internal* | C# .NET 10 ONNX inference & Lakehouse writer |
| **MinIO Console** | `minio` | `9001` (`9000` S3) | S3-compatible Lakehouse object store |
| **MLflow Server** | `mlflow` | `5000` | Experiment tracking & model registry |
| **Airflow UI** | `airflow` | `8082` | Pipeline orchestration & DAG scheduling |
| **Dashboard** | `dashboard` | `8501` | Streamlit real-time monitoring UI |
| **Kafka UI** *(dev profile)* | `kafka-ui` | `8085` | Kafka topic and consumer group inspector |
| **Prometheus** *(monitoring)* | `prometheus` | `9090` | Time-series metrics scraper |
| **Grafana** *(monitoring)* | `grafana` | `3001` | Infrastructure and system telemetry UI |

---

## Docker Compose Profiles

The cluster uses Docker Compose profiles to control resource consumption:

- **Core / Production (Default):** Runs the 10 core data pipeline and scoring services.
  ```bash
  docker compose up -d
  ```
- **Developer Profile (`--profile dev`):** Adds **Kafka UI** (port `8085`) and the transaction streamer service.
  ```bash
  docker compose --profile dev up -d
  ```
- **Monitoring Profile (`--profile monitoring`):** Adds Prometheus, Grafana, and system metric exporters.
  ```bash
  docker compose --profile monitoring up -d
  ```
- **Full Stack (`--profile all`):** Runs all services simultaneously.
  ```bash
  docker compose --profile all up -d
  ```

---

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose (v2.20+)
- Minimum 8 GB RAM allocated to Docker Engine
- .NET 10 SDK (only if modifying C# microservices locally)

### 2. Configure Environment
```bash
cp .env.example .env
# Verify credentials for PostgreSQL, MinIO, and Airflow
```

### 3. Launch the Pipeline
```bash
docker compose --profile dev up -d --build
```

### 4. Verify System Health
```bash
docker compose ps
```
All core services (`postgres`, `kafka`, `debezium`, `minio`, `lakehouse-ingestion`, `mlflow`, `airflow`, `dashboard`) should report `healthy` or `running`.

---

## Verification & Testing

### 1. Observe Live Scoring & DLQ
Stream transactions into PostgreSQL:
```bash
docker compose run --rm stream-service python -m stream_service.main
```

Follow the consumer logs:
```bash
docker compose logs -f lakehouse-ingestion
```
Sample output:
```text
info: FraudDetection.Consumer.Workers.KafkaPollerWorker[0]
      Subscribed to topic 'cdc.public.transactions'. DLQ enabled on 'cdc.public.transactions.dlq'.
warn: FraudDetection.Consumer.Workers.KafkaPollerWorker[0]
      [FRAUD_ALERT #1] Tx ID: 4541167 | Step: 326 | Type: TRANSFER | Fraud Prob: 100.00 % | Ground Truth: 1
info: FraudDetection.Infrastructure.Storage.MinioParquetWriter[0]
      Uploaded 5000 transactions to MinIO: bronze/transactions/batch_...parquet
```

### 2. Test Dead Letter Queue Fault Tolerance
Inject an invalid payload directly into the Kafka topic:
```bash
echo 'CORRUPTED_POISON_PILL' | docker compose exec -T kafka /bin/kafka-console-producer --bootstrap-server localhost:9092 --topic cdc.public.transactions
```

Inspect the consumer logs to verify non-blocking handling:
```text
fail: FraudDetection.Consumer.Workers.KafkaPollerWorker[0]
      Failed processing message at offset 1868887. Routing to DLQ.
warn: FraudDetection.Infrastructure.Kafka.KafkaDlqService[0]
      [DLQ_ROUTED] Offset 1868887 from topic 'cdc.public.transactions' routed to DLQ 'cdc.public.transactions.dlq'
```

Inspect the dead-lettered message in Kafka:
```bash
docker compose exec kafka /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic cdc.public.transactions.dlq --from-beginning --max-messages 1 --property print.headers=true
```

### 3. Verify Consumer Resumption & Offsets
Check the consumer group commit position and current lag:
```bash
docker compose exec kafka /bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group fraud-lakehouse-consumer-group
```

---

## Project Layout

```text
fraud-detection-pipeline/
├── data/                                 # Raw simulation dataset (ignored by git)
├── docs/                                 # Architectural reference guides
├── infra/
│   ├── grafana/                          # Dashboards and datasource provisioning
│   ├── monitoring/health_check.py        # Terminal health check script
│   ├── postgres/init/                    # DB schemas & logical replication config
│   └── prometheus/                       # Metrics scrapers & alerting rules
├── services/
│   ├── airflow/
│   │   └── dags/silver_pipeline_dag.py   # Automated incremental ETL & retraining DAG
│   ├── consumer/                         # .NET 10 Microservice Solution
│   │   ├── FraudDetection.Consumer/      # Host application & background workers
│   │   ├── FraudDetection.Core/          # Domain models & abstract contracts
│   │   └── FraudDetection.Infrastructure/# Kafka consumer/producer, ONNX ML, MinIO Parquet
│   ├── dashboard/app.py                  # Streamlit real-time monitoring frontend
│   ├── mlflow/Dockerfile                 # Tracking server with S3 artifact backend
│   ├── spark/                            # PySpark batch & training workloads
│   │   ├── check_retraining_threshold.py # Dynamic trigger evaluation
│   │   ├── run_training_with_timeout.py  # Guarded Spark runner
│   │   ├── train_model.py                # PySpark Random Forest classifier
│   │   ├── train_onnx_model.py           # Production ONNX exporter & evaluator
│   │   ├── transform_gold.py             # Feature engineering sliding-window job
│   │   ├── transform_silver.py           # Silver deduplication & typing job
│   │   └── validate_gold.py              # Great Expectations quality gate runner
│   └── stream/                           # Transaction simulation service
├── docker-compose.yml                    # Multi-profile container topology
├── LEARNING_ROADMAP.md                   # Comprehensive technical reference
└── README.md
```

---

## Operations & Maintenance

### Manual Spark Job Execution
```bash
# Execute Silver transformation
docker compose exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" /app/transform_silver.py

# Execute Gold feature engineering
docker compose exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" /app/transform_gold.py

# Execute Great Expectations data quality suite
docker compose exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" /app/validate_gold.py

# Run model training and ONNX export with Champion vs. Challenger gate
docker compose exec spark-processor /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g /app/train_onnx_model.py
```

### Clean Teardown
```bash
# Stop containers (preserves volume data)
docker compose down

# Reset environment and wipe volume data
docker compose down -v
```
