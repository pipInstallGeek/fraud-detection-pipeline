# 📖 Fraud Detection Pipeline: The Ultimate Concepts & Configuration Handbook

This handbook explains **every core concept, architectural pattern, engineering mechanism, and configuration parameter** used in the Fraud Detection Lakehouse Pipeline.

---

# Table of Contents
1. [Core Architectural Concepts](#1-core-architectural-concepts)
   - [1.1 Change Data Capture (CDC) & PostgreSQL WAL](#11-change-data-capture-cdc--postgresql-wal)
   - [1.2 Distributed Event Streaming (Kafka & Zookeeper)](#12-distributed-event-streaming-kafka--zookeeper)
   - [1.3 Schema Governance (Schema Registry)](#13-schema-governance-schema-registry)
   - [1.4 The Lakehouse & Medallion Architecture (Bronze, Silver, Gold)](#14-the-lakehouse--medallion-architecture-bronze-silver-gold)
   - [1.5 Object Storage vs File Systems (MinIO / S3 API)](#15-object-storage-vs-file-systems-minio--s3-api)
   - [1.6 Columnar Storage & Parquet Internals](#16-columnar-storage--parquet-internals)
   - [1.7 Distributed Processing with Apache Spark](#17-distributed-processing-with-apache-spark)
   - [1.8 Spark Structured Streaming & Checkpointing](#18-spark-structured-streaming--checkpointing)
   - [1.9 Temporal Sliding Window Feature Engineering](#19-temporal-sliding-window-feature-engineering)
   - [1.10 Automated Data Quality Gates (Great Expectations)](#110-automated-data-quality-gates-great-expectations)
   - [1.11 Continuous MLOps & Experiment Tracking (MLflow)](#111-continuous-mlops--experiment-tracking-mlflow)
   - [1.12 Machine Learning for Imbalanced Fraud Classification](#112-machine-learning-for-imbalanced-fraud-classification)
   - [1.13 Full-Stack Observability (Prometheus, Grafana & Exporters)](#113-full-stack-observability-prometheus-grafana--exporters)
   - [1.14 High-Speed Parquet Footer Metadata Inspection](#114-high-speed-parquet-footer-metadata-inspection)
2. [Exhaustive Configuration Reference](#2-exhaustive-configuration-reference)
   - [2.1 Environment Variables (.env)](#21-environment-variables-env)
   - [2.2 Docker Compose Services & Infrastructure](#22-docker-compose-services--infrastructure)
   - [2.3 PostgreSQL Engine & Replication Parameters](#23-postgresql-engine--replication-parameters)
   - [2.4 Kafka & Zookeeper Networking & Listeners](#24-kafka--zookeeper-networking--listeners)
   - [2.5 Debezium Connector Configuration](#25-debezium-connector-configuration)
   - [2.6 Apache Spark & Hadoop S3A Parameters](#26-apache-spark--hadoop-s3a-parameters)
   - [2.7 Apache Airflow DAG & Task Configuration](#27-apache-airflow-dag--task-configuration)
   - [2.8 MLflow Server Parameters](#28-mlflow-server-parameters)
   - [2.9 Prometheus Scrape Targets & Alert Rules](#29-prometheus-scrape-targets--alert-rules)
   - [2.10 Grafana Provisioning & Auth Parameters](#210-grafana-provisioning--auth-parameters)
3. [Engineering Glossary & Acronyms](#3-engineering-glossary--acronyms)

---

# 1. Core Architectural Concepts

---

### 1.1 Change Data Capture (CDC) & PostgreSQL WAL
* **What is it?** Change Data Capture is a pattern where every database mutation (`INSERT`, `UPDATE`, `DELETE`) is captured immediately as an event and pushed to a downstream system.
* **Why not simple polling?** The naive way to sync data is querying `SELECT * FROM transactions WHERE created_at > last_sync_time`. This causes severe database lock contention, misses hard deletions, puts heavy CPU load on the relational database, and creates synchronization lag.
* **How WAL works:** PostgreSQL logs every transaction to an append-only binary disk log called the **Write-Ahead Log (WAL)** *before* modifying data files on disk. Debezium connects to PostgreSQL as a replication client using the `pgoutput` plugin. It tails the WAL at the byte level with **zero query load** on the active transaction tables.

---

### 1.2 Distributed Event Streaming (Kafka & Zookeeper)
* **Apache Kafka:** A distributed, append-only commit log designed for high-throughput, low-latency data feeds.
* **Topic:** A named stream of records (e.g., `cdc.public.transactions`).
* **Partition:** The unit of parallelism in Kafka. A topic is split into partitions. Ordering is guaranteed *only within a single partition*.
* **Offset:** A sequential integer assigned to each record in a partition. It represents the exact position of a message.
* **Consumer Group:** A set of consumers cooperating to read from a topic. Kafka tracks the consumer group’s current offset. If a consumer crashes, another consumer in the group resumes from the last committed offset.
* **Consumer Lag:** The delta between the latest message produced to a partition and the current offset processed by the consumer. High lag indicates a downstream bottleneck.
* **Zookeeper:** The distributed coordinator that manages Kafka broker metadata, leader elections for topic partitions, and cluster membership.

---

### 1.3 Schema Governance (Schema Registry)
* **Problem:** In microservices and event pipelines, an upstream developer might change a column from `FLOAT` to `STRING` or drop a field, silently breaking downstream Spark and ML jobs.
* **Confluent Schema Registry:** A centralized HTTP server storing Avro/JSON schemas for Kafka topics. Producers and consumers register and validate schemas. It enforces compatibility rules (e.g., `BACKWARD`, `FORWARD`, `FULL`) preventing corrupt or incompatible message schemas from entering the pipeline.

---

### 1.4 The Lakehouse & Medallion Architecture (Bronze, Silver, Gold)
A Lakehouse combines the cost efficiency and scale of Object Storage (Data Lake) with the data structure and management features of a Data Warehouse.
The **Medallion Architecture** organizes data into three distinct tiers of increasing quality:

```
[Raw Stream] ──► BRONZE (Raw Ingest) ──► SILVER (Cleaned & Validated) ──► GOLD (Features & Aggregates)
```

1. **Bronze Layer (`/lakehouse/bronze/`):**
   * Raw, unedited, append-only history.
   * Preserves original source data as arrived from Kafka.
   * Allows complete pipeline reprocessing from scratch if downstream logic changes.
2. **Silver Layer (`/lakehouse/silver/`):**
   * Cleansed, standardized, typed, and deduplicated data.
   * Decodes binary types (such as Debezium decimal byte arrays), parses dates, casts booleans.
   * Deduplicated on unique primary key (`id`).
3. **Gold Layer (`/lakehouse/gold/`):**
   * Business-level aggregates, feature sets, and analytics-ready tables.
   * Contains window aggregations, velocity features, customer risk statistics.
   * Directly consumed by ML models and reporting dashboards.

---

### 1.5 Object Storage vs File Systems (MinIO / S3 API)
* **POSIX File System (e.g., ext4, NTFS):** Organizes files in hierarchical folders. Fast directory listing, atomic renames, but cannot scale horizontally across commodity servers.
* **Object Storage (AWS S3, MinIO):** Flat key-value store. A "path" like `bronze/transactions/part-001.parquet` is simply a key string inside a bucket. It scales infinitely and separates storage cost from compute cost.
* **MinIO:** A high-performance, S3-compatible open-source object storage server written in Go. In this pipeline, it serves as the private S3-compatible cloud storage layer.

---

### 1.6 Columnar Storage & Parquet Internals
* **Row-oriented (CSV, JSON, PostgreSQL tables):** Stores data row by row: `[Row 1: id, amount, type] [Row 2: id, amount, type]`. Great for transactional writes (`OLTP`), terrible for analytical queries (`OLAP`).
* **Columnar (Apache Parquet):** Stores data column by column: `[All IDs...] [All Amounts...] [All Types...]`.
* **Why Parquet is superior for Analytics & ML:**
  1. **Column Pruning:** If a model only needs `amount` and `type`, Spark only reads those specific byte ranges from disk/S3, skipping the other columns completely.
  2. **High Compression Ratio:** Identical data types stored together compress dramatically better (Snappy, GZIP).
  3. **Metadata Footers:** Every Parquet file ends with a metadata footer containing dictionary encoding, column statistics (`min`, `max`, `null_count`), and total `num_rows`. You can read the row count of a 5GB Parquet file in **2 milliseconds** by reading only its trailing footer bytes.

---

### 1.7 Distributed Processing with Apache Spark
* **Spark Core:** A distributed, memory-first computation engine that distributes data across worker nodes using **Resilient Distributed Datasets (RDDs)** and DataFrames.
* **Catalyst Optimizer:** Spark's query optimizer that compiles functional Python/SQL code into an optimized execution plan (predicate pushdown, projection pruning).
* **Tungsten Engine:** Spark's low-level execution engine that manages off-heap binary memory to avoid Python and JVM garbage-collection overhead.

---

### 1.8 Spark Structured Streaming & Checkpointing
* **Structured Streaming:** A stream-processing engine built on the Catalyst optimizer. It treats a live data stream as an **unbounded, continuously appended table**.
* **Micro-Batch Processing:** Rather than processing one event at a time, Spark pulls events in small micro-batches (e.g., every 500ms or 5s), achieving massive throughput.
* **Checkpointing (`checkpointLocation`):** S3 folder where Spark writes write-ahead logs of offsets processed from Kafka. If the streaming container crashes, restarting it reads the checkpoint and resumes exactly where it left off, ensuring **at-least-once fault tolerance**.
* **`trigger(availableNow=True)`:** A breakthrough feature used in [`transform_silver.py`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/services/spark/transform_silver.py). It allows Spark to run in **batch mode using the streaming engine**. It reads all newly arrived files since the last checkpoint, processes them incrementally, commits the checkpoint, and shuts down cleanly.

---

### 1.9 Temporal Sliding Window Feature Engineering
* In fraud detection, single transactions look normal in isolation. Fraud is revealed through **velocity signals**:
  - *"Did this user send 10 payments in the last 60 minutes?"*
  - *"Is this transaction amount 5x higher than their 24-hour average?"*
* **Spark Window Function:**
  ```python
  Window.partitionBy("name_orig").orderBy("step").rangeBetween(-24, 0)
  ```
  - `partitionBy("name_orig")`: Isolates calculations per customer account.
  - `orderBy("step")`: Orders transactions chronologically (`step` = hour).
  - `rangeBetween(-24, 0)`: Creates a rolling frame looking back 24 time steps from the current transaction's timestamp.
  - Computes `avg_amount_24h` and `count_24h`.

---

### 1.10 Automated Data Quality Gates (Great Expectations)
* **Concept:** In production data systems, bad data (null IDs, negative amounts, altered column names) will silently corrupt ML models.
* **Data Quality Gate:** An automated checkpoint before data enters downstream storage. If data fails validation, the pipeline stops immediately (**fail-fast**).
* In this pipeline, [`validate_gold.py`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/services/spark/validate_gold.py) asserts:
  - Row count is non-zero.
  - Required columns exist and have zero nulls.
  - Transaction amounts are $\ge 0$.
  - Transaction types match valid banking operations (`TRANSFER`, `CASH_OUT`, etc.).

---

### 1.11 Continuous MLOps & Experiment Tracking (MLflow)
* **MLflow Tracking Server:** Centralized service for logging machine learning runs.
* **Parameters:** Hyperparameters chosen before training (e.g., `num_trees=20`, `train_split=0.8`).
* **Metrics:** Quantitative evaluation results (e.g., `ROC-AUC`, `PR-AUC`, `accuracy`, `precision`, `recall`).
* **Artifacts:** Heavy binary outputs generated during training (the serialized Spark MLlib model pipeline, confusion matrix JSON reports, feature importance charts).
* **Model Registry:** Catalog managing model versions through staging, production, and archiving stages.

---

### 1.12 Machine Learning for Imbalanced Fraud Classification
* **The Class Imbalance Challenge:** In banking, genuine transactions represent 99.8% or more of total volume; fraud represents < 0.2%.
* **Why Accuracy is Deceptive:** A dummy model that predicts "NOT FRAUD" 100% of the time achieves **99.8% accuracy**, yet catches zero fraud.
* **Key Evaluation Metrics:**
  1. **Confusion Matrix:**
     - **True Positive (TP):** Fraud correctly caught.
     - **False Positive (FP):** Innocent customer blocked (causes friction).
     - **False Negative (FN):** Fraud missed (causes financial loss).
     - **True Negative (TN):** Normal transaction approved.
  2. **Precision:** $\frac{TP}{TP + FP}$ — When the model flags fraud, how often is it right?
  3. **Recall (Sensitivity):** $\frac{TP}{TP + FN}$ — What percentage of total fraud did the model catch?
  4. **ROC-AUC (Receiver Operating Characteristic - Area Under Curve):** Measures discrimination ability across all classification thresholds ($0.5$ = random guess, $1.0$ = perfect).
  5. **PR-AUC (Precision-Recall Area Under Curve):** The gold standard for heavily imbalanced fraud datasets because it ignores True Negatives and evaluates the exact trade-off between Precision and Recall.

---

### 1.13 Full-Stack Observability (Prometheus, Grafana & Exporters)
* **Prometheus:** A time-series database (TSDB) using a **pull model**. It scrapes metrics over HTTP at fixed intervals (e.g., every 10s) from designated endpoints (`/metrics`).
* **Metric Types:**
  - **Counter:** Monotonically increasing number (e.g., total transactions committed).
  - **Gauge:** Number that goes up and down (e.g., current Kafka consumer lag, memory usage, row count).
* **Exporters:** Light HTTP sidecars that convert native application state into Prometheus plaintext format:
  - `postgres-exporter` (:9187) $\rightarrow$ DB locks, connection counts, commits/sec.
  - `kafka-exporter` (:9308) $\rightarrow$ partition consumer lag, broker throughput.
  - `pipeline-telemetry` (:8008) $\rightarrow$ domain metrics (Lakehouse Bronze/Silver/Gold rows, Debezium health, MLflow AUC).
* **Grafana:** Observability visualization platform. Queries Prometheus via PromQL to render real-time graphs, gauges, and alerts.

---

### 1.14 High-Speed Parquet Footer Metadata Inspection
* **Challenge:** How to report real-time row counts of 550+ Parquet files in S3 to Prometheus every 10 seconds without running slow, expensive Spark queries?
* **Solution ([`pipeline_exporter.py`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/services/monitoring/pipeline_exporter.py)):**
  - Uses `pyarrow.fs.S3FileSystem` and `pyarrow.parquet.read_metadata()`.
  - Downloads **only the file footer dictionary (last few hundred bytes)** to read `num_rows`.
  - Maintains an in-memory cache keyed by `(file_path, file_size)`. Since historical Parquet files are immutable, their size never changes.
  - Scrapes 550+ files in **~8.7 milliseconds** with minimal CPU or bandwidth impact.

---

# 2. Exhaustive Configuration Reference

---

### 2.1 Environment Variables (`.env`)

| Variable | Value in Project | Detailed Purpose & Engineering Context |
| :--- | :--- | :--- |
| `POSTGRES_USER` | `postgres` | Superuser username for PostgreSQL database container. |
| `POSTGRES_PASSWORD` | `postgres` | Password for PostgreSQL authentication. |
| `POSTGRES_DB` | `fraud_db` | Name of the relational database holding the `transactions` table. |
| `MINIO_ROOT_USER` | `minioadmin` | S3 Access Key ID for MinIO Object Storage administration. |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | S3 Secret Access Key for MinIO. Used by Spark, MLflow, and Exporters. |
| `KAGGLE_USERNAME` | `marouanebouziane` | API credential to download the raw PaySim financial logs from Kaggle. |
| `KAGGLE_KEY` | `KGAT_...` | API secret token for Kaggle dataset authentication. |
| `BATCH_SIZE` | `50` | Number of transactions inserted per batch by the streaming generator into PostgreSQL. |
| `ROWS_PER_SECOND` | `20` | Rate limiter in the stream generator to simulate real-time banking TPS (Transactions Per Second). |
| `RETRAINING_MIN_NEW_ROWS` | `10000` | Gating threshold: Airflow will skip ML retraining until at least 10,000 new Gold rows arrive. |
| `AIRFLOW_USERNAME` | `admin` | Web UI administrator username for Apache Airflow (:8082). |
| `AIRFLOW_PASSWORD` | `admin` | Web UI administrator password for Apache Airflow. |

---

### 2.2 Docker Compose Services & Infrastructure

```yaml
version: "3.8"
```
The pipeline coordinates **15 microservices** across a shared virtual network:

| Service Name | Image / Build | Port Mappings | Primary Role |
| :--- | :--- | :--- | :--- |
| `postgres` | `postgres:14-alpine` | `5432:5432` | Core transactional banking database. Source of CDC stream. |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | `2181:2181` | Cluster coordinator and metadata store for Apache Kafka. |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | `9092:9092`, `29092` | Distributed streaming event log broker. |
| `schema-registry` | `confluentinc/cp-schema-registry:7.5.0` | `8081:8081` | Central schema repository for Kafka message validation. |
| `debezium` | `debezium/connect:2.4` | `8083:8083` | Kafka Connect runtime tailing PostgreSQL WAL. |
| `minio` | `minio/minio:RELEASE.2023-11-20...` | `9000:9000`, `9001:9001` | S3-compatible Object Storage for Lakehouse Bronze/Silver/Gold/MLflow. |
| `lakehouse-ingestion` | `./services/spark` (Spark 3.5.0) | None (Internal) | Long-running daemon executing `streaming_job.py` and batch ETL. |
| `airflow` | `./services/airflow` (Airflow 2.8.1) | `8082:8080` | Workflow orchestrator running DAGs every 2 minutes. |
| `mlflow` | `./services/mlflow` (Python 3.10) | `5000:5000` | ML experiment tracking server and artifact repository. |
| `dashboard` | `./services/dashboard` (Streamlit) | `8501:8501` | Real-time frontend for interactive fraud scoring. |
| `prometheus` | `prom/prometheus:v2.48.0` | `9090:9090` | Time-series metrics collection and alert evaluation engine. |
| `grafana` | `grafana/grafana:10.2.0` | `3001:3000` | Operational monitoring UI and alert dashboards. |
| `postgres-exporter` | `prometheuscommunity/postgres-exporter`| `9187:9187` | Scrapes database performance statistics for Prometheus. |
| `kafka-exporter` | `danielqsj/kafka-exporter:latest` | `9308:9308` | Scrapes Kafka topic offsets, lag, and consumer group status. |
| `pipeline-telemetry` | `./services/monitoring` (Python 3.10) | `8008:8008` | Custom exporter scraping Lakehouse row counts and pipeline status. |

---

### 2.3 PostgreSQL Engine & Replication Parameters

In [`docker-compose.yml`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/docker-compose.yml#L10-L22):
```yaml
command:
  - "postgres"
  - "-c"
  - "wal_level=logical"
  - "-c"
  - "max_wal_senders=10"
  - "-c"
  - "max_replication_slots=10"
```
* `wal_level=logical`: Crucial parameter. Configures PostgreSQL to write raw logical data changes (row values before and after change) to the WAL, rather than just physical byte blocks. Required for Debezium.
* `max_wal_senders=10`: Defines maximum concurrent streaming connections from replication clients (Debezium, backup tools).
* `max_replication_slots=10`: Maximum persistent replication slots. A replication slot guarantees that PostgreSQL will **never delete WAL segments** until Debezium confirms it has read them, preventing message loss during network partitions.

---

### 2.4 Kafka & Zookeeper Networking & Listeners

In [`docker-compose.yml`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/docker-compose.yml#L45-L80):
```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
* **Dual Listener Pattern:**
  - `PLAINTEXT://kafka:29092`: **Internal Docker listener**. Used by Debezium, Spark, and Kafka Exporter communicating over the internal Docker network.
  - `PLAINTEXT_HOST://localhost:9092`: **External Host listener**. Allows developers to connect client tools (e.g., Python scripts, Kafka GUI tools) directly from the Windows host.
* `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`: Internal offsets topic replica count. Set to 1 because this is a single-broker development cluster (production requires $\ge 3$).

---

### 2.5 Debezium Connector Configuration

Registered via REST API to `http://debezium:8083/connectors`:
```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "fraud_db",
    "database.server.name": "cdc",
    "table.include.list": "public.transactions",
    "tombstones.on.delete": "false"
  }
}
```
* `connector.class`: Uses Debezium's native PostgreSQL engine.
* `plugin.name`: `pgoutput` is the native PostgreSQL logical decoding plugin (available standard in Postgres 10+), avoiding external plugin compilations.
* `database.server.name`: Logical namespace prefix. Debezium routes records to a Kafka topic named: `<database.server.name>.<schema>.<table>` $\rightarrow$ `cdc.public.transactions`.
* `table.include.list`: Whitelists only the `transactions` table, ignoring internal tables.
* `tombstones.on.delete: false`: Avoids producing null-payload tombstone records in Kafka when a row is deleted.

---

### 2.6 Apache Spark & Hadoop S3A Parameters

Configured in Spark sessions across `streaming_job.py`, `transform_silver.py`, `transform_gold.py`, `train_model.py`:
```python
spark = SparkSession.builder \
    .appName("Lakehouse-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```
* `fs.s3a.endpoint`: Redirects Hadoop S3A client requests from standard AWS endpoints (`s3.amazonaws.com`) to local MinIO (`http://minio:9000`).
* `fs.s3a.path.style.access`: Set to `true`. Standard AWS S3 uses virtual-host styling (`http://bucket.s3.amazonaws.com/key`), whereas MinIO uses path styling (`http://minio:9000/bucket/key`).
* `fs.s3a.impl`: Binds the `s3a://` URI protocol to the Hadoop Java class `org.apache.hadoop.fs.s3a.S3AFileSystem`.

---

### 2.7 Apache Airflow DAG & Task Configuration

In [`services/airflow/dags/silver_pipeline_dag.py`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/services/airflow/dags/silver_pipeline_dag.py):
```python
schedule_interval="*/2 * * * *"
max_active_runs=1
catchup=False
```
* `schedule_interval="*/2 * * * *"`: Standard cron expression. Triggers the orchestration pipeline every 2 minutes.
* `max_active_runs=1`: Critical concurrency lock. Prevents Airflow from triggering a new DAG run if the previous run is still executing Spark transformations, eliminating race conditions on MinIO checkpoints.
* `catchup=False`: Prevents backfilling historic missed runs upon Airflow restart.
* `trigger_rule="none_failed_min_one_success"` on `training_gate_complete`: Allows downstream joining of a branched execution where one branch (`skip_model_training` or `trigger_model_training`) was deliberately skipped.

---

### 2.8 MLflow Server Parameters

In [`docker-compose.yml#L222-L227`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/docker-compose.yml#L222-L227):
```bash
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root s3://lakehouse/mlflow
```
* `--host 0.0.0.0`: Binds server to all interfaces inside container.
* `--backend-store-uri sqlite:///mlflow.db`: Lightweight relational store for run metadata, experiment names, tags, parameters, and metric time series.
* `--default-artifact-root s3://lakehouse/mlflow`: Designates the S3 bucket where heavy model binaries (`MLmodel`, pickle/conda environments, Spark stages) are saved.

---

### 2.9 Prometheus Scrape Targets & Alert Rules

In [`infra/prometheus/prometheus.yml`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/infra/prometheus/prometheus.yml):
```yaml
global:
  scrape_interval: 10s
  evaluation_interval: 10s
```
* Scrapes all exporters and evaluates alert rules every 10 seconds.
* Active Scrape Jobs:
  - `postgres` $\rightarrow$ `postgres-exporter:9187`
  - `kafka` $\rightarrow$ `kafka-exporter:9308`
  - `pipeline-telemetry` $\rightarrow$ `pipeline-telemetry:8008`
  - `minio` $\rightarrow$ `minio:9000/minio/v2/metrics/cluster`
  - `prometheus` $\rightarrow$ `localhost:9090`

In [`infra/prometheus/alert_rules.yml`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/infra/prometheus/alert_rules.yml):
* **`KafkaConsumerLagHigh`**:
  `expr: sum(kafka_consumergroup_lag{topic="cdc.public.transactions"}) > 1000 for: 2m`
  Triggers a `warning` if consumer lag exceeds 1,000 unconsumed transactions for more than 2 minutes (indicates Spark ingestion is stalled).
* **`DebeziumConnectorDown`**:
  `expr: fraud_debezium_connector_up == 0 for: 1m`
  Triggers a `critical` alert if PostgreSQL CDC stream drops out of `RUNNING` state.
* **`ServiceDown`**:
  `expr: up == 0 for: 1m`
  Fires if any exporter target is unreachable for 60 seconds.

---

### 2.10 Grafana Provisioning & Auth Parameters

In [`docker-compose.yml#L324-L337`](file:///c:/Users/Marouane/Documents/fraud-detection-pipeline/docker-compose.yml#L324-L337):
```yaml
environment:
  GF_SECURITY_ADMIN_USER: admin
  GF_SECURITY_ADMIN_PASSWORD: admin
  GF_USERS_ALLOW_SIGN_UP: "false"
  GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /etc/grafana/dashboards/fraud_pipeline_overview.json
  GF_AUTH_ANONYMOUS_ENABLED: "true"
  GF_AUTH_ANONYMOUS_ORG_ROLE: "Editor"
```
* `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH`: Overrides default Grafana welcome screen, immediately displaying the custom `Fraud Detection Pipeline - Observability & Telemetry` dashboard upon visiting [http://localhost:3001](http://localhost:3001).
* `GF_AUTH_ANONYMOUS_ENABLED: "true"`: Permits instant access without forcing credentials in development environments.
* `provisioning/datasources/datasources.yml`: Declaratively provisions Prometheus connection as the default data source (`http://prometheus:9090`).
* `provisioning/dashboards/dashboards.yml`: Automatically registers all dashboards in `infra/grafana/dashboards/` on startup with hot-reloading.

---

# 3. Engineering Glossary & Acronyms

| Term / Acronym | Definition |
| :--- | :--- |
| **CDC** | **Change Data Capture**: Stream-based ingestion capturing row-level database changes. |
| **WAL** | **Write-Ahead Log**: PostgreSQL append-only commit log on disk before table storage. |
| **Medallion** | Tiered lakehouse data architecture (**Bronze** $\rightarrow$ **Silver** $\rightarrow$ **Gold**). |
| **OLTP** | **Online Transaction Processing**: Relational systems optimized for fast row writes (PostgreSQL). |
| **OLAP** | **Online Analytical Processing**: Columnar systems optimized for large aggregations (Parquet/Spark). |
| **S3A** | Hadoop's open-source connector for reading and writing to AWS S3-compatible APIs. |
| **DAG** | **Directed Acyclic Graph**: Airflow workflow definition with non-circular dependencies. |
| **XCom** | **Cross-Communication**: Airflow mechanism for tasks to share small metadata values (JSON). |
| **Micro-batch**| Spark Structured Streaming design that groups stream events into small discrete batches. |
| **ROC-AUC** | **Receiver Operating Characteristic - Area Under Curve**: Measure of model discrimination power. |
| **PR-AUC** | **Precision-Recall Area Under Curve**: Evaluation metric optimized for heavily imbalanced classes. |
| **TSDB** | **Time-Series Database**: Database optimized for timestamped metric streams (Prometheus). |
| **PromQL** | **Prometheus Query Language**: Query engine for time-series arithmetic, aggregations, and rates. |
| **Compaction** | Batch process merging hundreds of small storage files into large block-optimized files. |
| **Watermark** | Streaming threshold specifying how long to wait for late-arriving out-of-order data. |
