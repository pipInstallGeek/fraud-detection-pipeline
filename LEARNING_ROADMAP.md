 # 🚀 FRAUD DETECTION PIPELINE - Complete Technical Learning Roadmap

> **A comprehensive guide to all technologies, concepts, and patterns used in this real-time fraud detection lakehouse project.**

---

## 📋 Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Infrastructure & Containerization](#2-infrastructure--containerization)
3. [Data Storage Layer](#3-data-storage-layer)
4. [Event Streaming (Kafka Ecosystem)](#4-event-streaming-kafka-ecosystem)
5. [Change Data Capture (CDC)](#5-change-data-capture-cdc)
6. [Lakehouse Architecture (Bronze/Silver/Gold)](#6-lakehouse-architecture-bronzesilvergold)
7. [Apache Spark](#7-apache-spark)
8. [Workflow Orchestration (Airflow)](#8-workflow-orchestration-airflow)
9. [Python CLI Development](#9-python-cli-development)
10. [Data Engineering Patterns](#10-data-engineering-patterns)
11. [DevOps & Operations](#11-devops--operations)
12. [Learning Path & Resources](#12-learning-path--resources)

---

## 1. Architecture Overview

### 🏗️ System Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FRAUD DETECTION LAKEHOUSE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐       │
│  │  Kaggle  │────▶│PostgreSQL│────▶│ Debezium │────▶│  Kafka   │       │
│  │ Dataset  │     │ (Source) │     │  (CDC)   │     │(Streaming│       │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘       │
│       │                                                   │             │
│       │                                                   ▼             │
│       │                                            ┌──────────┐         │
│       │                                            │  Spark   │         │
│       │                                            │Streaming │         │
│       │                                            └──────────┘         │
│       │                                                   │             │
│       ▼                                                   ▼             │
│  ┌──────────┐                                      ┌──────────┐         │
│  │   CLI    │                                      │  MinIO   │         │
│  │ (Typer)  │                                      │Lakehouse │         │
│  └──────────┘                                      └──────────┘         │
│                                                     │ Bronze  │         │
│  ┌──────────┐                                       │ Silver  │         │
│  │ Airflow  │─────────────────────────────────────▶│ Gold    │         │
│  │  (DAGs)  │                                      └──────────┘         │
│  └──────────┘                                                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Data Flow

1. **Source**: PaySim fraud dataset (Kaggle) → Downloaded via CLI
2. **Ingest**: Stream transactions to PostgreSQL (OLTP database)
3. **Capture**: Debezium captures changes via WAL (Write-Ahead Log)
4. **Stream**: Changes published to Kafka topics
5. **Process**: Spark Structured Streaming consumes and writes to Bronze
6. **Transform**: Batch Spark job transforms Bronze → Silver
7. **Orchestrate**: Airflow DAGs coordinate the pipeline

---

## 2. Infrastructure & Containerization

### 🐳 Docker & Docker Compose

**Files**: `docker-compose.yml`, `*/Dockerfile`

#### Key Concepts to Learn

| Concept | Description | Used In Project |
|---------|-------------|-----------------|
| **Multi-container Apps** | Defining multiple services in one file | 10+ services defined |
| **Service Dependencies** | `depends_on` with conditions | `airflow` waits for `airflow-init` |
| **Volume Mounts** | Persist data, share files | `./dags:/opt/airflow/dags` |
| **Named Volumes** | Managed storage | `postgres_data`, `kafka_data` |
| **Environment Variables** | `.env` file injection | `${POSTGRES_USER}` |
| **Port Mapping** | Host:Container ports | `9000:9000` (MinIO) |
| **Custom Images** | Build from Dockerfile | `build: ./spark` |
| **Restart Policies** | Container recovery | `restart: unless-stopped` |
| **User/Group Permissions** | File ownership | `user: "50000:0"` for Airflow |
| **Docker Socket Mounting** | Container-in-container ops | `/var/run/docker.sock` |

#### Services in This Project

```yaml
services:
  # Data Storage
  - postgres:15          # OLTP source database
  - minio                # S3-compatible object storage

  # Event Streaming
  - zookeeper            # Kafka coordination
  - kafka                # Message broker
  - schema-registry      # Schema management
  - kafka-ui             # Web UI for Kafka

  # CDC
  - debezium             # Change Data Capture

  # Processing
  - lakehouse-ingestion  # Spark Streaming job
  - airflow              # Workflow orchestration

  # Utilities
  - cli                  # Management commands
  - stream-service       # Continuous data feed
```

#### 📚 Resources

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Docker Best Practices](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)

---

## 3. Data Storage Layer

### 🐘 PostgreSQL

**Files**: `postgres/init/01-schema.sql`, `02-airflow.sql`

#### Key Concepts

| Concept | Description | Example |
|---------|-------------|---------|
| **DDL (Schema)** | CREATE TABLE, indexes | `CREATE TABLE transactions (...)` |
| **WAL (Write-Ahead Log)** | Transaction logging | `wal_level=logical` for CDC |
| **Logical Replication** | Streaming changes | `REPLICA IDENTITY FULL` |
| **Indexes** | Query optimization | `CREATE INDEX idx_transactions_is_fraud` |
| **Publications** | Debezium subscription | For CDC connector |

#### Schema Design

```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,              -- Auto-increment PK
    step INTEGER NOT NULL,              -- Time step
    type VARCHAR(20) NOT NULL,          -- PAYMENT, TRANSFER, etc.
    amount DECIMAL(15,2) NOT NULL,      -- Transaction amount
    name_orig VARCHAR(50) NOT NULL,     -- Origin account
    old_balance_orig DECIMAL(15,2),     -- Balance before
    new_balance_orig DECIMAL(15,2),     -- Balance after
    name_dest VARCHAR(50) NOT NULL,     -- Destination account
    old_balance_dest DECIMAL(15,2),
    new_balance_dest DECIMAL(15,2),
    is_fraud INTEGER DEFAULT 0,         -- Label: 0/1
    is_flagged_fraud INTEGER DEFAULT 0, -- Flagged by system
    created_at TIMESTAMP DEFAULT NOW()  -- Ingestion time
);

-- Required for Debezium CDC
ALTER TABLE transactions REPLICA IDENTITY FULL;
```

#### 📚 Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

---

### 🪣 MinIO (S3-Compatible Object Storage)

**Purpose**: Lakehouse storage for Bronze/Silver/Gold layers

#### Key Concepts

| Concept | Description |
|---------|-------------|
| **Bucket** | Top-level container (like a folder) |
| **Object** | Any file stored (Parquet, JSON, etc.) |
| **S3 API** | AWS S3-compatible interface |
| **mc CLI** | MinIO client for management |
| **Path Style Access** | `s3a://bucket/path` format |

#### Bucket Structure

```
lakehouse/
├── bronze/
│   └── transactions/           # Raw CDC events (Parquet)
│       ├── part-00000-xxx.parquet
│       └── ...
├── silver/
│   └── transactions/           # Cleaned, transformed data
│       ├── part-00000-xxx.parquet
│       └── ...
├── gold/                       # (Future) Aggregated analytics
└── checkpoints/
    ├── bronze/                 # Spark Streaming checkpoint
    └── silver_watermark.json   # Batch processing checkpoint
```

#### Common mc Commands

```bash
# Set alias
mc alias set local http://minio:9000 minioadmin minioadmin

# Create bucket
mc mb local/lakehouse

# List contents
mc ls local/lakehouse/bronze/transactions/ --recursive

# Remove files
mc rm -r --force local/lakehouse/silver/
```

---

## 4. Event Streaming (Kafka Ecosystem)

### 📬 Apache Kafka

**Files**: `docker-compose.yml` (kafka, zookeeper services)

#### Core Concepts

| Concept | Description |
|---------|-------------|
| **Broker** | Kafka server that stores messages |
| **Topic** | Named channel for messages |
| **Partition** | Parallelism unit within topic |
| **Offset** | Message position in partition |
| **Consumer Group** | Coordinated consumers sharing load |
| **Producer** | Sends messages to topics |
| **Consumer** | Reads messages from topics |

#### Topics in This Project

```
cdc.public.transactions    # Debezium CDC events
connect_configs           # Kafka Connect configs
connect_offsets           # Kafka Connect offsets
connect_statuses          # Kafka Connect status
```

#### Message Format (Debezium CDC)

```json
{
  "before": null,                    // Previous state (null for INSERT)
  "after": {                         // Current state
    "id": 1,
    "step": 1,
    "type": "PAYMENT",
    "amount": "1000.50",
    "name_orig": "C123456",
    "old_balance_orig": "5000.00",
    "new_balance_orig": "4000.00",
    "name_dest": "M789012",
    "old_balance_dest": "0.00",
    "new_balance_dest": "1000.50",
    "is_fraud": 0,
    "is_flagged_fraud": 0
  },
  "source": { ... },                 // CDC metadata
  "op": "c",                         // Operation: c=create, u=update, d=delete
  "ts_ms": 1706000000000             // Timestamp
}
```

#### Kafka Configuration

```yaml
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

#### 📚 Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka: The Definitive Guide (Book)](https://www.confluent.io/resources/kafka-the-definitive-guide/)

---

### 📋 Schema Registry

**Purpose**: Centralized schema management for Kafka messages

#### Key Concepts

| Concept | Description |
|---------|-------------|
| **Subject** | Name for a schema (topic-value, topic-key) |
| **Schema Evolution** | Backward/forward compatibility |
| **Avro/JSON Schema** | Supported formats |
| **Schema ID** | Unique identifier for each version |

---

## 5. Change Data Capture (CDC)

### 🔄 Debezium

**Files**: `docker-compose.yml` (debezium service), `cli/fraud_cli/commands/infrastructure.py`

#### What is CDC?

Change Data Capture captures database changes (INSERT, UPDATE, DELETE) and streams them as events.

#### Why Debezium?

- **Log-based**: Reads PostgreSQL WAL (no triggers needed)
- **Low latency**: Near real-time change capture
- **Reliable**: Exactly-once semantics
- **Rich metadata**: Before/after states, timestamps, operation type

#### Connector Configuration

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "fraud_db",
    
    "topic.prefix": "cdc",                    // Topic: cdc.public.transactions
    "table.include.list": "public.transactions",
    
    "plugin.name": "pgoutput",                // Native PostgreSQL plugin
    "slot.name": "debezium_slot",             // Replication slot
    "publication.name": "dbz_publication",    // PostgreSQL publication
    
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",  // No schema wrapper
    "value.converter.schemas.enable": "false"
  }
}
```

#### Debezium REST API

```bash
# List connectors
curl http://debezium:8083/connectors

# Get connector status
curl http://debezium:8083/connectors/postgres-connector/status

# Delete connector
curl -X DELETE http://debezium:8083/connectors/postgres-connector

# Create connector
curl -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{ ... config ... }'
```

#### 📚 Resources

- [Debezium Documentation](https://debezium.io/documentation/)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)

---

## 6. Lakehouse Architecture (Bronze/Silver/Gold)

### 🏠 Medallion Architecture

**Files**: `spark/streaming_job.py`, `spark/transform_silver.py`

#### Layers

| Layer | Purpose | Data Quality | Format |
|-------|---------|--------------|--------|
| **Bronze** | Raw ingestion | As-is from source | Parquet |
| **Silver** | Cleaned, validated | Deduplicated, typed | Parquet |
| **Gold** | Business aggregates | Analytics-ready | Parquet/Delta |

#### Bronze Layer (Raw)

```python
# streaming_job.py - Writes raw CDC events
bronze_df = df.selectExpr("CAST(value AS STRING) as json_msg") \
    .select(from_json(col("json_msg"), schema).alias("data")) \
    .select("data.after.*") \
    .filter(col("step").isNotNull())

bronze_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze") \
    .outputMode("append") \
    .start()
```

#### Silver Layer (Cleaned)

```python
# transform_silver.py - Batch transformation
new_silver_df = new_bronze_df \
    .withColumn("amount", decode_amount("amount")) \        # Decode encoded values
    .withColumn("is_fraud", col("is_fraud").cast(BooleanType())) \
    .withColumn("ingested_at", current_timestamp()) \
    .dropDuplicates(["id"])                                 # Deduplicate

# Idempotent write (handles retries)
new_silver_df = new_silver_df.join(existing_ids, "id", "left_anti")

new_silver_df.write \
    .mode("append") \
    .parquet(SILVER_PATH)
```

#### Gold Layer (Aggregates) - Future

```python
# Example: Daily fraud summary
gold_df = silver_df \
    .groupBy(date_trunc("day", "ingested_at").alias("date"), "type") \
    .agg(
        count("*").alias("total_transactions"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
        avg("amount").alias("avg_amount")
    )
```

---

## 7. Apache Spark

### ⚡ Spark Core Concepts

**Files**: `spark/Dockerfile`, `spark/streaming_job.py`, `spark/transform_silver.py`

#### Architecture

| Component | Description |
|-----------|-------------|
| **Driver** | Orchestrates job execution |
| **Executor** | Runs tasks on workers |
| **SparkSession** | Entry point for DataFrame API |
| **RDD** | Low-level resilient data structure |
| **DataFrame** | High-level structured API |
| **Catalyst** | Query optimizer |
| **Tungsten** | Memory management engine |

#### SparkSession Configuration

```python
spark = SparkSession.builder \
    .appName("Silver-Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### Spark Structured Streaming

```python
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "cdc.public.transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Write to Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

### Batch Processing

```python
# Read Parquet
bronze_df = spark.read.parquet("s3a://lakehouse/bronze/transactions")

# Transformations
silver_df = bronze_df \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .filter(col("amount") > 0) \
    .dropDuplicates(["id"])

# Write Parquet
silver_df.write \
    .mode("append") \
    .parquet("s3a://lakehouse/silver/transactions")
```

### Key Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **select** | Choose columns | `.select("id", "amount")` |
| **filter/where** | Row filtering | `.filter(col("is_fraud") == 1)` |
| **withColumn** | Add/modify column | `.withColumn("doubled", col("amount") * 2)` |
| **groupBy + agg** | Aggregations | `.groupBy("type").count()` |
| **join** | Combine DataFrames | `.join(other_df, "id", "left_anti")` |
| **dropDuplicates** | Remove duplicates | `.dropDuplicates(["id"])` |
| **union** | Combine rows | `df1.union(df2)` |

### Checkpointing (Idempotency)

```python
# File-based checkpoint for incremental processing
checkpoint_data = {
    "last_run": datetime.now().isoformat(),
    "processed_files": list(all_processed),
    "total_files_processed": len(all_processed)
}

# Atomic checkpoint update using Hadoop FileSystem API
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
```

#### 📚 Resources

- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [Learning Spark (Book)](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)

---

## 8. Workflow Orchestration (Airflow)

### 🌊 Apache Airflow

**Files**: `dags/master_pipeline_dag.py`, `dags/pipeline_operations_dag.py`

#### Core Concepts

| Concept | Description |
|---------|-------------|
| **DAG** | Directed Acyclic Graph of tasks |
| **Operator** | Single task unit (Bash, Python, etc.) |
| **Sensor** | Wait for condition |
| **Task** | Instance of an operator |
| **XCom** | Task-to-task communication |
| **Scheduler** | Triggers DAG runs |
| **Executor** | Runs tasks (Local, Celery, K8s) |

#### DAG Definition

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'master_pipeline_orchestration',
    default_args=default_args,
    description='Orchestrates fraud detection pipeline',
    schedule_interval=None,  # Manual trigger / '@daily' / cron
    catchup=False,
    tags=['master', 'pipeline'],
) as dag:
    
    # Task 1: Sensor (wait for condition)
    check_kafka = BashSensor(
        task_id='wait_for_kafka',
        bash_command='docker exec kafka kafka-broker-api-versions ...',
        timeout=120,
        poke_interval=10,
    )
    
    # Task 2: Operator (execute action)
    create_bucket = BashOperator(
        task_id='create_minio_bucket',
        bash_command='docker exec minio mc mb local/lakehouse',
    )
    
    # Task 3: Another operator
    run_spark = BashOperator(
        task_id='run_silver_transformation',
        bash_command='docker exec lakehouse-ingestion spark-submit ...',
    )
    
    # Dependencies
    check_kafka >> create_bucket >> run_spark
```

#### Task Dependencies

```python
# Sequential
task1 >> task2 >> task3

# Parallel then join
[task1, task2, task3] >> task4

# Fan-out
task1 >> [task2, task3, task4]
```

#### Common Operators

| Operator | Use Case |
|----------|----------|
| `BashOperator` | Run shell commands |
| `PythonOperator` | Execute Python functions |
| `DockerOperator` | Run Docker containers |
| `SparkSubmitOperator` | Submit Spark jobs |
| `HttpSensor` | Wait for HTTP endpoint |
| `FileSensor` | Wait for file existence |
| `ExternalTaskSensor` | Wait for other DAG |

#### 📚 Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow)

---

## 9. Python CLI Development

### 🖥️ Typer CLI Framework

**Files**: `cli/fraud_cli/main.py`, `cli/fraud_cli/commands/*.py`

#### Project Structure

```
cli/
├── Dockerfile
├── pyproject.toml           # Poetry dependencies
└── fraud_cli/
    ├── __init__.py
    ├── main.py              # Entry point
    ├── constants.py         # Environment config
    ├── utils.py             # Shared utilities
    └── commands/
        ├── __init__.py
        ├── infrastructure.py  # status, register
        ├── ingestion.py       # download, stream
        └── monitoring.py      # health checks
```

#### Typer Basics

```python
import typer
from typing import Optional

app = typer.Typer(
    name="fraud-cli",
    help="Fraud Detection Pipeline CLI",
    add_completion=False,
)

@app.command()
def download():
    """Download PaySim dataset from Kaggle"""
    # Implementation
    typer.echo("Downloading...")

@app.command()
def stream(
    batch_size: int = typer.Option(100, help="Rows per batch"),
    rate: float = typer.Option(10.0, help="Rows per second"),
):
    """Stream transactions to PostgreSQL"""
    typer.echo(f"Streaming with batch={batch_size}, rate={rate}")

if __name__ == "__main__":
    app()
```

#### Modular Commands

```python
# main.py
from fraud_cli.commands import infrastructure, ingestion

app = typer.Typer()

# Merge sub-apps
app.add_typer(infrastructure.app, name="infra", help="Infrastructure")
app.add_typer(ingestion.app, name="data", help="Data operations")

# Usage: fraud-cli infra status
#        fraud-cli data download
```

#### Rich Console Output

```python
from rich.console import Console
from rich.table import Table

console = Console()

console.print("[green]Success![/green]")
console.print("[red]Error:[/red] Connection failed")

table = Table(title="Pipeline Status")
table.add_column("Service")
table.add_column("Status")
table.add_row("PostgreSQL", "✓ Connected")
console.print(table)
```

#### 📚 Resources

- [Typer Documentation](https://typer.tiangolo.com/)
- [Click (Typer is built on)](https://click.palletsprojects.com/)
- [Rich Library](https://rich.readthedocs.io/)

---

## 10. Data Engineering Patterns

### 🎯 Key Patterns Used

#### 1. Idempotent Processing

```python
# Problem: Job might run twice on same data
# Solution: Check before insert

existing_ids = spark.read.parquet(SILVER_PATH).select("id")
new_data = new_data.join(existing_ids, "id", "left_anti")  # Exclude existing
```

#### 2. Incremental Processing

```python
# Problem: Processing all data every time is expensive
# Solution: Track what's processed

processed_files = load_checkpoint()
new_files = all_files - processed_files
process(new_files)
save_checkpoint(all_files)
```

#### 3. Exactly-Once Semantics

```python
# Spark Streaming with checkpoints ensures exactly-once
query = df.writeStream \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze") \
    .start()
```

#### 4. Schema Evolution

```python
# Define explicit schema to handle changes
schema = StructType([
    StructField("id", IntegerType()),
    StructField("amount", StringType()),  # Can handle null/missing
    # New fields can be added without breaking
])
```

#### 5. Data Quality Validation

```python
# validate_data.py - Consistency checks
bronze_unique = bronze_df.select("id").distinct().count()
silver_unique = silver_df.select("id").distinct().count()

# Check for duplicates
duplicates = silver_df.groupBy("id").count().filter(col("count") > 1)

# Coverage report
coverage = silver_unique / bronze_unique * 100
```

#### 6. Atomic Checkpoint Updates

```python
# Problem: Checkpoint update might fail mid-write
# Solution: Write to temp, then rename (atomic)

fs.delete(tmp_path, True)
spark.write(tmp_path)
fs.delete(final_path, True)
fs.rename(tmp_path, final_path)  # Atomic operation
```

---

## 11. DevOps & Operations

### 🔧 Key Operational Concepts

#### Environment Configuration

```bash
# .env file
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=fraud_db
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
BATCH_SIZE=100
ROWS_PER_SECOND=10
```

#### Makefile for Common Tasks

```makefile
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

rebuild:
	docker compose build --no-cache

status:
	docker exec fraud-cli fraud-cli status

silver:
	docker exec lakehouse-ingestion spark-submit /app/transform_silver.py
```

#### Monitoring Commands

```bash
# Kafka
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# PostgreSQL
docker exec postgres psql -U postgres -d fraud_db -c "SELECT COUNT(*) FROM transactions"

# MinIO
docker exec minio mc ls local/lakehouse/bronze/transactions/ --recursive | wc -l

# Spark
docker logs lakehouse-ingestion --tail 50

# Airflow
docker exec airflow airflow dags list
docker exec airflow airflow tasks list master_pipeline_orchestration
```

---

## 12. Learning Path & Resources

### 📖 Recommended Learning Order

#### Phase 1: Foundations (2-3 weeks)

1. **Docker & Docker Compose**
   - [ ] Complete Docker fundamentals
   - [ ] Practice multi-container apps
   - [ ] Understand networking, volumes

2. **PostgreSQL**
   - [ ] SQL basics (DDL, DML, DQL)
   - [ ] Indexing strategies
   - [ ] Logical replication concepts

3. **Python Basics**
   - [ ] Environment management (venv, Poetry)
   - [ ] Typing and dataclasses
   - [ ] CLI development (Typer/Click)

#### Phase 2: Streaming (3-4 weeks)

4. **Apache Kafka**
   - [ ] Core concepts (topics, partitions, consumers)
   - [ ] Producer/Consumer APIs
   - [ ] Kafka Connect basics

5. **Change Data Capture**
   - [ ] Debezium fundamentals
   - [ ] PostgreSQL connector
   - [ ] Event schemas

#### Phase 3: Big Data Processing (4-6 weeks)

6. **Apache Spark**
   - [ ] SparkSession, DataFrames
   - [ ] Transformations vs Actions
   - [ ] Structured Streaming
   - [ ] Reading/Writing Parquet
   - [ ] S3/MinIO integration

7. **Lakehouse Architecture**
   - [ ] Bronze/Silver/Gold layers
   - [ ] Data quality patterns
   - [ ] Incremental processing

#### Phase 4: Orchestration (2-3 weeks)

8. **Apache Airflow**
   - [ ] DAG design principles
   - [ ] Operators and Sensors
   - [ ] Task dependencies
   - [ ] Scheduling and monitoring

### 📚 Resource Links

| Topic | Resource |
|-------|----------|
| **Docker** | [Docker Docs](https://docs.docker.com/) |
| **Kafka** | [Confluent Developer](https://developer.confluent.io/) |
| **Spark** | [Spark Docs](https://spark.apache.org/docs/latest/) |
| **Debezium** | [Debezium Docs](https://debezium.io/documentation/) |
| **Airflow** | [Airflow Docs](https://airflow.apache.org/docs/) |
| **MinIO** | [MinIO Quickstart](https://min.io/docs/) |
| **PostgreSQL** | [PostgreSQL Tutorial](https://www.postgresqltutorial.com/) |

### 🎓 Certifications to Consider

1. **Databricks Certified Data Engineer Associate**
2. **Confluent Certified Developer for Apache Kafka**
3. **AWS Certified Data Engineer - Associate**
4. **Google Professional Data Engineer**

---

## 🏆 Project Achievements

By completing this project, you've implemented:

- ✅ **Real-time CDC pipeline** (PostgreSQL → Kafka → Spark → MinIO)
- ✅ **Lakehouse architecture** (Bronze/Silver layers)
- ✅ **Streaming data ingestion** (Spark Structured Streaming)
- ✅ **Batch transformations** (Spark batch jobs)
- ✅ **Workflow orchestration** (Airflow DAGs)
- ✅ **Data validation** (Consistency checks)
- ✅ **Idempotent processing** (Safe retries)
- ✅ **Containerized infrastructure** (Docker Compose)
- ✅ **CLI tooling** (Python Typer)

---

**Congratulations!** You now have hands-on experience with a production-grade data engineering stack! 🚀
