#!/usr/bin/env python3
"""
Fraud Detection Pipeline - Comprehensive Health Check CLI
Probes all pipeline, lakehouse, and monitoring components and reports their real-time operational status.
Usage:
    python infra/monitoring/health_check.py
"""

import os
import socket
import sys
import time
from typing import Dict, Tuple

try:
    import urllib.request
    import json
except ImportError:
    pass

# Host configurations (defaults for localhost execution)
SERVICES = [
    {"name": "PostgreSQL (Transactions DB)", "host": "localhost", "port": 5432, "type": "tcp"},
    {"name": "Apache Zookeeper", "host": "localhost", "port": 2181, "type": "tcp"},
    {"name": "Apache Kafka (Broker)", "host": "localhost", "port": 9092, "type": "tcp"},
    {"name": "Confluent Schema Registry", "url": "http://localhost:8081/subjects", "type": "http"},
    {"name": "Debezium CDC Connector", "url": "http://localhost:8083/connectors/postgres-connector/status", "type": "debezium"},
    {"name": "MinIO Object Storage (API)", "url": "http://localhost:9000/minio/health/ready", "type": "http"},
    {"name": "MinIO Console", "host": "localhost", "port": 9001, "type": "tcp"},
    {"name": "Apache Airflow (Orchestration)", "url": "http://localhost:8082/health", "type": "http"},
    {"name": "MLflow Tracking Server", "url": "http://localhost:5000/health", "type": "http"},
    {"name": "Streamlit Prediction Dashboard", "url": "http://localhost:8501/_stcore/health", "type": "http"},
    {"name": "Prometheus Metrics Server", "url": "http://localhost:9090/-/healthy", "type": "http"},
    {"name": "Grafana Observability UI", "url": "http://localhost:3001/api/health", "type": "http"},
    {"name": "PostgreSQL Exporter", "url": "http://localhost:9187/metrics", "type": "http"},
    {"name": "Kafka Exporter", "url": "http://localhost:9308/metrics", "type": "http"},
    {"name": "Pipeline Telemetry Exporter", "url": "http://localhost:8008/", "type": "http"},
]


def check_tcp(host: str, port: int, timeout: float = 2.0) -> Tuple[bool, float, str]:
    start = time.time()
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000.0
        return True, latency, "Connected"
    except Exception as exc:
        latency = (time.time() - start) * 1000.0
        return False, latency, str(exc)


def check_http(url: str, timeout: float = 2.5) -> Tuple[bool, float, str]:
    start = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "FraudPipelineHealthCheck/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = (time.time() - start) * 1000.0
            if 200 <= resp.status < 400:
                return True, latency, f"HTTP {resp.status}"
            return False, latency, f"HTTP {resp.status}"
    except Exception as exc:
        latency = (time.time() - start) * 1000.0
        return False, latency, str(exc)


def check_debezium(url: str, timeout: float = 2.5) -> Tuple[bool, float, str]:
    start = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "FraudPipelineHealthCheck/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = (time.time() - start) * 1000.0
            data = json.loads(resp.read().decode())
            state = data.get("connector", {}).get("state", "UNKNOWN")
            tasks = data.get("tasks", [])
            tasks_ok = all(t.get("state") == "RUNNING" for t in tasks) if tasks else False
            if state == "RUNNING" and tasks_ok:
                return True, latency, "RUNNING (Connector & Tasks OK)"
            return False, latency, f"State: {state}, Tasks OK: {tasks_ok}"
    except Exception as exc:
        latency = (time.time() - start) * 1000.0
        return False, latency, str(exc)


def run_checks() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print("\n" + "=" * 80)
    print(" [*] FRAUD DETECTION PIPELINE - SYSTEM HEALTH DIAGNOSTIC")
    print("=" * 80)
    print(f"{'SERVICE':<34} | {'STATUS':<12} | {'LATENCY':<10} | {'DETAILS'}")
    print("-" * 80)

    healthy_count = 0
    total_count = len(SERVICES)

    for svc in SERVICES:
        name = svc["name"]
        stype = svc["type"]

        if stype == "tcp":
            ok, lat, msg = check_tcp(svc["host"], svc["port"])
        elif stype == "http":
            ok, lat, msg = check_http(svc["url"])
        elif stype == "debezium":
            ok, lat, msg = check_debezium(svc["url"])
        else:
            ok, lat, msg = False, 0.0, "Unknown test type"

        if ok:
            healthy_count += 1
            status_str = "\033[92m[HEALTHY]\033[0m"
        else:
            status_str = "\033[91m[UNHEALTHY]\033[0m"

        print(f"{name:<34} | {status_str:<21} | {lat:>6.1f} ms  | {msg[:28]}")

    print("-" * 80)
    summary_color = "\033[92m" if healthy_count == total_count else "\033[93m"
    print(f" Summary: {summary_color}{healthy_count}/{total_count} Services Healthy\033[0m")

    # Lakehouse and Pipeline Metrics Summary
    try:
        req = urllib.request.Request("http://localhost:8008/", headers={"User-Agent": "FraudPipelineHealthCheck/1.0"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            content = resp.read().decode()
            metrics = {}
            for line in content.splitlines():
                if line.startswith("fraud_lakehouse_records_count"):
                    # e.g. fraud_lakehouse_records_count{layer="bronze"} 24050.0
                    if 'layer="bronze"' in line:
                        metrics["Bronze Rows"] = int(float(line.split()[-1]))
                    elif 'layer="silver"' in line:
                        metrics["Silver Rows"] = int(float(line.split()[-1]))
                    elif 'layer="gold"' in line:
                        metrics["Gold Rows"] = int(float(line.split()[-1]))
                elif line.startswith("fraud_postgres_transactions_total"):
                    metrics["PostgreSQL Rows"] = int(float(line.split()[-1]))
                elif line.startswith("fraud_mlflow_latest_model_auc"):
                    metrics["MLflow Model AUC"] = f"{float(line.split()[-1]):.4f}"

            if metrics:
                print("-" * 80)
                print(" [*] LIVE PIPELINE DATA & LAKEHOUSE METRICS:")
                for k, v in metrics.items():
                    print(f"     - {k:<20}: {v}")
    except Exception:
        pass

    print("=" * 80 + "\n")


if __name__ == "__main__":
    run_checks()
