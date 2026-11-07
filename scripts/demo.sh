#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[demo] 1. Starting PostgreSQL 16, Apache Kafka (KRaft), and Prometheus..."
docker compose -f deploy/docker-compose.yml up -d
until docker exec ledger-postgres pg_isready -U ledger -d ledger >/dev/null 2>&1; do sleep 1; done
until docker exec ledger-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done

echo "[demo] 2. Executing verification & contention benchmark suite..."
./scripts/load-test.sh
./scripts/benchmark-contention.sh

echo "[demo] Complete! Infrastructure running cleanly on localhost:5432 (Postgres), localhost:9092 (Kafka), localhost:9090 (Prometheus)."
