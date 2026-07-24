#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Chapter 7: Setup ==="

echo "[1/3] Starting Docker services (Flink, Kafka, MySQL)..."
cd "$CH_DIR"
docker compose up -d --build

echo "[2/3] Waiting for services to be ready..."
sleep 5

until docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
  echo "  Waiting for Kafka..."
  sleep 3
done
echo "  Kafka is ready."

until docker compose exec -T mysql mysqladmin ping -h localhost -u root -proot --silent &>/dev/null; do
  echo "  Waiting for MySQL..."
  sleep 3
done
echo "  MySQL is ready (seed data loaded via docker-entrypoint-initdb.d)."

until curl -sf http://localhost:8081/overview &>/dev/null; do
  echo "  Waiting for Flink JobManager..."
  sleep 3
done
echo "  Flink JobManager is ready."

echo "[3/3] Creating Kafka topic and producing sample events..."
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic payments.transactions --partitions 4 --replication-factor 1 \
  --if-not-exists

"$SCRIPT_DIR/produce_events.sh"

echo ""
echo "=== Setup complete ==="
echo "  Flink UI:  http://localhost:8081"
echo "  Kafka:     localhost:9092"
echo "  MySQL:     localhost:3306 (user: cdc_reader / cdc_pass, db: novapay)"
echo ""
echo "Run SQL files with:  ./scripts/run_section.sh sql/01_create_kafka_source.sql"
echo "Run everything:      ./scripts/run_all.sh"
