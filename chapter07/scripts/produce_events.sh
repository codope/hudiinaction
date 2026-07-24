#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"
DATA_FILE="$CH_DIR/data/sample_transactions.jsonl"

echo "Producing sample transaction events to Kafka topic payments.transactions..."

while IFS= read -r line; do
  echo "$line"
done < "$DATA_FILE" | docker compose -f "$CH_DIR/docker-compose.yml" exec -T kafka \
  kafka-console-producer --bootstrap-server localhost:9092 --topic payments.transactions

echo "Done. $(wc -l < "$DATA_FILE" | tr -d ' ') events produced."
