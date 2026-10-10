#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"
DATA_FILE="$CH_DIR/data/sample_transactions.jsonl"

echo "Producing sample transaction events to Kafka topic payments.transactions..."

while IFS= read -r line; do
  echo "$line"
done < "$DATA_FILE" | docker compose -f "$CH_DIR/docker-compose.yml" exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic payments.transactions

echo "Done. $(wc -l < "$DATA_FILE" | tr -d ' ') events produced."

REPLAY_FILE="$CH_DIR/data/replay_transactions.jsonl"
echo "Producing corrected events to payments.transactions.replay..."
docker compose -f "$CH_DIR/docker-compose.yml" exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic payments.transactions.replay < "$REPLAY_FILE"
echo "Done. $(wc -l < "$REPLAY_FILE" | tr -d ' ') events produced."
