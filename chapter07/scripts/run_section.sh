#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <sql-file>"
  echo "Example: $0 sql/01_create_kafka_source.sql"
  exit 1
fi

SQL_FILE="$1"
SQL_BASENAME="$(basename "$SQL_FILE")"

echo "=== Running: $SQL_BASENAME ==="
docker compose -f "$CH_DIR/docker-compose.yml" exec -T jobmanager \
  /opt/flink/bin/sql-client.sh -f "/opt/sql/$SQL_BASENAME"
echo "=== Done: $SQL_BASENAME ==="
