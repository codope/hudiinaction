#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Chapter 7: Running all SQL sections ==="
echo ""

for sql_file in "$CH_DIR"/sql/*.sql; do
  basename="$(basename "$sql_file")"
  echo "--- $basename ---"
  docker compose -f "$CH_DIR/docker-compose.yml" exec -T jobmanager \
    /opt/flink/bin/sql-client.sh -f "/opt/sql/$basename"
  echo ""
done

echo "=== All sections complete ==="
