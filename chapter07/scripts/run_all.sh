#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

STREAMING_FILES="03_run_ingestion.sql 06_run_ingestion_bucket.sql 11_run_cdc_pipeline.sql 15_run_silver_pipeline.sql"

submit_sql() {
  local file="$1"
  docker compose -f "$CH_DIR/docker-compose.yml" exec -T jobmanager \
    /opt/flink/bin/sql-client.sh -f "/opt/sql/$file"
}

is_streaming() {
  local file="$1"
  for s in $STREAMING_FILES; do
    [ "$file" = "$s" ] && return 0
  done
  return 1
}

echo "=== Chapter 7: Running all SQL sections ==="
echo ""
echo "NOTE: Streaming INSERT INTO jobs (03, 06, 11, 15) are submitted in"
echo "detached mode. They run as background Flink jobs. Use the Flink UI"
echo "at http://localhost:8081 to monitor or cancel them."
echo ""

for sql_file in "$CH_DIR"/sql/*.sql; do
  basename="$(basename "$sql_file")"
  echo "--- $basename ---"

  if is_streaming "$basename"; then
    echo "  [streaming] Submitting in detached mode..."
    docker compose -f "$CH_DIR/docker-compose.yml" exec -T jobmanager \
      /opt/flink/bin/sql-client.sh -D execution.attached=false -f "/opt/sql/$basename"
    sleep 5
  else
    submit_sql "$basename"
  fi
  echo ""
done

echo "=== All DDL/batch sections complete ==="
echo "Streaming jobs are running in the background."
echo "Monitor at http://localhost:8081"
