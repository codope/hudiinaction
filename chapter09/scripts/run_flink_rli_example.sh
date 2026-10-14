#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

FLINK_HOME="${FLINK_HOME:-$HOME/flink/flink-1.20.3}"

if ! curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
  echo "Flink cluster is not running. Run ./scripts/run_flink_start_cluster.sh first." >&2
  exit 1
fi

# Streaming INSERT INTO statements return as soon as the job is *submitted*, not once it has
# committed: the sql-client prints "successfully submitted to the cluster" and exits while the
# job keeps running (and committing) asynchronously on the cluster. Wait for all jobs to leave
# RUNNING/CREATED state before moving on, or the next step reads a table that hasn't caught up.
wait_for_flink_jobs_idle() {
  for i in $(seq 1 60); do
    local running
    running=$(curl -sf http://localhost:8081/jobs/overview 2>/dev/null | \
      python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for j in d.get('jobs',[]) if j.get('state') in ('RUNNING','CREATED')))" 2>/dev/null || echo "?")
    if [ "$running" = "0" ]; then
      return 0
    fi
    sleep 1
  done
  echo "WARNING: Flink jobs still running after 60s wait" >&2
}

echo "=== Chapter 9: Flink Record-Level Index - create table ==="
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_create_rli_table.sql"

echo "=== Chapter 9: Flink Record-Level Index - initial inserts ==="
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_insert_rli.sql"
wait_for_flink_jobs_idle

echo "=== Chapter 9: Flink Record-Level Index - upserts (must route to existing file groups) ==="
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_upsert_rli.sql"
wait_for_flink_jobs_idle

echo "=== Chapter 9: Flink Record-Level Index - verify ==="
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_verify_rli.sql"

echo "=== Chapter 9: metadata table partitions on disk ==="
find /tmp/hudiinaction/chapter09/flink_rli/orders/.hoodie/metadata -maxdepth 1 -mindepth 1 2>&1 || true
