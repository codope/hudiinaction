#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

FLINK_HOME="${FLINK_HOME:-$HOME/flink/flink-1.20.3}"

if ! curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
  echo "Flink cluster is not running. Run ./scripts/run_flink_start_cluster.sh first." >&2
  exit 1
fi
if [ ! -d /tmp/hudiinaction/chapter09/flink_rli/orders/.hoodie ]; then
  echo "No table at /tmp/hudiinaction/chapter09/flink_rli/orders. Run ./scripts/run_flink_rli_example.sh first." >&2
  exit 1
fi

echo "=== Chapter 9: Flink Source V2 - batch read comparison (legacy vs FLIP-27) ==="
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_source_v2_batch_compare.sql"

echo "=== Chapter 9: Flink Source V2 - streaming read (best-effort, bounded by a timeout) ==="
rm -rf /tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2
LOG="/tmp/hudiinaction/chapter09/flink_rli/source_v2_streaming_sqlclient.log"
"$FLINK_HOME/bin/sql-client.sh" -f "$CH_DIR/flink/flink_source_v2_streaming.sql" > "$LOG" 2>&1 &
CLIENT_PID=$!
echo "sql-client pid $CLIENT_PID, waiting up to 25s for the streaming job to appear and produce output..."
for i in $(seq 1 25); do
  if [ -d /tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2 ] && \
     find /tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2 -type f -name "*.csv" 2>/dev/null | grep -q .; then
    break
  fi
  sleep 1
done

echo "--- sql-client output so far ---"
cat "$LOG" || true

JOB_ID=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for j in d.get('jobs', []):
    if j.get('state') == 'RUNNING':
        print(j['jid'])
        break
" 2>/dev/null || true)

if [ -n "${JOB_ID:-}" ]; then
  echo "Cancelling streaming job $JOB_ID ..."
  curl -s -X PATCH "http://localhost:8081/jobs/${JOB_ID}?mode=cancel" || true
  echo
else
  echo "No RUNNING job found to cancel (it may have already finished, or never started)."
fi

kill "$CLIENT_PID" 2>/dev/null || true
wait "$CLIENT_PID" 2>/dev/null || true

echo "--- rows written to the streaming sink (file:///tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2) ---"
find /tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2 -type f 2>/dev/null -exec cat {} \; || echo "(no output files found)"
