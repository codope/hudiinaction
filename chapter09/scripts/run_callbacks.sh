#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/callbacks}"
export CALLBACK_PORT="${CALLBACK_PORT:-9192}"
RECEIVER_LOG="/tmp/hudiinaction/chapter09/callback_receiver.log"
mkdir -p "$(dirname "$RECEIVER_LOG")"

echo "=== Chapter 9: post-commit HTTP callback to a local receiver ==="
echo "Starting receiver on 127.0.0.1:$CALLBACK_PORT (log: $RECEIVER_LOG)"
python3 "$SCRIPT_DIR/http_callback_receiver.py" "$CALLBACK_PORT" > "$RECEIVER_LOG" 2>&1 &
RECEIVER_PID=$!
trap 'kill "$RECEIVER_PID" 2>/dev/null || true' EXIT
sleep 1

"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/callbacks.scala"

sleep 1
echo
echo "=== Payload captured by the receiver ==="
cat "$RECEIVER_LOG"
