#!/usr/bin/env bash
set -euo pipefail

# Stops the local Flink standalone cluster, idempotently: does nothing if it is not running.
FLINK_HOME="${FLINK_HOME:-$HOME/flink/flink-1.20.3}"
REST_PORT="${REST_PORT:-8081}"

if [ ! -x "$FLINK_HOME/bin/stop-cluster.sh" ]; then
  echo "No Flink at $FLINK_HOME. Set FLINK_HOME to a Flink 1.20.x install." >&2
  exit 1
fi

if ! curl -sf "http://localhost:${REST_PORT}/overview" >/dev/null 2>&1; then
  echo "Flink cluster is not running."
  exit 0
fi

echo "Stopping Flink standalone cluster at $FLINK_HOME ..."
"$FLINK_HOME/bin/stop-cluster.sh"
