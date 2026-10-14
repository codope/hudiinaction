#!/usr/bin/env bash
set -euo pipefail

# Starts a local Flink standalone cluster, idempotently: does nothing if the REST endpoint
# already answers. Override FLINK_HOME to point at another Flink 1.20.x install.

FLINK_HOME="${FLINK_HOME:-$HOME/flink/flink-1.20.3}"
REST_PORT="${REST_PORT:-8081}"

if [ ! -x "$FLINK_HOME/bin/start-cluster.sh" ]; then
  echo "No Flink at $FLINK_HOME. Set FLINK_HOME to a Flink 1.20.x install." >&2
  exit 1
fi

if curl -sf "http://localhost:${REST_PORT}/overview" >/dev/null 2>&1; then
  echo "Flink cluster already running at http://localhost:${REST_PORT}"
  exit 0
fi

echo "Starting Flink standalone cluster at $FLINK_HOME ..."
"$FLINK_HOME/bin/start-cluster.sh"

echo "Waiting for the REST endpoint to come up..."
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${REST_PORT}/overview" >/dev/null 2>&1; then
    echo "Flink cluster is up: http://localhost:${REST_PORT}"
    curl -s "http://localhost:${REST_PORT}/overview"
    echo
    exit 0
  fi
  sleep 1
done

echo "Flink cluster did not come up within 30s. Check $FLINK_HOME/log/*.log" >&2
exit 1
