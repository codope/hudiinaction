#!/usr/bin/env bash
# Run one SQL file, together with the files named on its "-- requires:" line.
# Streaming INSERT files are submitted detached; use the Flink UI to watch or stop them.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <sql-file> [detached]"
  echo "Example: $0 sql/03_run_ingestion.sql detached"
  exit 1
fi
run_sql "$1" "${2:-}"
