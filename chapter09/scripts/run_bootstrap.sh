#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export SOURCE_PATH="${SOURCE_PATH:-/tmp/hudiinaction/chapter09/legacy/trips}"
export TABLE_PATH="${TABLE_PATH:-/tmp/hudiinaction/chapter09/hudi/trips}"

if [ ! -d "$SOURCE_PATH" ]; then
  echo "No source data at $SOURCE_PATH. Run ./scripts/generate_source_data.sh first." >&2
  exit 1
fi

echo "=== Chapter 9: bootstrapping $SOURCE_PATH into $TABLE_PATH ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/bootstrap_trips.scala"
