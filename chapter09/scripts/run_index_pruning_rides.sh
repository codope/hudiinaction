#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/index_pruning_rides}"

echo "=== Chapter 9: index pruning, chapter's exact queries on 'rides' ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" --driver-memory 2g -i "$CH_DIR/spark/index_pruning_rides.scala"
