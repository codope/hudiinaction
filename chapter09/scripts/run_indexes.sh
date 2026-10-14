#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/indexes}"

echo "=== Chapter 9: SQL index management (secondary, expression, column-stats, bloom-filter) ==="
echo "Base path: $BASE_PATH"

# Three metadata-table partitions (record index, column stats, partition stats) built
# concurrently over a few thousand rows outgrows spark-shell's default 1g driver heap
# (OutOfMemoryError observed at the default). Bump it for this example only.
"$SCRIPT_DIR/spark_shell_with_hudi.sh" --driver-memory 2g -i "$CH_DIR/spark/indexes.scala"
