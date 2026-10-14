#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/early_conflict}"

echo "=== Chapter 9: early conflict detection vs. plain OCC (two writers, one JVM) ==="
echo "Base path: $BASE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/early_conflict.scala"
