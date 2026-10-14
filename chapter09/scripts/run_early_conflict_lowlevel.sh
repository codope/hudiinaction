#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/early_conflict_lowlevel}"

echo "=== Follow-up: OCC conflict via the low-level SparkRDDWriteClient API ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/early_conflict_lowlevel.scala"
