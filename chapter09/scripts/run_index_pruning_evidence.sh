#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/index_pruning_evidence}"

echo "=== Follow-up: index pruning evidence (data skipping ON vs OFF, scan metrics) ==="
echo "Base path: $BASE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" --driver-memory 2g -i "$CH_DIR/spark/index_pruning_evidence.scala"
