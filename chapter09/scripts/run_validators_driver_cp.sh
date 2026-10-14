#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/validators_driver_cp}"

echo "=== Follow-up: pre-commit validators via the driver-classpath launcher ==="
echo "Base path: $BASE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi_driver_cp.sh" -i "$CH_DIR/spark/validators.scala"
