#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/prewrite_validator}"

echo "=== Follow-up: 1.2.0 pre-write validator framework (hoodie.prewrite.validators) ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/prewrite_validator.scala"
