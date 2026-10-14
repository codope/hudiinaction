#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/schema_on_read_control}"

echo "=== Follow-up: schema-on-read CONTROL (scalar columns only) ==="
"$SCRIPT_DIR/spark_shell_with_hudi_schema_evolution.sh" -i "$CH_DIR/spark/schema_on_read_control.scala"
