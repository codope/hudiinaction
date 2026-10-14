#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/schema_ai_types}"

echo "=== Chapter 9: schema-on-read (add/rename column) on a table with VECTOR + BLOB ==="
echo "Base path: $BASE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi_schema_evolution.sh" -i "$CH_DIR/spark/schema_ai_types.scala"
