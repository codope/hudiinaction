#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/add_column_default_path}"

echo "=== Chapter 9: default (non-schema-on-read) ADD COLUMNS on a VECTOR+BLOB table, schema-evolution launcher ==="
"$SCRIPT_DIR/spark_shell_with_hudi_schema_evolution.sh" -i "$CH_DIR/spark/add_column_default_path.scala"
