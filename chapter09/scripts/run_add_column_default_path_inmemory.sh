#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/add_column_default_path_inmemory}"

echo "=== Chapter 9: default ADD COLUMNS on VECTOR+BLOB table, with spark.sql.catalogImplementation=in-memory ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" --conf spark.sql.catalogImplementation=in-memory -i "$CH_DIR/spark/add_column_default_path.scala"
