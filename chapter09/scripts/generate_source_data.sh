#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export SOURCE_PATH="${SOURCE_PATH:-/tmp/hudiinaction/chapter09/legacy/trips}"

echo "=== Chapter 9: generating the pre-existing Parquet dataset ==="
echo "Source path: $SOURCE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/generate_source_data.scala"
