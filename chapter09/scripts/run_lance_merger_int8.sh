#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/lance_merger_int8}"

echo "=== Chapter 9: Lance record-merger config key resolution + INT8 vector on Lance ==="
"$SCRIPT_DIR/spark_shell_with_hudi_lance.sh" -i "$CH_DIR/spark/lance_merger_int8.scala"
