#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export TABLE_PATH="${TABLE_PATH:-/tmp/hudiinaction/chapter09/hudi/trips}"

echo "=== Chapter 9: querying the bootstrapped table ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/verify_bootstrap.scala"
