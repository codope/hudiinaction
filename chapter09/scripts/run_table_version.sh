#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/table_version}"

echo "=== Chapter 9: table version create / auto-upgrade / upgrade_table / downgrade_table ==="
echo "Base path: $BASE_PATH"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/table_version.scala"
