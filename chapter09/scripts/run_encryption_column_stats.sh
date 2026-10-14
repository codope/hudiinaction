#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/encryption_column_stats}"
export ENCRYPTION_COLUMN_KEYS="k2:rider"

echo "=== Chapter 9: metadata-table column-stats leakage check for an encrypted column (rider) ==="
"$SCRIPT_DIR/spark_shell_with_hudi_encryption.sh" -i "$CH_DIR/spark/encryption_column_stats.scala"
