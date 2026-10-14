#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Follow-up: plain Spark (no Hudi) Parquet encryption control ==="
echo "--- Step 1/2: write + read-with-keys (encryption_control_plain_parquet.scala) ---"
"$SCRIPT_DIR/spark_shell_with_hudi_encryption.sh" -i "$CH_DIR/spark/encryption_control_plain_parquet.scala"

echo
echo "--- Step 2/2: read WITHOUT keys, fresh session (encryption_control_plain_parquet_no_keys.scala) ---"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/encryption_control_plain_parquet_no_keys.scala"
