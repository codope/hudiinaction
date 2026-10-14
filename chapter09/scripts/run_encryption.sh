#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/encryption}"

echo "=== Chapter 9: Parquet modular encryption on COW and MOR tables ==="
echo "Base path: $BASE_PATH"

echo
echo "--- Step 1/3: write + read-with-keys + upsert, COW table (encryption_cow.scala) ---"
"$SCRIPT_DIR/spark_shell_with_hudi_encryption.sh" -i "$CH_DIR/spark/encryption_cow.scala"

echo
echo "--- Step 2/3: read WITHOUT keys, fresh session, NO crypto confs at all (encryption_read_no_keys.scala) ---"
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/encryption_read_no_keys.scala"

echo
echo "--- Step 3/3: MOR table + log-file plaintext check (encryption_mor.scala) ---"
"$SCRIPT_DIR/spark_shell_with_hudi_encryption.sh" -i "$CH_DIR/spark/encryption_mor.scala"
