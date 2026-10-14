#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export BASE_PATH="${BASE_PATH:-/tmp/hudiinaction/chapter09/lock_audit}"

echo "=== Chapter 9: StorageBasedLockProvider on a local file:// path ==="
"$SCRIPT_DIR/spark_shell_with_hudi.sh" -i "$CH_DIR/spark/lock_audit.scala"
