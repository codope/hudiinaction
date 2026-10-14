#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export VARIANT_BASE="${VARIANT_BASE:-/tmp/hudiinaction/chapter09}"

echo "=== Chapter 9: VARIANT on Spark 4.x (COW + MOR) into $VARIANT_BASE ==="
"$SCRIPT_DIR/spark4_shell_with_hudi_variant.sh" -i "$CH_DIR/spark/variant_events.scala"
