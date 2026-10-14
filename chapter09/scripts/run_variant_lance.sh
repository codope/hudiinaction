#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export VARIANT_BASE="${VARIANT_BASE:-/tmp/hudiinaction/chapter09}"
export EXTRA_PACKAGES="org.lance:lance-spark-bundle-4.1_2.13:0.4.0"

echo "=== Chapter 9: VARIANT on a Lance base-file-format table (expected to fail per docs) ==="
"$SCRIPT_DIR/spark4_shell_with_hudi_variant.sh" -i "$CH_DIR/spark/variant_lance.scala"
