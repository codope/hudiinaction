#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

export VARIANT_BASE="${VARIANT_BASE:-/tmp/hudiinaction/chapter09}"

if [ ! -d "$VARIANT_BASE/variant_cow/events" ]; then
  echo "No table at $VARIANT_BASE/variant_cow/events. Run ./scripts/run_variant_events.sh first." >&2
  exit 1
fi

echo "=== Chapter 9: inspecting the VARIANT column's on-disk Parquet schema (COW) ==="
"$SCRIPT_DIR/spark4_shell_with_hudi_variant.sh" -i "$CH_DIR/spark/variant_inspect_parquet_schema.scala"
