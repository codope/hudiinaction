#!/usr/bin/env bash
set -euo pipefail

# Prints the on-disk layout the bootstrap produced. This is Figure 9.1 as files:
# metadata-only partitions hold small skeleton files while their data stays in the
# source directory, and full-record partitions hold ordinary base files.

SOURCE_PATH="${SOURCE_PATH:-/tmp/hudiinaction/chapter09/legacy/trips}"
TABLE_PATH="${TABLE_PATH:-/tmp/hudiinaction/chapter09/hudi/trips}"

echo "=== Source Parquet (untouched by the bootstrap) ==="
find "$SOURCE_PATH" -name '*.parquet' -exec ls -lh {} \; | awk '{print $5"\t"$NF}' | sort -k2

echo
echo "=== Hudi table base files ==="
echo "Small files in trip_date=2020..2024 are skeleton files holding only the Hudi"
echo "metadata columns. Files in later partitions carry the full records."
find "$TABLE_PATH" -name '*.parquet' -not -path '*/.hoodie/*' -exec ls -lh {} \; \
  | awk '{print $5"\t"$NF}' | sort -k2

echo
echo "=== Bootstrap index ==="
if [ -d "$TABLE_PATH/.hoodie/.aux/.bootstrap" ]; then
  find "$TABLE_PATH/.hoodie/.aux/.bootstrap" -type f -exec ls -lh {} \; | awk '{print $5"\t"$NF}'
else
  echo "No bootstrap index found under $TABLE_PATH/.hoodie/.aux/.bootstrap"
fi
