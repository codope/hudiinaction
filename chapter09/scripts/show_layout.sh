#!/usr/bin/env bash
set -euo pipefail

# Prints the on-disk layout the bootstrap produced. This is Figure 9.1 as files: the
# metadata-only partitions hold skeleton files whose data stays in the source directory,
# the full-record partitions hold ordinary base files, and the bootstrap index maps one
# to the other. Run verify_bootstrap.sh to see which columns each file actually holds.

SOURCE_PATH="${SOURCE_PATH:-/tmp/hudiinaction/chapter09/legacy/trips}"
TABLE_PATH="${TABLE_PATH:-/tmp/hudiinaction/chapter09/hudi/trips}"

echo "=== Source Parquet (untouched by the bootstrap) ==="
find "$SOURCE_PATH" -name '*.parquet' -exec ls -lh {} \; | awk '{print $5"\t"$NF}' | sort -k2

echo
echo "=== Hudi table base files ==="
echo "Files committed at instant ...01 are skeleton files holding only the Hudi metadata"
echo "columns; those at ...02 carry the full records. At this dataset size the two are"
echo "about the same size, because fixed per-file overhead dwarfs eight rows. What"
echo "metadata-only saves is the rewrite itself, which shows at real volumes."
find "$TABLE_PATH" -name '*.parquet' -not -path '*/.hoodie/*' -exec ls -lh {} \; \
  | awk '{print $5"\t"$NF}' | sort -k2

echo
echo "=== Bootstrap index ==="
if [ -d "$TABLE_PATH/.hoodie/.aux/.bootstrap" ]; then
  find "$TABLE_PATH/.hoodie/.aux/.bootstrap" -type f -exec ls -lh {} \; | awk '{print $5"\t"$NF}'
else
  echo "No bootstrap index found under $TABLE_PATH/.hoodie/.aux/.bootstrap"
fi
