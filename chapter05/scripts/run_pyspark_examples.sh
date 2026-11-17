#!/usr/bin/env bash
#
# Runs the PySpark examples in chapter order, or only the ones named:
#   ./run_pyspark_examples.sh                      # all seven
#   ./run_pyspark_examples.sh 02_inline_compaction 07_recovery
#
# Requirements: Java 11 or 17, Spark 3.5.x with spark-submit on the PATH, Python 3.8 or later.
# Tables are written under /tmp/hudiinaction/chapter05 (override with BASE_PATH per example).
# Unlike run_spark_examples.sh, this stops at the first example that fails.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYSPARK_DIR="$(dirname "$SCRIPT_DIR")/pyspark"

examples=("$@")
if [ ${#examples[@]} -eq 0 ]; then
  examples=(01_file_sizing 02_inline_compaction 03_schedule_compaction 04_streaming_async_compaction
            05_inline_clustering 06_cleaning 07_recovery)
fi

for name in "${examples[@]}"; do
  echo
  echo "##### $name #####"
  "$SCRIPT_DIR/spark_submit_with_hudi.sh" --driver-memory 2g "$PYSPARK_DIR/${name%.py}.py"
done
