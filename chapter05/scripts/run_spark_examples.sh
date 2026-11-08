#!/usr/bin/env bash
#
# Runs the spark-shell examples in chapter order, or only the ones named:
#   ./run_spark_examples.sh                      # all seven
#   ./run_spark_examples.sh 02_inline_compaction 07_recovery
#
# Requirements: Java 11 or 17, Spark 3.5.x with spark-shell on the PATH. Tables are written under
# /tmp/hudiinaction/chapter05 (override with BASE_PATH per example).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_DIR="$(dirname "$SCRIPT_DIR")/spark"

examples=("$@")
if [ ${#examples[@]} -eq 0 ]; then
  examples=(01_file_sizing 02_inline_compaction 03_schedule_compaction 04_streaming_async_compaction
            05_inline_clustering 06_cleaning 07_recovery)
fi

for name in "${examples[@]}"; do
  echo
  echo "##### $name #####"
  "$SCRIPT_DIR/spark_shell_with_hudi.sh" --driver-memory 2g -i "$SPARK_DIR/${name%.scala}.scala"
done
