#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Runs the PySpark examples that need only Spark 3.5 (no Flink, Polaris or Spark 4.1), in the
# chapter's section order. The VARIANT and Polaris examples are run separately with
# run_pyspark.sh -- see the README.

for name in ai_types blob_support_tickets lance lance_merger_int8 \
            indexes index_pruning_rides \
            early_conflict lock_audit \
            validators callbacks \
            add_column_default_path schema_on_read_control schema_ai_types table_version; do
  echo
  echo "### $name ###"
  "$SCRIPT_DIR/run_pyspark.sh" "$name"
done

echo "### run_all_pyspark.sh complete. VARIANT (Spark 4.1) and Polaris examples are separate -- see README.md. ###"
