#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Runs the Spark 3.5 examples that need no external service (no Flink, no Polaris) in a
# sensible order, following the chapter's own section order. Spark 4.1 (VARIANT), Flink, and
# Polaris examples are separate, documented scripts -- see the README's example table and
# "Running it" section.

echo "### Bootstrap (Running a bootstrap / Living with a bootstrapped table) ###"
"$SCRIPT_DIR/generate_source_data.sh"
"$SCRIPT_DIR/run_bootstrap.sh"
"$SCRIPT_DIR/verify_bootstrap.sh"
"$SCRIPT_DIR/show_layout.sh"

echo "### AI-native types: VECTOR, BLOB, vector search ###"
"$SCRIPT_DIR/run_ai_types.sh"
"$SCRIPT_DIR/run_blob_support_tickets.sh"

echo "### Base file formats for AI workloads: Lance ###"
"$SCRIPT_DIR/run_lance.sh"
"$SCRIPT_DIR/run_lance_merger_int8.sh"

echo "### Encrypting columns at rest ###"
"$SCRIPT_DIR/run_encryption_control.sh"
"$SCRIPT_DIR/run_encryption.sh"
"$SCRIPT_DIR/run_encryption_column_stats.sh"

echo "### Indexing: SQL-managed indexes and pruning evidence ###"
"$SCRIPT_DIR/run_indexes.sh"
"$SCRIPT_DIR/run_index_pruning_evidence.sh"
"$SCRIPT_DIR/run_index_pruning_rides.sh"

echo "### Concurrency: early conflict detection, lock providers ###"
"$SCRIPT_DIR/run_early_conflict.sh"
"$SCRIPT_DIR/run_early_conflict_lowlevel.sh"
"$SCRIPT_DIR/run_early_conflict_lowlevel_detection.sh"
"$SCRIPT_DIR/run_lock_audit.sh"

echo "### Guarding and announcing commits: validators, callbacks ###"
"$SCRIPT_DIR/run_validators_driver_cp.sh"
"$SCRIPT_DIR/run_prewrite_validator.sh"
"$SCRIPT_DIR/run_callbacks.sh"

echo "### Letting the table change over time: schema evolution, table version ###"
"$SCRIPT_DIR/run_schema_on_read_control.sh"
"$SCRIPT_DIR/run_schema_ai_types.sh"
"$SCRIPT_DIR/run_add_column_default_path.sh"
"$SCRIPT_DIR/run_table_version.sh"

echo "### Connecting the table to catalogs and engines: session options ###"
"$SCRIPT_DIR/run_session_options.sh"

echo "### run_all.sh complete. Spark 4.1 VARIANT, Flink, and Polaris examples are separate -- see README.md. ###"
