#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

: "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
: "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"

export SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"
export POLARIS_SPARK_CLIENT="org.apache.polaris:polaris-spark-3.5_2.12:1.4.0"

# REPRO ONLY: with the Polaris Spark client bumped to 1.4.0 (which fixes the None.get crash),
# CREATE TABLE succeeds, but the following INSERT fails with HoodieMetaSyncException, because
# Hudi's default Hive-metastore sync is driven by the *global*
# spark.sql.catalogImplementation session flag (which spark-shell defaults to "hive"), not by
# which named catalog actually created the table -- so it tries to Hive-sync a table that was
# never meant to go through Hive at all. See the run log's "Working recipe" section. For the
# confirmed full fix (in-memory catalog), use scripts/run_polaris_hudi_table_working.sh.
echo "=== Chapter 9 (REPRO): Polaris client 1.4.0, default hive catalogImplementation -- reproduces HoodieMetaSyncException on INSERT ==="
"$SCRIPT_DIR/spark_shell_with_hudi_polaris.sh" -i "$CH_DIR/spark/polaris_hudi_table_repro_hivesync.scala"
