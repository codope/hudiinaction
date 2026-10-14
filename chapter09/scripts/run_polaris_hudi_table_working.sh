#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

: "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
: "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"

export SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"
# Both fixes below are needed together (confirmed in the run log's "Polaris investigation,
# 2026-09-27" section): the Polaris Spark client bump alone fixes the None.get crash on
# CREATE TABLE, but INSERT still fails with HoodieMetaSyncException unless the session's
# catalog implementation is also switched off "hive" (Hudi's default Hive-metastore sync is
# gated on the *global* spark.sql.catalogImplementation flag, not on which named catalog
# actually created the table).
export POLARIS_SPARK_CLIENT="${POLARIS_SPARK_CLIENT:-org.apache.polaris:polaris-spark-3.5_2.12:1.4.0}"

echo "=== Chapter 9: CREATE TABLE + INSERT + SELECT through Polaris, confirmed working recipe ==="
echo "Polaris Spark client: $POLARIS_SPARK_CLIENT ; spark.sql.catalogImplementation=in-memory"
"$SCRIPT_DIR/spark_shell_with_hudi_polaris.sh" --conf spark.sql.catalogImplementation=in-memory \
  -i "$CH_DIR/spark/polaris_hudi_table_working.scala"
