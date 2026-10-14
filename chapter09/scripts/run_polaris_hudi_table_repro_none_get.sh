#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

: "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
: "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"

export SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"

# REPRO ONLY: with Polaris Spark client 1.3.0-incubating, a fully-qualified
# `polaris_catalog.mydb.<table>` CREATE TABLE does reach the real Polaris Spark catalog, but
# crashes inside it with `java.util.NoSuchElementException: None.get`
# (org.apache.polaris.spark.utils.PolarisCatalogUtils.loadSparkTable) -- a genuine bug in that
# client version. See the run log's "Result 2". For the confirmed working recipe (client
# 1.4.0 + spark.sql.catalogImplementation=in-memory), use
# scripts/run_polaris_hudi_table_working.sh instead.
echo "=== Chapter 9 (REPRO): fully-qualified CREATE TABLE through Polaris client 1.3.0 -- reproduces None.get ==="
"$SCRIPT_DIR/spark_shell_with_hudi_polaris.sh" -i "$CH_DIR/spark/polaris_hudi_table_repro_none_get.scala"
