#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

: "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
: "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"

if ! curl -sf http://localhost:8181/q/health/live >/dev/null 2>&1 && ! curl -sf http://localhost:8181/api/catalog/v1/config >/dev/null 2>&1; then
  echo "Polaris server does not seem to be reachable at http://localhost:8181. Run ./scripts/run_polaris_start_server.sh first." >&2
fi

export SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"

# REPRO ONLY: with Polaris Spark client 1.3.0-incubating, catalog_polaris.md's own bare,
# unqualified `CREATE TABLE mydb.hudi_table ... USING hudi` recipe silently never reaches
# Polaris at all (HoodieCatalog's DelegatingCatalogExtension always forwards to Spark's
# default session catalog, never to a separately-named catalog) -- the write succeeds but
# nothing is registered in Polaris, and the following DESCRIBE fails with
# TABLE_OR_VIEW_NOT_FOUND. See the run log's "Result 1". For the confirmed working recipe,
# use scripts/run_polaris_hudi_table_working.sh instead.
echo "=== Chapter 9 (REPRO): bare-name CREATE TABLE through Polaris -- reproduces a silent no-op ==="
"$SCRIPT_DIR/spark_shell_with_hudi_polaris.sh" -i "$CH_DIR/spark/polaris_hudi_table_repro_bare_name.scala"
