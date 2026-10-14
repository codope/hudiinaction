#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

: "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
: "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"

export SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"
export POLARIS_SPARK_CLIENT="org.apache.polaris:polaris-spark-3.5_2.12:1.4.0"

# REPRO ONLY: confirms that a per-table 'hoodie.datasource.hive_sync.enable' = 'false'
# TBLPROPERTIES override does NOT avoid the HoodieMetaSyncException from
# run_polaris_hudi_table_repro_hivesync.sh -- ProvidesHoodieConfig.buildHiveSyncConfig
# unconditionally overwrites that key from the session-level spark.sql.catalogImplementation
# flag after combining user options, so it cannot be disabled per-table. The only fix is the
# session-level flag itself. See scripts/run_polaris_hudi_table_working.sh for that fix.
echo "=== Chapter 9 (REPRO): per-table hive_sync.enable=false does NOT avoid the metastore sync error ==="
"$SCRIPT_DIR/spark_shell_with_hudi_polaris.sh" -i "$CH_DIR/spark/polaris_hudi_table_repro_hivesync_override.scala"
