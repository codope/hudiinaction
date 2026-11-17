#!/usr/bin/env bash
set -euo pipefail

# Runs one PySpark example from pyspark/ by name, with the launcher options it needs:
#   ./scripts/run_pyspark.sh ai_types
#   ./scripts/run_pyspark.sh variant_events      # Spark 4.1, see below
#
# Most examples need only Spark 3.5 (spark-submit on the PATH) and Python 3.8 or later.
# - variant_*: Spark 4.1 from SPARK4_HOME and Python 3.10 or later (PySpark 4.1's minimum);
#   set PYSPARK4_PYTHON if `python3` is older. variant_inspect_parquet_schema needs only
#   Python with pyarrow.
# - lance, lance_merger_int8, variant_lance: add the Lance Spark bundle.
# - validators: puts the Hudi bundle on --driver-class-path (see the README's environment notes).
# - callbacks: starts scripts/http_callback_receiver.py and prints what it received.
# - polaris_hudi_table_working: needs a running, bootstrapped Polaris server and
#   POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_DIR="$(dirname "$SCRIPT_DIR")/pyspark"
SUBMIT="$SCRIPT_DIR/spark_submit_with_hudi.sh"
HUDI_VERSION="${HUDI_VERSION:-1.2.1}"

name="${1:?Usage: $0 <example>, one of the file names in pyspark/ without .py}"
name="${name%.py}"
shift
script="$PY_DIR/$name.py"
[ -f "$script" ] || { echo "No such example: $script" >&2; exit 1; }

case "$name" in
  variant_inspect_parquet_schema)
    exec python3 "$script" "$@"
    ;;
  variant_*)
    SPARK4_HOME="${SPARK4_HOME:-$HOME/spark/spark-4.1.3-bin-hadoop3}"
    export PYSPARK_PYTHON="${PYSPARK4_PYTHON:-${PYSPARK_PYTHON:-python3}}"
    # parquet-variant: Hudi 1.2.0's upsert path needs it for VARIANT; 1.2.1 shades it, so it is
    # harmless there.
    EXTRA_PACKAGES="org.apache.parquet:parquet-variant:1.16.0"
    if [ "$name" = variant_lance ]; then
      EXTRA_PACKAGES="$EXTRA_PACKAGES,org.lance:lance-spark-bundle-4.1_2.13:0.4.0"
    fi
    SPARK_SUBMIT="$SPARK4_HOME/bin/spark-submit" SPARK_BUNDLE=hudi-spark4.1-bundle_2.13 EXTRA_PACKAGES="$EXTRA_PACKAGES" \
      exec "$SUBMIT" --conf 'spark.jars.ivy=/tmp/ivy-ch9-spark4' "$script" "$@"
    ;;
  lance|lance_merger_int8)
    EXTRA_PACKAGES="${LANCE_PACKAGE:-org.lance:lance-spark-bundle-3.5_2.12:0.4.0}" exec "$SUBMIT" "$script" "$@"
    ;;
  indexes|index_pruning_rides)
    # Building three metadata-table partitions outgrows the default 1g driver heap.
    exec "$SUBMIT" --driver-memory 2g "$script" "$@"
    ;;
  validators)
    SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"
    HUDI_JAR="${HUDI_JAR:-}"
    if [ -z "$HUDI_JAR" ]; then
      for candidate in \
          "$HOME/.ivy2/jars/org.apache.hudi_${SPARK_BUNDLE}-${HUDI_VERSION}.jar" \
          "$HOME/.ivy2/cache/org.apache.hudi/${SPARK_BUNDLE}/jars/${SPARK_BUNDLE}-${HUDI_VERSION}.jar" \
          "$HOME/.m2/repository/org/apache/hudi/${SPARK_BUNDLE}/${HUDI_VERSION}/${SPARK_BUNDLE}-${HUDI_VERSION}.jar"; do
        if [ -f "$candidate" ]; then HUDI_JAR="$candidate"; break; fi
      done
    fi
    if [ -z "$HUDI_JAR" ]; then
      echo "Could not find $SPARK_BUNDLE-$HUDI_VERSION.jar in the Ivy or Maven cache. Run any other example" >&2
      echo "first so --packages downloads it, or set HUDI_JAR=/path/to/jar." >&2
      exit 1
    fi
    exec "$SUBMIT" --driver-class-path "$HUDI_JAR" "$script" "$@"
    ;;
  callbacks)
    export CALLBACK_PORT="${CALLBACK_PORT:-9192}"
    RECEIVER_LOG="/tmp/hudiinaction/chapter09/callback_receiver.log"
    mkdir -p "$(dirname "$RECEIVER_LOG")"
    python3 "$SCRIPT_DIR/http_callback_receiver.py" "$CALLBACK_PORT" > "$RECEIVER_LOG" 2>&1 &
    RECEIVER_PID=$!
    trap 'kill "$RECEIVER_PID" 2>/dev/null || true' EXIT
    sleep 1
    "$SUBMIT" "$script" "$@"
    sleep 1
    echo
    echo "=== Payload captured by the receiver ==="
    cat "$RECEIVER_LOG"
    ;;
  polaris_*)
    : "${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID (see scripts/run_polaris_bootstrap.sh output)}"
    : "${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET}"
    EXTRA_PACKAGES="${POLARIS_SPARK_CLIENT:-org.apache.polaris:polaris-spark-3.5_2.12:1.4.0}" exec "$SUBMIT" \
      --conf 'spark.sql.catalogImplementation=in-memory' \
      --conf 'spark.sql.catalog.polaris_catalog=org.apache.polaris.spark.SparkCatalog' \
      --conf "spark.sql.catalog.polaris_catalog.uri=${POLARIS_URI:-http://localhost:8181/api/catalog}" \
      --conf "spark.sql.catalog.polaris_catalog.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
      --conf "spark.sql.catalog.polaris_catalog.warehouse=${POLARIS_CATALOG_NAME:-hudi_catalog}" \
      --conf 'spark.sql.catalog.polaris_catalog.scope=PRINCIPAL_ROLE:ALL' \
      --conf 'spark.sql.catalog.polaris_catalog.token-refresh-enabled=true' \
      --conf 'hoodie.spark.polaris.catalog.class=org.apache.polaris.spark.SparkCatalog' \
      "$script" "$@"
    ;;
  *)
    exec "$SUBMIT" "$script" "$@"
    ;;
esac
