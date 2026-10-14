#!/usr/bin/env bash
set -euo pipefail

# Launches spark-shell on Spark 3.5.6 with Hudi 1.2.0 AND the Polaris Spark catalog client
# configured, per docs/versioned_docs/version-1.2.0/catalog_polaris.md: Hudi delegates table
# creation to the Polaris Spark catalog when a spark.sql.catalog.<name> value matches
# hoodie.spark.polaris.catalog.class. The Polaris REST connection options (uri, credential,
# warehouse, scope) are not documented on that page; they are the standard Iceberg
# REST-catalog-style options the Polaris Spark client (org.apache.polaris.spark.SparkCatalog)
# expects, taken from polaris.apache.org's own Spark quickstart. Requires a running Polaris
# server (scripts/run_polaris_start_server.sh) and POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET
# for a principal already granted a role on POLARIS_CATALOG_NAME.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"
SPARK_HOME_DIR="${SPARK_HOME_DIR:-$HOME/spark/spark-3.5.6-bin-hadoop3}"
POLARIS_SPARK_CLIENT="${POLARIS_SPARK_CLIENT:-org.apache.polaris:polaris-spark-3.5_2.12:1.4.0}"
POLARIS_URI="${POLARIS_URI:-http://localhost:8181/api/catalog}"
POLARIS_CATALOG_NAME="${POLARIS_CATALOG_NAME:-hudi_catalog}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:?Set POLARIS_CLIENT_ID to a Polaris principal's clientId}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:?Set POLARIS_CLIENT_SECRET to that principal's clientSecret}"

if [ ! -x "$SPARK_HOME_DIR/bin/spark-shell" ]; then
  echo "No spark-shell at $SPARK_HOME_DIR/bin/spark-shell. Set SPARK_HOME_DIR to a Spark 3.5.x install." >&2
  exit 1
fi

"$SPARK_HOME_DIR/bin/spark-shell" \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION},${POLARIS_SPARK_CLIENT}" \
  --conf 'spark.jars.ivy=/tmp/ivy-ch9-spark4' \
  --conf 'spark.sql.warehouse.dir=file:///tmp/hudiinaction/chapter09/spark-warehouse' \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.polaris_catalog=org.apache.polaris.spark.SparkCatalog' \
  --conf "spark.sql.catalog.polaris_catalog.uri=${POLARIS_URI}" \
  --conf "spark.sql.catalog.polaris_catalog.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
  --conf "spark.sql.catalog.polaris_catalog.warehouse=${POLARIS_CATALOG_NAME}" \
  --conf 'spark.sql.catalog.polaris_catalog.scope=PRINCIPAL_ROLE:ALL' \
  --conf 'spark.sql.catalog.polaris_catalog.token-refresh-enabled=true' \
  --conf 'spark.sql.catalog.polaris_catalog.header.X-Iceberg-Access-Delegation=vended-credentials' \
  --conf 'hoodie.spark.polaris.catalog.class=org.apache.polaris.spark.SparkCatalog' \
  --conf 'spark.sql.catalogImplementation=in-memory' \
  "$@"
