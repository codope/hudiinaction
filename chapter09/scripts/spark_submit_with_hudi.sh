#!/usr/bin/env bash
set -euo pipefail

# Runs a PySpark script with spark-submit, the Hudi bundle and the four settings every
# Hudi-on-Spark session needs. The same options start an interactive pyspark shell.
# Override HUDI_VERSION or SPARK_BUNDLE to test another combination, EXTRA_PACKAGES to add
# comma-separated Maven coordinates, and SPARK_SUBMIT to use another Spark installation.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"
EXTRA_PACKAGES="${EXTRA_PACKAGES:-}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"

PACKAGES="org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION}"
if [ -n "$EXTRA_PACKAGES" ]; then
  PACKAGES="${PACKAGES},${EXTRA_PACKAGES}"
fi

"$SPARK_SUBMIT" \
  --packages "$PACKAGES" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"
