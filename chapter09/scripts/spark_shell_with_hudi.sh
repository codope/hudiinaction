#!/usr/bin/env bash
set -euo pipefail

# Launches spark-shell with the Hudi bundle and the four settings every Hudi-on-Spark
# session needs. Override HUDI_VERSION or SPARK_BUNDLE to test another combination.

HUDI_VERSION="${HUDI_VERSION:-1.2.0}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"

spark-shell \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION}" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"
