#!/usr/bin/env bash
set -euo pipefail

# Variant launcher for the "lance" example only: adds the external Lance Spark bundle
# (not on the Hudi bundle's classpath) alongside the Hudi bundle via --packages. Every
# other Hudi-on-Spark setting matches spark_shell_with_hudi.sh exactly.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"
LANCE_PACKAGE="${LANCE_PACKAGE:-org.lance:lance-spark-bundle-3.5_2.12:0.4.0}"

spark-shell \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION},${LANCE_PACKAGE}" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"
