#!/usr/bin/env bash
set -euo pipefail

HUDI_VERSION="${HUDI_VERSION:-0.15.0}"

spark-sql \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:${HUDI_VERSION} \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"

