#!/usr/bin/env bash
set -euo pipefail

# Variant launcher for the schema-on-read follow-up: adds the Hive-metastore compatibility-
# check override schema_evolution.md documents for ALTER TABLE ADD/reorder COLUMNS against
# Spark's own embedded (Derby-backed) metastore:
#   --conf 'spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false'
# Without this, ADD COLUMNS on an already-populated table can fail with:
#   "The following columns have types incompatible with the existing columns in their
#    respective positions" -- which is exactly what chapter09's schema_ai_types example hit
#   in the first pass, on a table with VECTOR/BLOB columns (the doc confirms this check is
#   generic, not specific to those types).
# Every other Hudi-on-Spark setting matches spark_shell_with_hudi.sh exactly.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"

spark-shell \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION}" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false' \
  "$@"
