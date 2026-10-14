#!/usr/bin/env bash
set -euo pipefail

# Launches spark-shell on Spark 4.1.x with the Hudi bundle and the four settings every
# Hudi-on-Spark session needs, for the VARIANT-on-Spark-4.x examples. Uses SPARK4_HOME
# (a local Spark 4.1.x install; Hudi 1.2.0 does not publish a Scala 2.13 hudi-spark4
# bundle to a Maven repo Spark's own --packages resolver can reach without also pulling
# a matching Spark distribution, so this launcher is separate from spark_shell_with_hudi.sh
# and points at a Spark 4.x binary instead of relying on `spark-shell` on PATH).
#
# Override HUDI_VERSION, SPARK_BUNDLE, or SPARK4_HOME to test another combination.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark4.1-bundle_2.13}"
SPARK4_HOME="${SPARK4_HOME:-$HOME/spark/spark-4.1.3-bin-hadoop3}"
# Workaround: Hudi's tagging/upsert path reads existing base files through
# parquet-avro's AvroReadSupport, which reflectively loads
# org.apache.parquet.variant.VariantConverters$ParentConverter once a VARIANT field is
# present in the Avro schema. Spark 4.1.x's own distribution does not ship
# parquet-variant on its classpath (only parquet-column/-common/-encoding/-hadoop), and
# hudi-spark4.1-bundle_2.13:1.2.0 does not shade it in either, so any upsert into a
# VARIANT-bearing table fails with NoClassDefFoundError without this extra artifact.
# Confirmed against Maven Central: org.apache.parquet:parquet-variant exists from 1.16.0.
PARQUET_VARIANT_VERSION="${PARQUET_VARIANT_VERSION:-1.16.0}"
# Optional comma-separated extra coordinates (e.g. the Lance Spark bundle) appended to
# --packages. Left empty by default.
EXTRA_PACKAGES="${EXTRA_PACKAGES:-}"

PACKAGES="org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION},org.apache.parquet:parquet-variant:${PARQUET_VARIANT_VERSION}"
if [ -n "$EXTRA_PACKAGES" ]; then
  PACKAGES="${PACKAGES},${EXTRA_PACKAGES}"
fi

if [ ! -x "$SPARK4_HOME/bin/spark-shell" ]; then
  echo "No spark-shell at $SPARK4_HOME/bin/spark-shell. Set SPARK4_HOME to a Spark 4.1.x install." >&2
  exit 1
fi

"$SPARK4_HOME/bin/spark-shell" \
  --packages "$PACKAGES" \
  --conf 'spark.jars.ivy=/tmp/ivy-ch9-spark4' \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"
