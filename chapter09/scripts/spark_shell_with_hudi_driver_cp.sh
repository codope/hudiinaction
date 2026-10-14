#!/usr/bin/env bash
set -euo pipefail

# Variant launcher for the "validators" follow-up only: puts the Hudi bundle jar on the
# driver's system classpath (--jars + --driver-class-path) in addition to Spark's normal
# --packages resolution, instead of relying on --packages alone.
#
# Why: hoodie.precommit.validators run on a background thread via
# CompletableFuture.supplyAsync(...) with no explicit Executor (SparkValidatorUtils.java),
# so they execute on the JVM's common ForkJoinPool. Those worker threads pick up the
# *system* classloader as their context classloader, not Spark's MutableURLClassLoader that
# --packages jars get added to -- so a class like org.apache.spark.sql.hudi.catalog.HoodieCatalog,
# perfectly loadable on the main/REPL thread, is invisible from inside the validator's own
# thread. Putting the jar on --driver-class-path puts its classes on the actual system
# classpath/system classloader, which the common ForkJoinPool's context classloader
# defaults to, closing that gap.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"

HUDI_JAR="${HUDI_JAR:-}"
if [ -z "$HUDI_JAR" ]; then
  # Prefer the Ivy cache (populated by prior --packages runs), fall back to the local Maven repo.
  IVY_JAR="$HOME/.ivy2/cache/org.apache.hudi/${SPARK_BUNDLE}/jars/${SPARK_BUNDLE}-${HUDI_VERSION}.jar"
  M2_JAR="$HOME/.m2/repository/org/apache/hudi/${SPARK_BUNDLE}/${HUDI_VERSION}/${SPARK_BUNDLE}-${HUDI_VERSION}.jar"
  if [ -f "$IVY_JAR" ]; then
    HUDI_JAR="$IVY_JAR"
  elif [ -f "$M2_JAR" ]; then
    HUDI_JAR="$M2_JAR"
  else
    echo "Could not find $SPARK_BUNDLE-$HUDI_VERSION.jar in the Ivy or Maven cache. Set HUDI_JAR=/path/to/jar." >&2
    exit 1
  fi
fi
echo "Using Hudi bundle jar: $HUDI_JAR"

spark-shell \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION}" \
  --jars "$HUDI_JAR" \
  --driver-class-path "$HUDI_JAR" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  "$@"
