#!/usr/bin/env bash
set -euo pipefail

# Variant launcher for the encryption examples only: passes the Parquet modular
# encryption settings as spark.hadoop.* launch-time confs, so they are part of the
# Hadoop Configuration from the moment the SparkContext is created (mutating
# spark.sparkContext.hadoopConfiguration from inside a running spark-shell session, after
# the fact, was tried first and did not reach the Parquet writer -- see the run log).
#
# CONTROL TEST FINDING (see the run log's dated Follow-up section): the original
# "Could not instantiate KmsClient class: null" failure reproduced on a PLAIN
# df.write.parquet() with no Hudi involved at all, and the real cause underneath it was
# ClassNotFoundException: org.apache.parquet.crypto.keytools.mocks.InMemoryKMS. Spark
# 3.5.6 ships parquet-hadoop-1.13.1.jar, whose *main* jar does not include the
# org.apache.parquet.crypto.keytools.mocks package at all -- InMemoryKMS only exists in the
# parquet-hadoop test-jar (classifier "tests"), which is not on Spark's classpath and is not
# resolvable through a plain `--packages group:artifact:version` coordinate (no classifier
# support there). So this was a test-environment gap, not a Hudi or Spark bug: the mock KMS
# client the docs' recipe depends on simply isn't shipped with a standard Spark 3.5
# distribution. Fixed by downloading that test-jar once and adding it via --jars.
#
# Every other Hudi-on-Spark setting matches spark_shell_with_hudi.sh exactly.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
SPARK_BUNDLE="${SPARK_BUNDLE:-hudi-spark3.5-bundle_2.12}"
PARQUET_VERSION="${PARQUET_VERSION:-1.13.1}"
# Overridable so other examples (e.g. the column-stats-leakage check, which needs `rider`
# encrypted to match the chapter's exact recipe) can point at a different column without a
# second copy of this launcher.
ENCRYPTION_COLUMN_KEYS="${ENCRYPTION_COLUMN_KEYS:-k2:fare}"

EXTRA_JARS_DIR="${EXTRA_JARS_DIR:-$HOME/spark-extra-jars}"
mkdir -p "$EXTRA_JARS_DIR"
PARQUET_TESTS_JAR="$EXTRA_JARS_DIR/parquet-hadoop-${PARQUET_VERSION}-tests.jar"
if [ ! -f "$PARQUET_TESTS_JAR" ]; then
  echo "Downloading parquet-hadoop-${PARQUET_VERSION}-tests.jar (holds the InMemoryKMS mock KMS client the encryption recipe needs) ..."
  curl -sL -o "$PARQUET_TESTS_JAR" \
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/${PARQUET_VERSION}/parquet-hadoop-${PARQUET_VERSION}-tests.jar"
fi

spark-shell \
  --packages "org.apache.hudi:${SPARK_BUNDLE}:${HUDI_VERSION}" \
  --jars "$PARQUET_TESTS_JAR" \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.hadoop.parquet.crypto.factory.class=org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory' \
  --conf 'spark.hadoop.parquet.encryption.kms.client.class=org.apache.parquet.crypto.keytools.mocks.InMemoryKMS' \
  --conf 'spark.hadoop.parquet.encryption.key.list=k1:AAECAwQFBgcICQoLDA0ODw==, k2:AAECAAECAAECAAECAAECAA==' \
  --conf 'spark.hadoop.parquet.encryption.footer.key=k1' \
  --conf "spark.hadoop.parquet.encryption.column.keys=${ENCRYPTION_COLUMN_KEYS}" \
  "$@"
