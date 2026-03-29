#!/usr/bin/env bash
set -euo pipefail

APP_JAR="target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar"

spark-submit \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --class com.hudiinaction.chapter06.raw.RawIngestion \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  "$APP_JAR"
