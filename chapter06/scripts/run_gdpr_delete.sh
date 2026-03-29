#!/usr/bin/env bash
set -euo pipefail

VENDOR_ID="${1:-V-0001}"
APP_JAR="target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar"

spark-submit \
  --class com.hudiinaction.chapter06.governance.GdprDelete \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  "$APP_JAR" \
  "$VENDOR_ID"
