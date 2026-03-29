#!/usr/bin/env bash
set -euo pipefail

TABLE_TYPE="${1:-raw}"          # raw | silver
INSTANT_TIME="${2:-}"
VENDOR_ID="${3:-V-101}"

if [[ -z "${INSTANT_TIME}" ]]; then
  echo "Usage: $0 <raw|silver> <instant_time> [vendor_id]"
  exit 1
fi

APP_JAR="target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar"

spark-submit \
  --class com.hudiinaction.chapter06.governance.TimeTravelExamples \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  "$APP_JAR" \
  "$TABLE_TYPE" "$INSTANT_TIME" "$VENDOR_ID"
