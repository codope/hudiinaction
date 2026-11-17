#!/usr/bin/env bash
set -euo pipefail

TABLE_TYPE="${1:-raw}"          # raw | silver
INSTANT_TIME="${2:-}"
VENDOR_ID="${3:-V-101}"

if [[ -z "${INSTANT_TIME}" ]]; then
  echo "Usage: $0 <raw|silver> <instant_time> [vendor_id]"
  exit 1
fi

spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  pyspark/time_travel_examples.py \
  "$TABLE_TYPE" "$INSTANT_TIME" "$VENDOR_ID"
