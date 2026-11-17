#!/usr/bin/env bash
set -euo pipefail

VENDOR_ID="${1:-V-101}"

spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  pyspark/gdpr_delete.py \
  "$VENDOR_ID"
