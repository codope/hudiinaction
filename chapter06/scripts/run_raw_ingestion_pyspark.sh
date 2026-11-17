#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  pyspark/raw_ingestion.py
