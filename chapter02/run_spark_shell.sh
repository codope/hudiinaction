#!/bin/bash
# Spark Shell command with Hudi packages and optimized memory settings

spark-shell \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --driver-memory 8g \
  --executor-memory 8g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3

