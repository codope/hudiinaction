#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --class com.hudiinaction.chapter06.governance.SavepointManager \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar \
  create
