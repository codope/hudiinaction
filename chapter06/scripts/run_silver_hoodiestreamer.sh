#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --master local[2] \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1,org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.2.1 \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar \
  --table-type MERGE_ON_READ \
  --source-ordering-field line_version \
  --target-base-path /tmp/hudi/silver/vendor_payouts \
  --target-table hudi_vendor_payouts \
  --props conf/silver-hoodiestreamer.properties \
  --source-class org.apache.hudi.utilities.sources.HoodieIncrSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --transformer-class com.hudiinaction.chapter06.silver.VendorPayoutTransformer \
  --continuous \
  --op UPSERT \
  --min-sync-interval-seconds 60
