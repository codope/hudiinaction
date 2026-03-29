#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --master local[2] \
  --jars target/scala-2.12/chapter06-marketplace-payout-lakehouse_2.12-0.1.0-SNAPSHOT.jar \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  /Users/nsb/dev/projects/hudi_sivabalan_branch_0x/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.15.0.jar \
  --table-type MERGE_ON_READ \
  --source-ordering-field last_updated_ts \
  --target-base-path /tmp/hudi/silver/vendor_payouts \
  --target-table hudi_vendor_payouts \
  --props conf/silver-hoodiestreamer.properties \
  --source-class org.apache.hudi.utilities.sources.HoodieIncrSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --transformer-class com.hudiinaction.chapter06.silver.VendorPayoutTransformer \
  --continuous \
  --op UPSERT \
  --min-sync-interval-seconds 60
