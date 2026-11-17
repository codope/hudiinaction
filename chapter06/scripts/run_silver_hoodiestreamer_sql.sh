#!/usr/bin/env bash
set -euo pipefail

# The same HoodieStreamer job as run_silver_hoodiestreamer.sh, without the sbt-built jar: the
# transformation is SqlQueryBasedTransformer with the query in
# conf/silver-hoodiestreamer-sql.properties, and the application jar is Hudi's utilities
# bundle, downloaded once into $BUNDLES.

HUDI_VERSION="${HUDI_VERSION:-1.2.1}"
BUNDLES="${BUNDLES:-/tmp/hudi_bundles}"
UTILITIES_JAR="$BUNDLES/hudi-utilities-slim-bundle_2.12-$HUDI_VERSION.jar"

mkdir -p "$BUNDLES"
[ -f "$UTILITIES_JAR" ] || curl -fL -o "$UTILITIES_JAR" \
  "https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-slim-bundle_2.12/$HUDI_VERSION/$(basename "$UTILITIES_JAR")"

spark-submit \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --master local[2] \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:${HUDI_VERSION} \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$UTILITIES_JAR" \
  --table-type MERGE_ON_READ \
  --source-ordering-field line_version \
  --target-base-path /tmp/hudi/silver/vendor_payouts \
  --target-table hudi_vendor_payouts \
  --props conf/silver-hoodiestreamer-sql.properties \
  --source-class org.apache.hudi.utilities.sources.HoodieIncrSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer \
  --continuous \
  --op UPSERT \
  --min-sync-interval-seconds 60
