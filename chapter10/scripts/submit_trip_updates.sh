#!/usr/bin/env bash
#
# Chapter 10, "Generating the submission command": HoodieStreamer in continuous mode for
# trip_updates, with Glue catalog sync.
#
# Load-bearing flags follow from the design; changing one changes pipeline semantics.
# Environment values (everything read from the variables below, plus master, deploy mode, memory,
# and executor flags) belong to your platform and cannot be inferred from the workload.
#
# Usage:
#   export SPARK_VERSION=3.5 SCALA=2.12 HUDI_VERSION=1.2.1
#   export UTILITIES_JAR=/path/to/hudi-utilities-slim-bundle_2.12-1.2.1.jar
#   export TABLE_BASE_PATH=s3://<bucket>/<prefix>/trip_updates
#   ./submit_trip_updates.sh [extra spark-submit options, e.g. --master yarn --deploy-mode cluster]

set -euo pipefail
: "${SPARK_VERSION:?e.g. 3.5}" "${SCALA:?e.g. 2.12}" "${HUDI_VERSION:?e.g. 1.2.1}"
: "${UTILITIES_JAR:?path to hudi-utilities-slim-bundle_<SCALA>-<HUDI_VERSION>.jar}"
: "${TABLE_BASE_PATH:?base path of the trip_updates table}"
PROPS="${PROPS:-$(cd "$(dirname "$0")/../config" && pwd)/trip_updates.properties}"

# hudi-aws-bundle carries the Glue sync tool and the DynamoDB lock provider. Without it the job
# submits, starts, and dies at the first commit. The utilities slim bundle is the application JAR
# itself, so it goes after the options as a path, not in --packages.
spark-submit \
  "$@" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --packages "org.apache.hudi:hudi-spark${SPARK_VERSION}-bundle_${SCALA}:${HUDI_VERSION},org.apache.hudi:hudi-aws-bundle:${HUDI_VERSION}" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  "$UTILITIES_JAR" \
  --props "$PROPS" \
  --target-base-path "$TABLE_BASE_PATH" --target-table trip_updates \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-ordering-fields updated_at \
  --op UPSERT --continuous --min-sync-interval-seconds 300 \
  --enable-sync --sync-tool-classes org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool
