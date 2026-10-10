#!/usr/bin/env bash
#
# HoodieStreamer example for Chapter 3: continuous ingestion of Parquet files into a
# Merge-on-Read table, with asynchronous compaction.
#
# Requirements: Java 11, Spark 3.5.x (SPARK_HOME set), Hudi 1.2.0 (JARs downloaded below).
# Runs on macOS and Linux; on Windows, run it inside WSL2.
#
# Usage:
#   export SPARK_HOME=/path/to/spark-3.5.x-bin-hadoop3
#   ./run_hudi_streamer.sh
#
# Stop the streamer with Ctrl+C once all batches have been ingested (see README.md).

set -euo pipefail

: "${SPARK_HOME:?Set SPARK_HOME to your Spark 3.5.x installation}"

HUDI_VERSION=1.2.0
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STREAMER_HOME=/tmp/hudi_streamer   # the archive extracts to hudi_streamer/
BUNDLES=${BUNDLES:-/tmp/hudi_bundles}
TARGET_PATH=/tmp/hudi-deltastreamer-ny

if [ -e "$TARGET_PATH" ]; then
  echo "$TARGET_PATH already exists. Remove it to start from an empty table: rm -rf $TARGET_PATH"
  exit 1
fi

# 1. Extract the sample data, properties file and schema.
#    Creates streamer_input/, streamer_props/input.props and streamer_schema/ny.avsc.
if [ ! -d "$STREAMER_HOME" ]; then
  tar -xzf "$SCRIPT_DIR/hudi_streamer.tar.gz" -C /tmp
fi

# 2. Download the Hudi utilities slim bundle (contains HoodieStreamer) and the Hudi Spark bundle.
MAVEN=https://repo1.maven.org/maven2/org/apache/hudi
UTILITIES_JAR=hudi-utilities-slim-bundle_2.12-$HUDI_VERSION.jar
SPARK_BUNDLE_JAR=hudi-spark3.5-bundle_2.12-$HUDI_VERSION.jar
mkdir -p "$BUNDLES"
[ -f "$BUNDLES/$UTILITIES_JAR" ] || curl -fL -o "$BUNDLES/$UTILITIES_JAR" \
  "$MAVEN/hudi-utilities-slim-bundle_2.12/$HUDI_VERSION/$UTILITIES_JAR"
[ -f "$BUNDLES/$SPARK_BUNDLE_JAR" ] || curl -fL -o "$BUNDLES/$SPARK_BUNDLE_JAR" \
  "$MAVEN/hudi-spark3.5-bundle_2.12/$HUDI_VERSION/$SPARK_BUNDLE_JAR"

# 3. Prepare the input.
#    HoodieStreamer's DFS source reads files in order of modification time. Extracted files all
#    share one timestamp, which would put every file into the first batch, so each file gets its
#    own timestamp. A copy of the first file is added as the fifth batch to simulate an upstream
#    job delivering the same data twice: its rows become updates, which on a Merge-on-Read table
#    go to log files that the asynchronous compaction later merges.
cd "$STREAMER_HOME/streamer_input"
if [ ! -f replay-part-00000.parquet ]; then
  cp part-00000-*.parquet replay-part-00000.parquet
fi
i=1
for f in part-0000[0-3]-*.parquet replay-part-00000.parquet part-0000[4-9]-*.parquet; do
  touch -t "2026010100$(printf '%02d' "$i")" "$f"
  i=$((i+1))
done

# 4. Launch HoodieStreamer in continuous mode.
#    Record key and partition field are in input.props; the ordering field is set here.
"$SPARK_HOME/bin/spark-submit" \
  --driver-memory 4g \
  --executor-memory 4g \
  --jars "$BUNDLES/$SPARK_BUNDLE_JAR" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$BUNDLES/$UTILITIES_JAR" \
  --props "file://$STREAMER_HOME/streamer_props/input.props" \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --source-ordering-fields tpep_dropoff_datetime \
  --table-type MERGE_ON_READ \
  --target-base-path "file://$TARGET_PATH" \
  --target-table ny_hudi_tbl \
  --op UPSERT \
  --continuous \
  --source-limit 30000000 \
  --min-sync-interval-seconds 30 \
  --hoodie-conf "hoodie.streamer.source.dfs.root=file://$STREAMER_HOME/streamer_input" \
  --hoodie-conf "hoodie.streamer.schemaprovider.source.schema.file=file://$STREAMER_HOME/streamer_schema/ny.avsc"
