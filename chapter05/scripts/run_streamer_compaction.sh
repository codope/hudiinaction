#!/usr/bin/env bash
#
# Chapter 5, "Configuring compaction with HoodieStreamer": continuous ingestion into a
# Merge-on-Read table, with compaction scheduled by the cadence in streamer/compaction.properties
# (10 delta commits or 30 minutes) and executed asynchronously in the same application.
#
# Requirements: Java 11 or 17, Spark 3.5.x (SPARK_HOME set), network access to Maven Central.
# Usage: ./run_streamer_compaction.sh
# The streamer is stopped once a compaction completes, or after TIMEOUT_SECONDS (default 900).

set -euo pipefail
source "$(dirname "$0")/common.sh"

TARGET="$WORK_DIR/streamer_compaction/ny_hudi_mor"
LOG="$WORK_DIR/streamer_compaction.log"
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-900}

download_bundles
prepare_data
rm -rf "$TARGET"
mkdir -p "$(dirname "$TARGET")"

echo "Starting HoodieStreamer (log: $LOG)"
"$SPARK_HOME/bin/spark-submit" \
  --driver-memory 4g \
  --jars "$SPARK_BUNDLE_JAR" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$UTILITIES_JAR" \
  --props "file://$CH05_DIR/streamer/compaction.properties" \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --source-ordering-fields tpep_dropoff_datetime \
  --table-type MERGE_ON_READ \
  --target-base-path "file://$TARGET" \
  --target-table ny_hudi_mor \
  --op UPSERT \
  --continuous \
  --source-limit 30000000 \
  --min-sync-interval-seconds 15 \
  --hoodie-conf "hoodie.streamer.source.dfs.root=file://$DATA_DIR/streamer_input" \
  --hoodie-conf "hoodie.streamer.schemaprovider.source.schema.file=file://$DATA_DIR/streamer_schema/ny.avsc" \
  > "$LOG" 2>&1 &
STREAMER_PID=$!
trap 'kill $STREAMER_PID 2>/dev/null || true' EXIT

for ((s = 0; s < TIMEOUT_SECONDS; s += 15)); do
  sleep 15
  if ! kill -0 "$STREAMER_PID" 2>/dev/null; then
    echo "HoodieStreamer exited early; see $LOG"
    exit 1
  fi
  echo "  ${s}s: $(completed "$TARGET" deltacommit) delta commits, $(completed "$TARGET" commit) compaction(s)"
  [ "$(completed "$TARGET" commit)" -ge 1 ] && break
done

echo
echo "Completed instants on the timeline:"
ls "$TARGET/.hoodie/timeline" | grep '_' | sed 's/^/  /'
