#!/usr/bin/env bash
#
# Chapter 5, "Asynchronous indexing": HoodieStreamer keeps ingesting into a Copy-on-Write table
# while HoodieIndexer builds a record index in two phases, schedule and execute.
#
# Both processes take the same file-system lock (streamer/write.properties and
# streamer/indexer.properties), which is safe on local disk; the chapter's production setup uses
# ZooKeeper.
#
# Requirements: Java 11 or 17, Spark 3.5.x (SPARK_HOME set), network access to Maven Central.
# Usage: ./run_async_indexing.sh

set -euo pipefail
source "$(dirname "$0")/common.sh"

TARGET_DIR="$WORK_DIR/async_indexing/ny_hudi_tbl"
TARGET="file://$TARGET_DIR"            # the indexer must use the same base path string
LOG="$WORK_DIR/async_indexing_streamer.log"

download_bundles
prepare_data
rm -rf "$TARGET_DIR"
mkdir -p "$(dirname "$TARGET_DIR")"

wait_for_commits() {   # wait_for_commits <count>; gives up after 5 minutes (source exhausted)
  local waited=0
  until [ "$(completed "$TARGET_DIR" commit)" -ge "$1" ]; do
    kill -0 "$STREAMER_PID" 2>/dev/null || { echo "HoodieStreamer exited early; see $LOG"; exit 1; }
    [ "$waited" -ge 300 ] && { echo "No new commit within 5 minutes; the source may be exhausted."; return; }
    sleep 5
    waited=$((waited + 5))
  done
}

echo "Starting HoodieStreamer (log: $LOG)"
"$SPARK_HOME/bin/spark-submit" \
  --driver-memory 4g \
  --jars "$SPARK_BUNDLE_JAR" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$UTILITIES_JAR" \
  --props "file://$CH05_DIR/streamer/write.properties" \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --source-ordering-fields tpep_dropoff_datetime \
  --table-type COPY_ON_WRITE \
  --target-base-path "$TARGET" \
  --target-table ny_hudi_tbl \
  --op UPSERT \
  --continuous \
  --source-limit 30000000 \
  --min-sync-interval-seconds 30 \
  --hoodie-conf "hoodie.streamer.source.dfs.root=file://$DATA_DIR/streamer_input" \
  --hoodie-conf "hoodie.streamer.schemaprovider.source.schema.file=file://$DATA_DIR/streamer_schema/ny.avsc" \
  > "$LOG" 2>&1 &
STREAMER_PID=$!
trap 'kill $STREAMER_PID 2>/dev/null || true' EXIT

wait_for_commits 2
echo "Streamer has completed $(completed "$TARGET_DIR" commit) commits; it keeps running."

run_indexer() {   # run_indexer schedule|execute
  echo
  echo "=== HoodieIndexer --mode $1 ==="
  "$SPARK_HOME/bin/spark-submit" \
    --jars "$SPARK_BUNDLE_JAR" \
    --class org.apache.hudi.utilities.HoodieIndexer \
    "$UTILITIES_JAR" \
    --props "file://$CH05_DIR/streamer/indexer.properties" \
    --mode "$1" \
    --base-path "$TARGET" \
    --table-name ny_hudi_tbl \
    --index-types RECORD_INDEX \
    --parallelism 2 \
    --spark-memory 1g \
    2>&1 | grep -E 'HoodieIndexer|Exception|ERROR' || true
}

run_indexer schedule
echo "Indexing plans on the timeline: $(ls "$TARGET_DIR/.hoodie/timeline" | grep 'indexing' | tr '\n' ' ')"
run_indexer execute

# One more streamer commit after the index exists, to show ingestion carries on and keeps it.
committed=$(completed "$TARGET_DIR" commit)
wait_for_commits $((committed + 1))

echo
echo "Streamer commits: $(completed "$TARGET_DIR" commit), completed indexing: $(completed "$TARGET_DIR" indexing)"
echo "Metadata partitions: $(grep '^hoodie.table.metadata.partitions=' "$TARGET_DIR/.hoodie/hoodie.properties" | cut -d= -f2)"
