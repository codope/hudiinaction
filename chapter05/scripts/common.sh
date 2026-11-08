#!/usr/bin/env bash
# Shared setup for the HoodieStreamer and HoodieIndexer examples: downloads the Hudi bundles and
# prepares Chapter 3's NYC taxi sample as a stream of batches. Sourced, not run.

: "${SPARK_HOME:?Set SPARK_HOME to your Spark 3.5.x installation}"

HUDI_VERSION=${HUDI_VERSION:-1.2.1}
CH05_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(dirname "$CH05_DIR")"
WORK_DIR=${WORK_DIR:-/tmp/hudiinaction/chapter05}
BUNDLES=${BUNDLES:-/tmp/hudi_bundles}
DATA_DIR="$WORK_DIR/streamer_data/hudi_streamer"   # the archive extracts to hudi_streamer/

MAVEN=https://repo1.maven.org/maven2/org/apache/hudi
UTILITIES_JAR="$BUNDLES/hudi-utilities-slim-bundle_2.12-$HUDI_VERSION.jar"
SPARK_BUNDLE_JAR="$BUNDLES/hudi-spark3.5-bundle_2.12-$HUDI_VERSION.jar"

download_bundles() {
  mkdir -p "$BUNDLES"
  [ -f "$UTILITIES_JAR" ] || curl -fL -o "$UTILITIES_JAR" \
    "$MAVEN/hudi-utilities-slim-bundle_2.12/$HUDI_VERSION/$(basename "$UTILITIES_JAR")"
  [ -f "$SPARK_BUNDLE_JAR" ] || curl -fL -o "$SPARK_BUNDLE_JAR" \
    "$MAVEN/hudi-spark3.5-bundle_2.12/$HUDI_VERSION/$(basename "$SPARK_BUNDLE_JAR")"
}

# Extracts Chapter 3's sample (10 Parquet files plus schema) and gives each file its own
# modification time, because the DFS source reads files in that order and would otherwise put
# them all in the first batch. A copy of the first file is added as the 5th batch, so its rows
# arrive again as updates.
prepare_data() {
  rm -rf "$WORK_DIR/streamer_data"
  mkdir -p "$WORK_DIR/streamer_data"
  tar -xzf "$REPO_DIR/chapter03/hudi_streamer.tar.gz" -C "$WORK_DIR/streamer_data"
  cd "$DATA_DIR/streamer_input"
  cp part-00000-*.parquet replay-part-00000.parquet
  local i=1 f
  for f in part-0000[0-3]-*.parquet replay-part-00000.parquet part-0000[4-9]-*.parquet; do
    touch -t "2026010100$(printf '%02d' "$i")" "$f"
    i=$((i+1))
  done
  cd - > /dev/null
}

# Number of completed instants of an action, e.g. `completed "$TABLE" commit`. Completed
# instants are named <requested>_<completed>.<action> under .hoodie/timeline.
completed() {
  ls "$1/.hoodie/timeline" 2>/dev/null | grep -c "_.*\.$2\$" || true
}
