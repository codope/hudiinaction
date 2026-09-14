#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-/tmp/hudiinaction/chapter09}"

echo "This removes $WORK_DIR, including the source Parquet and the Hudi table."
read -r -p "Continue? [y/N] " reply
case "$reply" in
  [yY]) rm -rf "$WORK_DIR"; echo "Removed $WORK_DIR." ;;
  *)    echo "Left $WORK_DIR in place." ;;
esac
