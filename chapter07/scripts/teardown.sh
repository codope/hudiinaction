#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Chapter 7: Teardown ==="
cd "$CH_DIR"
docker compose down -v
echo "All services stopped and volumes removed."
