#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"$SCRIPT_DIR/generate_source_data.sh"
"$SCRIPT_DIR/run_bootstrap.sh"
"$SCRIPT_DIR/verify_bootstrap.sh"
"$SCRIPT_DIR/show_layout.sh"
