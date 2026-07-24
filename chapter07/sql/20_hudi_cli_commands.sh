#!/usr/bin/env bash
# Section 7.8: Hudi CLI commands for timeline debugging
# Run these interactively inside a Hudi CLI session.
# Start the CLI: docker run -it --rm apachehudi/hudi-cli:1.2.0

set -euo pipefail

echo "=== Hudi CLI Commands for Timeline Debugging ==="
echo ""
echo "1. Connect to the table:"
echo "   hudi-cli> connect --path /tmp/hudi/bronze/transactions"
echo ""
echo "2. View recent commits:"
echo "   hudi-cli> commits show --limit 10"
echo ""
echo "3. Inspect file layout for a specific partition:"
echo "   hudi-cli> show fsview all --pathFilter dt=2024-06-15"
echo ""
echo "4. Check compaction status:"
echo "   hudi-cli> compactions show all"
echo ""
echo "For more: https://hudi.apache.org/docs/cli"
