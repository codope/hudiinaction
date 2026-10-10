#!/usr/bin/env bash
# Debugging timeline issues with the Hudi CLI.
# Run these interactively inside a Hudi CLI session (hudi-cli from a Hudi
# installation; see https://hudi.apache.org/docs/cli).

set -euo pipefail

echo "=== Hudi CLI Commands for Timeline Debugging ==="
echo ""
echo "1. Connect to the table:"
echo "   hudi-cli> connect --path /tmp/hudi/bronze/transactions_v2"
echo ""
echo "2. View the 10 most recent instants:"
echo "   hudi-cli> timeline show active --limit 10"
echo ""
echo "3. Inspect file layout for a specific partition:"
echo "   hudi-cli> show fsview all --pathRegex dt=2024-06-15"
echo ""
echo "4. Check compaction status:"
echo "   hudi-cli> compactions show all"
echo ""
echo "For more: https://hudi.apache.org/docs/cli"
