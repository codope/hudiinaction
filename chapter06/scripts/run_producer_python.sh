#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-normal}"
ITERATIONS="${2:-1000}"
FIXED="${3:-}"   # polluted mode only: pass "fixed" to send corrected values

python3 pyspark/payment_event_producer.py localhost:9092 payments.events "${MODE}" "${ITERATIONS}" ${FIXED}
