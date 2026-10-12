#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-normal}"
ITERATIONS="${2:-1000}"
FIXED="${3:-}"   # polluted mode only: pass "fixed" to send corrected values

sbt "runMain com.hudiinaction.chapter06.producer.PaymentEventProducer localhost:9092 payments.events ${MODE} ${ITERATIONS} ${FIXED}"