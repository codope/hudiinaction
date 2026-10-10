#!/usr/bin/env bash
# Run the whole chapter in order. Each stage starts its streaming jobs, waits for
# a few checkpoints, prints a verification query, and stops the jobs the next
# stage replaces. Expected output is listed in README.md.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "### Stage 1: streaming ingestion with FLINK_STATE index"
run_sql 03_run_ingestion.sql detached
wait_for_commits
run_sql 04_verify_ingestion.sql
cancel_jobs

echo "### Stage 2: bucket index"
run_sql 06_run_ingestion_bucket.sql detached
wait_for_commits
run_sql 06b_verify_bucket.sql
cancel_jobs

echo "### Stage 3: two writers with NBCC (live + backfill)"
run_sql 07b_run_nbcc_writers.sql detached
wait_for_commits
run_sql 07c_verify_nbcc.sql
cancel_jobs

echo "### Stage 4: event-time ordering on a new table"
run_sql 08b_run_ingestion_v2.sql detached
wait_for_commits
run_sql 08c_verify_event_time.sql
# The live writer keeps running for the medallion stages; the backfill is done.
cancel_jobs hudi_transactions_v2_backfill

echo "### Stage 5: MySQL CDC"
run_sql 11_run_cdc_pipeline.sql detached
wait_for_commits
run_sql 12_verify_cdc.sql

echo "### Stage 6: Silver (lookup join)"
run_sql 15_run_silver_pipeline.sql detached
wait_for_commits 105
run_sql 15b_verify_silver.sql

echo "### Stage 7: Gold (batch recompute)"
run_sql 17_run_gold_aggregation.sql

echo "### Stage 8: partition recovery with INSERT OVERWRITE"
# Pause the writers on hudi_transactions_v2 first, as the chapter does.
cancel_jobs silver_enriched_transactions
cancel_jobs hudi_transactions_v2
run_sql 19_insert_overwrite_recovery.sql

echo "### Done. The CDC job is still running; ./scripts/teardown.sh stops everything."
