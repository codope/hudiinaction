-- requires: 02_create_hudi_sink_flink_state.sql
-- Streaming event ingestion: verify (batch query against the latest snapshot).
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT dt, status, COUNT(*) AS cnt
FROM hudi_transactions
GROUP BY dt, status
ORDER BY dt, status;
