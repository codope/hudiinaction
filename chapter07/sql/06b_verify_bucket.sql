-- requires: 05_switch_to_bucket_index.sql
-- Scaling writes with bucket index: one row per transaction, including TXN-0011,
-- which was authorized before midnight and settled after it.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT COUNT(*) AS rows_total, COUNT(DISTINCT transaction_id) AS transactions FROM hudi_transactions;
SELECT transaction_id, status, dt FROM hudi_transactions WHERE transaction_id = 'TXN-0011';
