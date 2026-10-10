-- requires: 07_enable_nbcc.sql
-- Both writers committed; TXN-0002 shows whichever version committed last
-- (still commit-time ordering).
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT COUNT(*) AS rows_total FROM hudi_transactions;
SELECT transaction_id, merchant_id, status FROM hudi_transactions WHERE transaction_id = 'TXN-0002';
