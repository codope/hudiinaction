-- requires: 08_event_time_ordering.sql
-- The correction (record_version 3, merchant M004) wins regardless of commit order.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT transaction_id, merchant_id, status, record_version, dt
FROM hudi_transactions_v2
WHERE transaction_id IN ('TXN-0002', 'TXN-0004', 'TXN-0011')
ORDER BY transaction_id;
