-- requires: 14_create_silver_sink.sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT transaction_id, merchant_id, business_name, category, status, dt
FROM silver_enriched_transactions
ORDER BY transaction_id;
