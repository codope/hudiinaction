-- requires: 10_create_hudi_merchants_sink.sql
-- Replicating MySQL with Flink CDC: verify (batch snapshot query).
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT category, COUNT(*) AS cnt
FROM hudi_merchants
GROUP BY category
ORDER BY category;
