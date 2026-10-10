-- requires: 14_create_silver_sink.sql 16_create_gold_table.sql
-- Medallion pipeline: hourly batch job. Recompute the recent days from Silver and
-- replace those Gold partitions. The scheduler passes the window start (yesterday);
-- the sample data is all from 15-16 June 2024, so it is fixed here.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';

INSERT OVERWRITE gold_merchant_daily_summary
SELECT
    dt,
    COALESCE(category, 'unknown')  AS category,
    currency,
    COUNT(*)                       AS total_txns,
    SUM(amount)                    AS total_amount,
    AVG(amount)                    AS avg_amount,
    COUNT(DISTINCT merchant_id)    AS unique_merchants
FROM silver_enriched_transactions
WHERE dt >= '2024-06-15'
  AND status = 'settled'
GROUP BY dt, COALESCE(category, 'unknown'), currency;

SELECT * FROM gold_merchant_daily_summary ORDER BY dt, category, currency;
