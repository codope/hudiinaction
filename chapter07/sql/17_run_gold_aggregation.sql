-- Section 7.7: Gold aggregation — recompute affected day-partitions
INSERT INTO gold_merchant_daily_summary
SELECT
    dt,
    category,
    COUNT(*)        AS total_txns,
    SUM(amount)     AS total_amount,
    AVG(amount)     AS avg_amount,
    COUNT(DISTINCT merchant_id) AS unique_merchants
FROM silver_enriched_transactions
WHERE dt = '2024-06-15'
  AND status = 'settled'
GROUP BY dt, category;
