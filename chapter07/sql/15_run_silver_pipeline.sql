-- Section 7.7: Silver pipeline — join transactions with merchants
INSERT INTO silver_enriched_transactions
SELECT
    t.transaction_id,
    t.event_type,
    t.merchant_id,
    m.business_name,
    m.category,
    m.fee_tier,
    t.customer_id,
    t.amount,
    t.currency,
    t.status,
    t.event_ts,
    t.record_version,
    t.dt
FROM bronze_transactions_incremental t
JOIN bronze_merchants_incremental m
    ON t.merchant_id = m.merchant_id;
