-- requires: 10_create_hudi_merchants_sink.sql 13_create_incremental_sources.sql 14_create_silver_sink.sql
-- Medallion pipeline: enrich each transaction with the merchant record as it is
-- when the transaction is processed (lookup join against the bronze merchants
-- Hudi table). LEFT JOIN keeps transactions whose merchant has been deleted.
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
FROM bronze_transactions_incremental AS t
LEFT JOIN hudi_merchants FOR SYSTEM_TIME AS OF t.proc_time AS m
    ON t.merchant_id = m.merchant_id;
