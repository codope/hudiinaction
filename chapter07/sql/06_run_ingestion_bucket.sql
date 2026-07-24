-- Section 7.3: Re-run ingestion with bucket index
-- Same INSERT INTO query as 03, the table config has changed underneath
INSERT INTO hudi_transactions
SELECT
    transaction_id,
    event_type,
    merchant_id,
    customer_id,
    amount,
    currency,
    status,
    event_ts,
    processing_ts,
    DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions;
