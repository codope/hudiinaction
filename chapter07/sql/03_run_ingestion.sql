-- Section 7.2: Start ingestion pipeline
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
