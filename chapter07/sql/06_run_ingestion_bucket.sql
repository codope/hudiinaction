-- requires: 01_create_kafka_source.sql 05_switch_to_bucket_index.sql
-- Scaling writes with bucket index: same INSERT as 03, into the bucket-index table.
INSERT INTO hudi_transactions
SELECT
    transaction_id,
    event_type,
    merchant_id,
    customer_id,
    amount,
    currency,
    status,
    created_ts,
    event_ts,
    processing_ts,
    DATE_FORMAT(created_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions;
