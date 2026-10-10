-- requires: 01_create_kafka_source.sql 02_create_hudi_sink_flink_state.sql
-- Streaming event ingestion: start the pipeline.
-- dt comes from created_ts, not event_ts, so every event of a transaction lands in
-- the same partition (a bucket index only looks for a key inside one partition).
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
