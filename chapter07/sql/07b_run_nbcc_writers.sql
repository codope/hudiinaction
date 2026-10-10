-- requires: 01_create_kafka_source.sql 07_enable_nbcc.sql
-- Running concurrent pipelines with NBCC: two independent jobs writing one table.
INSERT INTO hudi_transactions
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       created_ts, event_ts, processing_ts, DATE_FORMAT(created_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions;

INSERT INTO hudi_transactions_backfill
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       created_ts, event_ts, processing_ts, DATE_FORMAT(created_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions_replay;
