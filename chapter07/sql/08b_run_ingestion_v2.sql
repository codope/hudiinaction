-- requires: 01_create_kafka_source.sql 07_enable_nbcc.sql 08_event_time_ordering.sql
-- Handling out-of-order events: live and backfill writers on the event-time table.
INSERT INTO hudi_transactions_v2
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       record_version, created_ts, event_ts, processing_ts, DATE_FORMAT(created_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions;

INSERT INTO hudi_transactions_v2_backfill
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       record_version, created_ts, event_ts, processing_ts, DATE_FORMAT(created_ts, 'yyyy-MM-dd') AS dt
FROM kafka_transactions_replay;
