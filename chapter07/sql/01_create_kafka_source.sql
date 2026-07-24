-- Section 7.2: Kafka source table for NovaPay transaction events
CREATE TABLE kafka_transactions (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
    'connector'                          = 'kafka',
    'topic'                              = 'payments.transactions',
    'properties.bootstrap.servers'       = 'kafka:29092',
    'properties.group.id'                = 'hudi-flink-ingest',
    'scan.startup.mode'                  = 'earliest-offset',
    'format'                             = 'json',
    'json.timestamp-format.standard'     = 'ISO-8601'
);
