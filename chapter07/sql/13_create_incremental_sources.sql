-- Medallion pipeline: streaming (incremental) read of the bronze transactions table.
-- proc_time is a processing-time attribute for the lookup join in 15.
CREATE TABLE bronze_transactions_incremental (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    record_version  BIGINT,
    created_ts      TIMESTAMP(3),
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector'                      = 'hudi',
    'path'                           = '/tmp/hudi/bronze/transactions_v2',
    'table.type'                     = 'MERGE_ON_READ',
    'read.streaming.enabled'         = 'true',
    'read.start-commit'              = 'earliest',
    'read.streaming.check-interval'  = '60'
);
