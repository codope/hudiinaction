-- Section 7.7: Incremental source tables for the Bronze layer
CREATE TABLE bronze_transactions_incremental (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    record_version  BIGINT,
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING
) WITH (
    'connector'                      = 'hudi',
    'path'                           = '/tmp/hudi/bronze/transactions',
    'table.type'                     = 'MERGE_ON_READ',
    'read.streaming.enabled'         = 'true',
    'read.start-commit'              = 'earliest',
    'read.streaming.check-interval'  = '60'
);

CREATE TABLE bronze_merchants_incremental (
    merchant_id   STRING,
    business_name STRING,
    category      STRING,
    country_code  STRING,
    status        STRING,
    fee_tier      STRING,
    compliance_ok BOOLEAN,
    created_at    TIMESTAMP(3),
    updated_at    TIMESTAMP(3),
    PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
    'connector'                      = 'hudi',
    'path'                           = '/tmp/hudi/bronze/merchants',
    'table.type'                     = 'MERGE_ON_READ',
    'read.streaming.enabled'         = 'true',
    'read.start-commit'              = 'earliest',
    'read.streaming.check-interval'  = '60'
);
