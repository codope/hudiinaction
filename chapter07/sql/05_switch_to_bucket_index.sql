-- Section 7.3: Switch hudi_transactions to BUCKET index
-- Changes from 02: FLINK_STATE -> BUCKET, added num.buckets=128,
-- enabled async compaction

DROP TABLE IF EXISTS hudi_transactions;

CREATE TABLE hudi_transactions (
    -- schema and columns unchanged from Section 7.2
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'         = 'transaction_id',
    'hoodie.write.record.merge.mode'                  = 'COMMIT_TIME_ORDERING',
    'hoodie.datasource.write.partitionpath.field'     = 'dt',
    'write.tasks'                                    = '4',
    'write.rate.limit'                               = '5000',

    -- Changed: switch from FLINK_STATE to BUCKET index
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',

    -- Compaction now enabled (async, every 5 delta commits)
    'compaction.schedule.enabled'                    = 'true',
    'compaction.async.enabled'                       = 'true',
    'hoodie.metadata.enable'                         = 'true'
);
