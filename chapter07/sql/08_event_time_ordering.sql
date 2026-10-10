-- Handling out-of-order events: merge mode is fixed when a table is created, so
-- event-time ordering means a new table at a new path, reloaded from the source
-- topics. (Dropping or re-declaring a Flink SQL table does not change the Hudi
-- table stored at its path.)
CREATE TABLE hudi_transactions_v2 (
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
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions_v2',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL',

    -- Changed: event-time ordering on record_version
    'hoodie.write.record.merge.mode'                 = 'EVENT_TIME_ORDERING',
    'ordering.fields'                                = 'record_version',

    'write.tasks'                                    = '2',
    'write.batch.size'                               = '128'
);

-- Backfill writer for the new table (table services off)
CREATE TABLE hudi_transactions_v2_backfill (
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
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions_v2',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'hoodie.write.record.merge.mode'                 = 'EVENT_TIME_ORDERING',
    'ordering.fields'                                = 'record_version',
    'write.tasks'                                    = '2',
    'compaction.schedule.enabled'                    = 'false',
    'compaction.async.enabled'                       = 'false',
    'clean.async.enabled'                            = 'false'
);
