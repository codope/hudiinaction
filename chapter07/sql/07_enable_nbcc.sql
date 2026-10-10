-- Running concurrent pipelines with NBCC: the bucket-index table from 05 with
-- non-blocking concurrency control, declared twice, once per writer.
--
-- Locks: with no lock provider set, Flink uses a file-system lock on the table
-- path, which is what this local demo relies on. On S3, the book sets
--   'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.StorageBasedLockProvider'
-- and adds the hudi-aws bundle to the Flink classpath.
-- LAZY cleaning of failed writes is inferred for multi-writer modes; setting
-- 'hoodie.clean.failed.writes.policy' = 'LAZY' explicitly is equivalent.

-- Live writer: owns compaction and cleaning (Flink defaults).
CREATE TABLE hudi_transactions (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    created_ts      TIMESTAMP(3),
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'write.tasks'                                    = '2',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.record.merge.mode'                 = 'COMMIT_TIME_ORDERING',

    -- New: non-blocking concurrency control
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL'
);

-- Backfill writer: same table, table services switched off.
CREATE TABLE hudi_transactions_backfill (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    created_ts      TIMESTAMP(3),
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'write.tasks'                                    = '2',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.record.merge.mode'                 = 'COMMIT_TIME_ORDERING',
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'compaction.schedule.enabled'                    = 'false',
    'compaction.async.enabled'                       = 'false',
    'clean.async.enabled'                            = 'false'
);

-- Corrected events for the backfill
CREATE TABLE kafka_transactions_replay (
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
    processing_ts   TIMESTAMP(3)
) WITH (
    'connector'                          = 'kafka',
    'topic'                              = 'payments.transactions.replay',
    'properties.bootstrap.servers'       = 'kafka:29092',
    'properties.group.id'                = 'hudi-flink-backfill',
    'scan.startup.mode'                  = 'earliest-offset',
    'format'                             = 'json',
    'json.timestamp-format.standard'     = 'ISO-8601'
);
