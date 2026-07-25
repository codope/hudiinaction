-- Section 7.5: Switch to EVENT_TIME_ORDERING with record_version
-- Changes from 07: added record_version column, merge mode -> EVENT_TIME,
-- ordering.fields = record_version, write.tasks scaled to 128

DROP TABLE IF EXISTS hudi_transactions;

CREATE TABLE hudi_transactions (
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
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'         = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'     = 'dt',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'hoodie.clean.failed.writes.policy'              = 'LAZY',
    'hoodie.write.lock.provider'                     = 'org.apache.hudi.client.transaction.lock.StorageBasedLockProvider',

    -- Changed: switch from COMMIT_TIME to EVENT_TIME ordering
    'hoodie.write.record.merge.mode'                  = 'EVENT_TIME_ORDERING',
    'ordering.fields'                                = 'record_version',

    -- Compaction and write tuning
    'compaction.schedule.enabled'                    = 'true',
    'compaction.async.enabled'                       = 'true',
    'compaction.delta_commits'                       = '5',
    'write.tasks'                                    = '128',
    'write.batch.size'                               = '128'
);
