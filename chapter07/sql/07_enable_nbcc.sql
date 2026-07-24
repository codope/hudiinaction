-- Section 7.4: Enable Non-Blocking Concurrency Control
-- Changes from 05: added NBCC mode, LAZY cleaning, StorageBasedLockProvider

DROP TABLE IF EXISTS hudi_transactions;

CREATE TABLE hudi_transactions (
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
    'hoodie.datasource.write.partitionpath.field'     = 'dt',
    'write.tasks'                                    = '4',
    'write.rate.limit'                               = '5000',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',

    -- New: non-blocking concurrency control
    'hoodie.write.concurrency.mode'                  = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'hoodie.clean.failed.writes.policy'              = 'LAZY',
    'hoodie.write.lock.provider'                     = 'org.apache.hudi.client.transaction.lock.StorageBasedLockProvider',

    -- Ordering still defaults to commit time; we set an ordering field in 08
    'hoodie.write.record.merge.mode'                  = 'COMMIT_TIME_ORDERING',

    -- Compaction runs async inside the Flink job
    'compaction.schedule.enabled'                    = 'true',
    'compaction.async.enabled'                       = 'true',
    'compaction.delta_commits'                       = '5'
);
