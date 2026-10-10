-- Scaling writes with bucket index: same schema, BUCKET index with 128 buckets.
-- The index type is fixed when a table is created, so this is a new table at a
-- new path rather than a change to the FLINK_STATE table.
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
    'hoodie.write.record.merge.mode'                 = 'COMMIT_TIME_ORDERING',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'write.tasks'                                    = '2',
    'write.rate.limit'                               = '5000',
    'hoodie.metadata.enable'                         = 'true',

    -- Changed: switch from FLINK_STATE to BUCKET index
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128'
);
