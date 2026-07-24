-- Section 7.2: Hudi MoR sink with FLINK_STATE index (first version)
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
    'hoodie.write.record.merge.mode'                  = 'COMMIT_TIME_ORDERING',
    'hoodie.datasource.write.partitionpath.field'     = 'dt',
    'write.tasks'                                    = '4',
    'write.rate.limit'                               = '5000',
    'compaction.async.enabled'                       = 'false',
    'compaction.schedule.enabled'                    = 'false',
    'hoodie.metadata.enable'                         = 'true',
    'hoodie.index.type'                              = 'FLINK_STATE'
);
