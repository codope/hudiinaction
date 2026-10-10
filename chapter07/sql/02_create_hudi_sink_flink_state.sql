-- Streaming event ingestion: Hudi MoR sink with FLINK_STATE index (first version).
-- Compaction is not set: Flink schedules and runs it asynchronously by default for
-- MoR tables, every 5 delta commits.
-- write.tasks is 2 here so the local demo fits one TaskManager; the book uses 4.
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
    'path'                                           = '/tmp/hudi/bronze/transactions_flink_state',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.write.record.merge.mode'                 = 'COMMIT_TIME_ORDERING',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'write.tasks'                                    = '2',
    'write.rate.limit'                               = '5000',
    'hoodie.metadata.enable'                         = 'true',
    'hoodie.index.type'                              = 'FLINK_STATE'
);
