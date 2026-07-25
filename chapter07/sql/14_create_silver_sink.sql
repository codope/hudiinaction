-- Section 7.7: Silver layer — enriched transactions with merchant data
CREATE TABLE silver_enriched_transactions (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    business_name   STRING,
    category        STRING,
    fee_tier        STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    event_ts        TIMESTAMP(3),
    record_version  BIGINT,
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector'                              = 'hudi',
    'path'                                   = '/tmp/hudi/silver/enriched_transactions',
    'table.type'                             = 'MERGE_ON_READ',
    'hoodie.index.type'                      = 'BUCKET',
    'hoodie.bucket.index.num.buckets'        = '64',
    'hoodie.write.record.merge.mode'         = 'EVENT_TIME_ORDERING',
    'ordering.fields'                        = 'record_version',
    'compaction.schedule.enabled'            = 'true',
    'compaction.async.enabled'               = 'true',
    'compaction.delta_commits'               = '5',
    'write.tasks'                            = '64'
);
