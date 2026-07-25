-- Section 7.6: Hudi MoR sink for CDC merchant data
CREATE TABLE hudi_merchants (
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
    'connector'                              = 'hudi',
    'path'                                   = '/tmp/hudi/bronze/merchants',
    'table.type'                             = 'MERGE_ON_READ',

    -- Bucket index
    'hoodie.index.type'                      = 'BUCKET',
    'hoodie.bucket.index.num.buckets'        = '4',

    -- CDC-specific: preserve changelog semantics for downstream streaming consumers
    'changelog.enabled'                      = 'true',
    'hoodie.write.record.merge.mode'         = 'EVENT_TIME_ORDERING',
    'ordering.fields'                        = 'updated_at',

    -- Compaction
    'compaction.schedule.enabled'            = 'true',
    'compaction.async.enabled'               = 'true',
    'compaction.delta_commits'               = '5',

    -- Write tuning
    'write.tasks'                            = '4',
    'write.batch.size'                       = '64'
);
