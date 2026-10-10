-- Replicating MySQL with Flink CDC: Hudi MoR sink for merchant data (not partitioned).
-- Updates and deletes are applied either way; changelog.enabled additionally keeps
-- intermediate changes in the log files for streaming readers (best effort: compaction
-- merges them).
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
    'write.tasks'                            = '2',
    'write.batch.size'                       = '64'
);
