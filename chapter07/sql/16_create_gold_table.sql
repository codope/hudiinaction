-- Section 7.7: Gold layer — daily merchant summary (CoW)
CREATE TABLE gold_merchant_daily_summary (
    dt                STRING,
    category          STRING,
    total_txns        BIGINT,
    total_amount      DECIMAL(18, 2),
    avg_amount        DECIMAL(18, 2),
    unique_merchants  BIGINT,
    PRIMARY KEY (dt, category) NOT ENFORCED
) WITH (
    'connector'                              = 'hudi',
    'path'                                   = '/tmp/hudi/gold/merchant_daily_summary',
    'table.type'                             = 'COPY_ON_WRITE',
    'hoodie.index.type'                      = 'BUCKET',
    'hoodie.bucket.index.num.buckets'        = '2',
    'hoodie.write.record.merge.mode'         = 'COMMIT_TIME_ORDERING',
    'write.tasks'                            = '2'
);
