-- Medallion pipeline: Gold daily summary (CoW), one partition per day.
-- Dynamic overwrite mode makes INSERT OVERWRITE replace only the days the query
-- returns. Without it, Hudi's Flink sink replaces the whole table.
CREATE TABLE gold_merchant_daily_summary (
    dt                STRING,
    category          STRING,
    currency          STRING,
    total_txns        BIGINT,
    total_amount      DECIMAL(18, 2),
    avg_amount        DECIMAL(18, 2),
    unique_merchants  BIGINT,
    PRIMARY KEY (dt, category, currency) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                              = 'hudi',
    'path'                                   = '/tmp/hudi/gold/merchant_daily_summary',
    'table.type'                             = 'COPY_ON_WRITE',
    'hoodie.index.type'                      = 'BUCKET',
    'hoodie.bucket.index.num.buckets'        = '2',
    'write.partition.overwrite.mode'         = 'dynamic',
    'write.tasks'                            = '2'
);
