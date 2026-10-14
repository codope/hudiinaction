-- Chapter 9, "Reading with Flink's Source V2" - streaming read via read.source-v2.enabled=true,
-- writing to a filesystem sink so the streamed rows are visible as files after the job is
-- cancelled (sql-client's own stdout for an unbounded SELECT would otherwise stream forever
-- inside the same process running this script).
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '5s';

CREATE TABLE IF NOT EXISTS orders_rli (
    order_id     STRING,
    customer_id  STRING,
    amount       DECIMAL(10, 2),
    status       STRING,
    ts           BIGINT,
    dt           STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                  = 'hudi',
    'path'                                       = 'file:///tmp/hudiinaction/chapter09/flink_rli/orders',
    'table.type'                                 = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'    = 'order_id',
    'hoodie.datasource.write.partitionpath.field' = 'dt',
    'ordering.fields'                             = 'ts',
    'write.tasks'                                = '2',
    'metadata.enabled'                           = 'true',
    'index.type'                                 = 'RECORD_LEVEL_INDEX',
    'compaction.async.enabled'                   = 'false',
    'compaction.schedule.enabled'                = 'false',
    'read.streaming.enabled'                     = 'true',
    'read.streaming.check-interval'              = '4',
    'read.source-v2.enabled'                     = 'true',
    'read.start-commit'                          = 'earliest'
);

CREATE TABLE IF NOT EXISTS orders_rli_stream_sink (
    order_id     STRING,
    customer_id  STRING,
    amount       DECIMAL(10, 2),
    status       STRING,
    ts           BIGINT,
    dt           STRING
) WITH (
    'connector' = 'filesystem',
    'path'      = 'file:///tmp/hudiinaction/chapter09/flink_rli/stream_sink_v2',
    'format'    = 'csv'
);

INSERT INTO orders_rli_stream_sink
SELECT order_id, customer_id, amount, status, ts, dt FROM orders_rli;
