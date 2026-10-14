-- Chapter 9, "Reading with Flink's Source V2" - batch read comparison: the legacy
-- InputFormat-based source (default, read.source-v2.enabled=false) against the opt-in
-- FLIP-27 Source V2 (read.source-v2.enabled=true, FlinkOptions.READ_SOURCE_V2_ENABLED at
-- release-1.2.0), using Flink SQL dynamic table options so both reads hit the same table
-- declaration. On a five-row local table this can only check correctness (same rows back),
-- not the resumable-split / stronger-pushdown behavior Source V2 adds at scale.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

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
    'compaction.schedule.enabled'                = 'false'
);

-- Default legacy source (read.source-v2.enabled defaults to false).
SELECT 'legacy_source' AS which, order_id, status, dt
FROM orders_rli /*+ OPTIONS('read.source-v2.enabled'='false') */
ORDER BY order_id;

-- Opt-in FLIP-27 Source V2.
SELECT 'source_v2' AS which, order_id, status, dt
FROM orders_rli /*+ OPTIONS('read.source-v2.enabled'='true') */
ORDER BY order_id;
