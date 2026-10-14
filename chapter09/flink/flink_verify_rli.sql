-- Chapter 9 - Flink Record-Level Index: verify the upsert routed correctly (6 distinct keys,
-- o1/o3 updated in place, no duplicates).
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

SELECT order_id, customer_id, amount, status, ts, dt FROM orders_rli ORDER BY order_id;

SELECT COUNT(*) AS total_rows, COUNT(DISTINCT order_id) AS distinct_keys FROM orders_rli;
