-- Chapter 9 - Flink Record-Level Index: upserts that must route to the existing file group
-- for o1 and o3 (status/amount change, same key, same partition) rather than landing as new
-- records, plus one brand-new key (o6) to confirm inserts still work alongside upserts.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '2s';

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

INSERT INTO orders_rli VALUES
    ('o1', 'c1', CAST(19.99 AS DECIMAL(10,2)), 'SHIPPED',   2000, '2026-09-25'),
    ('o3', 'c1', CAST(12.50 AS DECIMAL(10,2)), 'CANCELLED', 2001, '2026-09-26'),
    ('o6', 'c4', CAST(7.00  AS DECIMAL(10,2)), 'CREATED',   2002, '2026-09-26');
