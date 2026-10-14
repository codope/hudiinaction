-- Chapter 9 - Flink Record-Level Index: initial inserts across two partitions.
-- Flink's default catalog (GenericInMemoryCatalog) does not persist DDL across separate
-- sql-client invocations, so every script that touches orders_rli redeclares the same table
-- against the same 'path'; the Hudi connector picks up the existing table on disk.
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
    ('o1', 'c1', CAST(19.99 AS DECIMAL(10,2)), 'CREATED', 1000, '2026-09-25'),
    ('o2', 'c2', CAST(45.00 AS DECIMAL(10,2)), 'CREATED', 1001, '2026-09-25'),
    ('o3', 'c1', CAST(12.50 AS DECIMAL(10,2)), 'CREATED', 1002, '2026-09-26'),
    ('o4', 'c3', CAST(99.00 AS DECIMAL(10,2)), 'CREATED', 1003, '2026-09-26'),
    ('o5', 'c2', CAST(5.25  AS DECIMAL(10,2)), 'CREATED', 1004, '2026-09-26');
