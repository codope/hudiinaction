-- requires: 01_create_kafka_source.sql
-- Managing streaming tables in production: replace one corrupted partition.
-- Stop the live and backfill writers for hudi_transactions_v2 before running this.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';

-- The recovery job declares hudi_transactions_v2 without NBCC. The writers are
-- paused, so single-writer mode is correct, and in Hudi 1.2 an INSERT OVERWRITE
-- under NBCC leaves the partition reading as empty: NBCC gives each bucket a fixed
-- file ID, and the new files reuse the ID that the overwrite marks as replaced.
CREATE TABLE hudi_transactions_v2 (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    record_version  BIGINT,
    created_ts      TIMESTAMP(3),
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    dt              STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'connector'                                      = 'hudi',
    'path'                                           = '/tmp/hudi/bronze/transactions_v2',
    'table.type'                                     = 'MERGE_ON_READ',
    'hoodie.datasource.write.recordkey.field'        = 'transaction_id',
    'hoodie.datasource.write.partitionpath.field'    = 'dt',
    'hoodie.index.type'                              = 'BUCKET',
    'hoodie.bucket.index.num.buckets'                = '128',
    'hoodie.write.record.merge.mode'                 = 'EVENT_TIME_ORDERING',
    'ordering.fields'                                = 'record_version',
    'write.tasks'                                    = '2'
);

-- Step 1: replay the day from Kafka into a staging Hudi table
CREATE TABLE staging_transactions (
    transaction_id  STRING,
    event_type      STRING,
    merchant_id     STRING,
    customer_id     STRING,
    amount          DECIMAL(18, 2),
    currency        STRING,
    status          STRING,
    record_version  BIGINT,
    created_ts      TIMESTAMP(3),
    event_ts        TIMESTAMP(3),
    processing_ts   TIMESTAMP(3),
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector'                       = 'hudi',
    'path'                            = '/tmp/hudi/staging/transactions_20240615_v2',
    'table.type'                      = 'COPY_ON_WRITE',
    'hoodie.write.record.merge.mode'  = 'EVENT_TIME_ORDERING',
    'ordering.fields'                 = 'record_version'
);

INSERT INTO staging_transactions
-- (CAST drops the time attribute from the source watermark; a batch insert cannot carry it.)
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       record_version, created_ts, CAST(event_ts AS TIMESTAMP(3)), processing_ts
FROM kafka_transactions /*+ OPTIONS('scan.bounded.mode' = 'latest-offset') */
WHERE DATE_FORMAT(created_ts, 'yyyy-MM-dd') = '2024-06-15';

-- Step 2: verify the staging data
SELECT COUNT(*) AS staged_rows, SUM(amount) AS staged_amount FROM staging_transactions;

-- Step 3: replace the partition. dt is set by the PARTITION clause, so the SELECT
-- lists every column except dt.
INSERT OVERWRITE hudi_transactions_v2
PARTITION (dt = '2024-06-15')
SELECT transaction_id, event_type, merchant_id, customer_id, amount, currency, status,
       record_version, created_ts, event_ts, processing_ts
FROM staging_transactions;

SELECT dt, COUNT(*) AS rows_in_partition FROM hudi_transactions_v2 GROUP BY dt ORDER BY dt;
