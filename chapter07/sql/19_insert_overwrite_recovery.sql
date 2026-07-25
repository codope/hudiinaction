-- Section 7.8: INSERT OVERWRITE to recover a corrupted partition
-- Assumes you have replayed the correct data into a staging table first.

-- Step 1: Create a staging table from replayed Kafka data (not shown here;
-- use the same schema as hudi_transactions pointing to a separate path).

-- Step 2: Replace the corrupted partition atomically
INSERT OVERWRITE hudi_transactions
PARTITION (dt = '2024-06-03')
SELECT
    transaction_id,
    event_type,
    merchant_id,
    customer_id,
    amount,
    currency,
    status,
    event_ts,
    processing_ts,
    '2024-06-03' AS dt
FROM staging_transactions_0603;
