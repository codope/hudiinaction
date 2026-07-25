-- Section 7.8: Partition TTL configuration for bronze transactions
-- These properties can be added to the CREATE TABLE WITH clause or set via ALTER TABLE.
-- Hudi drops partitions whose date-based path exceeds the retention window.

-- Example: add to hudi_transactions WITH clause
-- 'hoodie.partition.ttl.inline'                       = 'true',
-- 'hoodie.partition.ttl.management.strategy.type'     = 'KEEP_BY_TIME',
-- 'hoodie.partition.ttl.strategy.days.retain'         = '180'

-- For the CDC merchants table (not date-partitioned), use cleaner-based retention:
-- 'hoodie.clean.policy'           = 'KEEP_LATEST_COMMITS',
-- 'hoodie.clean.commits.retained' = '3'
