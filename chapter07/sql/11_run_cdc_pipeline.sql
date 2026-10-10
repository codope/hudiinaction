-- requires: 09_create_mysql_cdc_source.sql 10_create_hudi_merchants_sink.sql
-- Replicating MySQL with Flink CDC: start replication
INSERT INTO hudi_merchants
SELECT
    merchant_id,
    business_name,
    category,
    country_code,
    status,
    fee_tier,
    compliance_ok,
    created_at,
    updated_at
FROM mysql_merchants;
