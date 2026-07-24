-- Section 7.6: Start CDC replication pipeline
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
