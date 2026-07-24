-- Section 7.6: Flink CDC source table for MySQL merchants
CREATE TABLE mysql_merchants (
    merchant_id   STRING,
    business_name STRING,
    category      STRING,
    country_code  STRING,
    status        STRING,
    fee_tier      STRING,
    compliance_ok BOOLEAN,
    created_at    TIMESTAMP(3),
    updated_at    TIMESTAMP(3),
    PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
    'connector'      = 'mysql-cdc',
    'hostname'       = 'mysql',
    'port'           = '3306',
    'username'       = 'cdc_reader',
    'password'       = 'cdc_pass',
    'database-name'  = 'novapay',
    'table-name'     = 'merchants',
    'server-id'      = '5401-5404'
);
