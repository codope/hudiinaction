CREATE DATABASE IF NOT EXISTS novapay;
USE novapay;

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   VARCHAR(36) PRIMARY KEY,
    business_name VARCHAR(200) NOT NULL,
    category      VARCHAR(50)  NOT NULL,
    country_code  VARCHAR(5)   NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'active',
    fee_tier      VARCHAR(20)  NOT NULL DEFAULT 'standard',
    compliance_ok BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO merchants (merchant_id, business_name, category, country_code, status, fee_tier, compliance_ok) VALUES
('M001', 'QuickBites',        'food_delivery',   'US', 'active',   'premium',  TRUE),
('M002', 'UrbanThreads',      'retail',          'US', 'active',   'standard', TRUE),
('M003', 'CloudKitchen Co',   'food_delivery',   'UK', 'active',   'premium',  TRUE),
('M004', 'TechGadgets Plus',  'electronics',     'US', 'active',   'standard', TRUE),
('M005', 'FreshMart Online',  'grocery',         'CA', 'active',   'premium',  TRUE),
('M006', 'StyleHub',          'retail',          'US', 'suspended', 'standard', FALSE),
('M007', 'PayEasy Services',  'financial',       'IN', 'active',   'enterprise', TRUE),
('M008', 'GreenGrocer',       'grocery',         'UK', 'active',   'standard', TRUE),
('M009', 'BookWorm Digital',  'media',           'US', 'active',   'standard', TRUE),
('M010', 'TestMerchant',      'retail',          'US', 'test',     'standard', TRUE);

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%';
FLUSH PRIVILEGES;
