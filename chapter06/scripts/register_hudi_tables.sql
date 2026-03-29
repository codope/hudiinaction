CREATE TABLE IF NOT EXISTS hudi_payments_raw
USING hudi
LOCATION '/tmp/hudi/raw/payments';

CREATE TABLE IF NOT EXISTS hudi_vendor_payouts
USING hudi
LOCATION '/tmp/hudi/silver/vendor_payouts';
