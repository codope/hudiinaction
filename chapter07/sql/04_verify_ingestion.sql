-- Section 7.2: Verify ingestion landed in the Hudi table
SELECT status, COUNT(*) AS cnt
FROM hudi_transactions
WHERE dt = '2024-06-15'
GROUP BY status;
