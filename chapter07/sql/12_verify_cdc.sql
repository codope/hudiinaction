-- Section 7.6: Verify CDC replication
SELECT category, COUNT(*) AS cnt
FROM hudi_merchants
GROUP BY category;
