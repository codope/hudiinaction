-- Chapter 4: querying the store_sales Hudi table with Spark SQL.
-- Prerequisite: hudi_querying_tutorial.scala has created /tmp/hudi_demo_tbl.
-- Run with the spark-sql launch command from README.md:
--   spark-sql ... -f chapter04/spark_sql_queries.sql

DROP TABLE IF EXISTS hudi_tbl;

-- Map a catalog table onto the existing Hudi table location.
CREATE TABLE hudi_tbl USING hudi LOCATION '/tmp/hudi_demo_tbl';

-- Snapshot query (the default query type).
SELECT count(*) FROM hudi_tbl WHERE ss_store_sk = 7;
-- 458194

-- Commits on the timeline: commit_time is the instant stamped into _hoodie_commit_time,
-- state_transition_time is when the commit completed.
CALL show_commits(table => 'hudi_tbl');

-- Time travel to the table as of a commit (replace with a commit_time from show_commits).
-- SELECT count(*) FROM hudi_tbl TIMESTAMP AS OF '20261012152336309';

-- Latest state of every row changed since the start of the timeline.
-- Replace 'earliest' with a state_transition_time to read only later changes.
SELECT count(*) FROM hudi_table_changes('hudi_tbl', 'latest_state', 'earliest');
