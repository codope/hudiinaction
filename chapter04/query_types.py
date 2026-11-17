"""
Chapter 4: Hudi query types on the store_sales table, PySpark version of query_types.scala.
  snapshot, read-optimized, time travel, incremental (latest_state), and CDC.

Prerequisite: run hudi_querying_tutorial.py first (it creates /tmp/hudi_demo_tbl). Unlike the
Scala version, this script does not depend on variables from the tutorial's session, so it can
run as its own spark-submit job:
  spark-submit <options from README.md> /path/to/hudiinaction/chapter04/query_types.py

The commit times printed below come from one run; yours will differ.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, expr, rand

spark = SparkSession.builder.getOrCreate()

input_path = "/tmp/input_data"
base_path = "/tmp/hudi_demo_tbl"

# The same input and write options as the tutorial.
raw = spark.read.parquet(input_path)
df = raw.drop(*[c for c in raw.columns if c.startswith("_hoodie_")])
write_opts = {
    "hoodie.table.name": "hudi_demo_tbl",
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.datasource.write.operation": "bulk_insert",
}

# Register the table in the session catalog (this replaces the hudi_tbl temporary view from
# the tutorial). CALL procedures and hudi_table_changes look tables up by name.
spark.sql("DROP TABLE IF EXISTS hudi_tbl")
spark.sql(f"CREATE TABLE hudi_tbl USING hudi LOCATION '{base_path}'")

# ============================================================================
# Snapshot and read-optimized queries
# ============================================================================

(spark.read.format("hudi")
   .load(base_path)
   .where("ss_item_sk BETWEEN 1000 AND 2000")
   .orderBy("ss_item_sk")
   .show(truncate=False))

# On a Copy-on-Write table, read_optimized returns the same rows as a snapshot query.
(spark.read.format("hudi")
   .option("hoodie.datasource.query.type", "read_optimized")
   .load(base_path)
   .where("ss_item_sk BETWEEN 1000 AND 2000")
   .orderBy("ss_item_sk")
   .show(truncate=False))

# ============================================================================
# Time travel
# ============================================================================

# commit_time is the instant Hudi stamps into _hoodie_commit_time;
# state_transition_time is when that commit completed.
(spark.sql("CALL show_commits(table => 'hudi_tbl')")
   .select("commit_time", "state_transition_time", "action").show(truncate=False))
# +-----------------+---------------------+------+
# |commit_time      |state_transition_time|action|
# +-----------------+---------------------+------+
# |20261117112834681|20261117112843483    |commit|
# +-----------------+---------------------+------+

load_commit = spark.sql("CALL show_commits(table => 'hudi_tbl')").first()["commit_time"]
df_as_of = (spark.read.format("hudi")
            .option("as.of.instant", load_commit)
            .load(base_path))
df_as_of.select("ss_item_sk", "ss_net_paid", "ss_net_profit").show(truncate=False)

# ============================================================================
# Incremental queries
# ============================================================================

# 1. Upsert 50 rows with a changed ss_net_paid. Upserts write whole rows: dropping columns
#    from the DataFrame fails with MissingSchemaFieldException.
new_sales_batch = (spark.read.format("hudi").load(base_path)
                   .orderBy(rand()).limit(50)
                   .withColumn("ss_net_paid", col("ss_net_paid") + expr("cast(0.25 as decimal(7,2))")))
upsert_opts = {**write_opts, "hoodie.datasource.write.operation": "upsert"}
new_sales_batch.write.format("hudi").options(**upsert_opts).mode("append").save(base_path)

# 2. Read only the changes made after the initial load. In Hudi 1.x the begin value is a
#    completion time (state_transition_time), and it is exclusive: commits that completed
#    after it are returned. Passing the load's commit_time instead returns the whole table,
#    because the load completed after its own commit_time.
begin = (spark.sql("CALL show_commits(table => 'hudi_tbl')")
         .orderBy("commit_time").first()["state_transition_time"])
incr = (spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", begin)
        .load(base_path))
incr.select("_hoodie_commit_time", "ss_item_sk", "ss_ticket_number", "ss_net_paid").show(10, truncate=False)
print("Incremental rows: " + str(incr.count()))   # 50

# 3. The same through the hudi_table_changes table-valued function. latest_state returns the
#    table's own columns, one latest row per changed key.
spark.sql(f"""
  SELECT _hoodie_commit_time, ss_item_sk, ss_ticket_number, ss_net_paid
  FROM hudi_table_changes('hudi_tbl', 'latest_state', '{begin}')
  ORDER BY _hoodie_commit_time DESC
  LIMIT 20""").show(truncate=False)

# ============================================================================
# Change Data Capture (CDC)
# ============================================================================

# 1. Create a CDC-enabled Merge-on-Read table from the same data.
mor_path = "file:///tmp/hudi/demo/store_sales_mor"
mor_table = "store_sales_mor"
mor_opts = {
    "hoodie.table.name": mor_table,
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "ss_item_sk,ss_ticket_number",
    "hoodie.table.cdc.enabled": "true",
    # DATA_BEFORE_AFTER stores both images; OP_KEY_ONLY minimizes storage
    "hoodie.table.cdc.supplemental.logging.mode": "DATA_BEFORE_AFTER",
}
df.write.format("hudi").options(**mor_opts).mode("overwrite").save(mor_path)
spark.sql(f"DROP TABLE IF EXISTS {mor_table}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {mor_table} USING hudi LOCATION '{mor_path}'")

# 2a. Update 30 rows.
updates = (spark.read.format("hudi").load(mor_path)
           .orderBy(rand()).limit(30)
           .withColumn("ss_quantity", col("ss_quantity") + 1))
(updates.write.format("hudi")
   .options(**{**mor_opts, "hoodie.datasource.write.operation": "upsert"})
   .mode("append").save(mor_path))

# 2b. Delete 5 rows. A delete needs the full record key.
del_keys = (spark.read.format("hudi").load(mor_path)
            .select("ss_item_sk", "ss_ticket_number").orderBy(rand()).limit(5))
(del_keys.write.format("hudi")
   .options(**{**mor_opts, "hoodie.datasource.write.operation": "delete"})
   .mode("append").save(mor_path))

# 3. CDC through SQL. before and after are JSON strings; deletes have no after image, so the
#    key is taken from whichever image exists.
#    Note: in Hudi 1.2.1, DECIMAL columns are not serialized correctly in these JSON images,
#    which is why this example changes the INT column ss_quantity.
spark.sql("""
  SELECT ts_ms AS commit_time,
         op,
         get_json_object(before, '$.ss_quantity') AS qty_before,
         get_json_object(after, '$.ss_quantity')  AS qty_after,
         coalesce(get_json_object(after, '$.ss_item_sk'),
                  get_json_object(before, '$.ss_item_sk')) AS ss_item_sk,
         coalesce(get_json_object(after, '$.ss_ticket_number'),
                  get_json_object(before, '$.ss_ticket_number')) AS ss_ticket_number
  FROM hudi_table_changes('store_sales_mor', 'cdc', 'earliest')
  ORDER BY commit_time DESC
  LIMIT 20""").show(truncate=False)
# +-----------------+---+----------+---------+----------+----------------+
# |commit_time      |op |qty_before|qty_after|ss_item_sk|ss_ticket_number|
# +-----------------+---+----------+---------+----------+----------------+
# |20261117113042377|d  |8         |NULL     |16309     |134652          |
# |20261117113042377|d  |20        |NULL     |14596     |22135           |
# ...
# |20261117113039508|u  |80        |81       |3846      |129542          |
# |20261117113039508|u  |16        |17       |5371      |28333           |
# ...
# Over the whole timeline: i = 2,880,404 (the initial load), u = 30, d = 5.

# 4. CDC through the DataFrame API, starting after the initial load.
#    (In Hudi 1.2.1 the DataFrame CDC reader returns no rows for begin = "earliest";
#    the SQL function above handles "earliest" correctly.)
cdc_begin = (spark.sql(f"CALL show_commits(table => '{mor_table}')")
             .orderBy("commit_time").first()["state_transition_time"])
cdc = (spark.read.format("hudi")
       .option("hoodie.datasource.query.type", "incremental")
       .option("hoodie.datasource.query.incremental.format", "cdc")
       .option("hoodie.datasource.read.begin.instanttime", cdc_begin)
       .load(mor_path))
(cdc.selectExpr("ts_ms as commit_time", "op",
                "get_json_object(before, '$.ss_quantity') as qty_before",
                "get_json_object(after, '$.ss_quantity') as qty_after")
    .orderBy(desc("commit_time"))
    .show(20, truncate=False))
# 35 rows: 30 'u' and 5 'd'.
