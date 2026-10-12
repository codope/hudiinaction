/**
 * Chapter 4: Hudi query types on the store_sales table.
 *   snapshot, read-optimized, time travel, incremental (latest_state), and CDC.
 *
 * Prerequisite: run hudi_querying_tutorial.scala first (it creates /tmp/hudi_demo_tbl and
 * defines inputPath, df, and writeOpts). Then, in the same spark-shell:
 *   scala> :paste /path/to/hudiinaction/chapter04/query_types.scala
 *
 * The commit times printed below come from one run; yours will differ.
 */

import org.apache.spark.sql.functions._

val basePath = "/tmp/hudi_demo_tbl"

// Register the table in the session catalog (this replaces the hudi_tbl temporary view from
// the tutorial). CALL procedures and hudi_table_changes look tables up by name.
spark.sql("DROP TABLE IF EXISTS hudi_tbl")
spark.sql(s"CREATE TABLE hudi_tbl USING hudi LOCATION '$basePath'")

// ============================================================================
// Snapshot and read-optimized queries
// ============================================================================

spark.read.format("hudi")
  .load(basePath)
  .where("ss_item_sk BETWEEN 1000 AND 2000")
  .orderBy("ss_item_sk")
  .show(false)

// On a Copy-on-Write table, read_optimized returns the same rows as a snapshot query.
spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "read_optimized")
  .load(basePath)
  .where("ss_item_sk BETWEEN 1000 AND 2000")
  .orderBy("ss_item_sk")
  .show(false)

// ============================================================================
// Time travel
// ============================================================================

// commit_time is the instant Hudi stamps into _hoodie_commit_time;
// state_transition_time is when that commit completed.
spark.sql("CALL show_commits(table => 'hudi_tbl')")
  .select("commit_time", "state_transition_time", "action").show(false)
// +-----------------+---------------------+------+
// |commit_time      |state_transition_time|action|
// +-----------------+---------------------+------+
// |20261012152336309|20261012152349459    |commit|
// +-----------------+---------------------+------+

val loadCommit = spark.sql("CALL show_commits(table => 'hudi_tbl')").first.getAs[String]("commit_time")
val dfAsOf = spark.read.format("hudi")
  .option("as.of.instant", loadCommit)
  .load(basePath)
dfAsOf.select("ss_item_sk", "ss_net_paid", "ss_net_profit").show(false)

// ============================================================================
// Incremental queries
// ============================================================================

// 1. Upsert 50 rows with a changed ss_net_paid. Upserts write whole rows: dropping columns
//    from the DataFrame fails with MissingSchemaFieldException.
val new_sales_batch = spark.read.format("hudi").load(basePath)
  .orderBy(rand()).limit(50)
  .withColumn("ss_net_paid", col("ss_net_paid") + expr("cast(0.25 as decimal(7,2))"))
val upsertOpts = writeOpts + ("hoodie.datasource.write.operation" -> "upsert")
new_sales_batch.write.format("hudi").options(upsertOpts).mode("append").save(basePath)

// 2. Read only the changes made after the initial load. In Hudi 1.x the begin value is a
//    completion time (state_transition_time), and it is exclusive: commits that completed
//    after it are returned. Passing the load's commit_time instead returns the whole table,
//    because the load completed after its own commit_time.
val begin = spark.sql("CALL show_commits(table => 'hudi_tbl')")
  .orderBy("commit_time").first.getAs[String]("state_transition_time")
val incr = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.read.begin.instanttime", begin)
  .load(basePath)
incr.select("_hoodie_commit_time", "ss_item_sk", "ss_ticket_number", "ss_net_paid").show(10, false)
println("Incremental rows: " + incr.count())   // 50

// 3. The same through the hudi_table_changes table-valued function. latest_state returns the
//    table's own columns, one latest row per changed key.
spark.sql(s"""
  SELECT _hoodie_commit_time, ss_item_sk, ss_ticket_number, ss_net_paid
  FROM hudi_table_changes('hudi_tbl', 'latest_state', '$begin')
  ORDER BY _hoodie_commit_time DESC
  LIMIT 20""").show(false)

// ============================================================================
// Change Data Capture (CDC)
// ============================================================================

// 1. Create a CDC-enabled Merge-on-Read table from the same data.
val morPath  = "file:///tmp/hudi/demo/store_sales_mor"
val morTable = "store_sales_mor"
val morOpts = Map(
  "hoodie.table.name" -> morTable,
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.table.cdc.enabled" -> "true",
  // DATA_BEFORE_AFTER stores both images; OP_KEY_ONLY minimizes storage
  "hoodie.table.cdc.supplemental.logging.mode" -> "DATA_BEFORE_AFTER")
df.write.format("hudi").options(morOpts).mode("overwrite").save(morPath)
spark.sql(s"DROP TABLE IF EXISTS $morTable")
spark.sql(s"CREATE TABLE IF NOT EXISTS $morTable USING hudi LOCATION '$morPath'")

// 2a. Update 30 rows.
val updates = spark.read.format("hudi").load(morPath)
  .orderBy(rand()).limit(30)
  .withColumn("ss_quantity", col("ss_quantity") + 1)
updates.write.format("hudi").options(morOpts +
  ("hoodie.datasource.write.operation" -> "upsert"))
  .mode("append").save(morPath)

// 2b. Delete 5 rows. A delete needs the full record key.
val delKeys = spark.read.format("hudi").load(morPath)
  .select("ss_item_sk", "ss_ticket_number").orderBy(rand()).limit(5)
delKeys.write.format("hudi")
  .options(morOpts + ("hoodie.datasource.write.operation" -> "delete"))
  .mode("append").save(morPath)

// 3. CDC through SQL. before and after are JSON strings; deletes have no after image, so the
//    key is taken from whichever image exists.
//    Note: in Hudi 1.2.1, DECIMAL columns are not serialized correctly in these JSON images,
//    which is why this example changes the INT column ss_quantity.
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
  LIMIT 20""").show(false)
// +-----------------+---+----------+---------+----------+----------------+
// |commit_time      |op |qty_before|qty_after|ss_item_sk|ss_ticket_number|
// +-----------------+---+----------+---------+----------+----------------+
// |20261012152445426|d  |70        |NULL     |1217      |19764           |
// |20261012152445426|d  |55        |NULL     |15769     |76373           |
// ...
// |20261012152440727|u  |75        |76       |5581      |185528          |
// |20261012152440727|u  |44        |45       |4274      |114150          |
// ...
// Over the whole timeline: i = 2,880,404 (the initial load), u = 30, d = 5.

// 4. CDC through the DataFrame API, starting after the initial load.
//    (In Hudi 1.2.1 the DataFrame CDC reader returns no rows for begin = "earliest";
//    the SQL function above handles "earliest" correctly.)
val cdcBegin = spark.sql(s"CALL show_commits(table => '$morTable')")
  .orderBy("commit_time").first.getAs[String]("state_transition_time")
val cdc = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.query.incremental.format", "cdc")
  .option("hoodie.datasource.read.begin.instanttime", cdcBegin)
  .load(morPath)
cdc.selectExpr("ts_ms as commit_time", "op",
    "get_json_object(before, '$.ss_quantity') as qty_before",
    "get_json_object(after, '$.ss_quantity') as qty_after")
  .orderBy(desc("commit_time"))
  .show(20, false)
// 35 rows: 30 'u' and 5 'd'.
