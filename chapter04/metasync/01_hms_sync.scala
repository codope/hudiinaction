/*
 * 01_hms_sync.scala: sync a Hudi table to a Hive Metastore (HMS).
 *
 * Dataset: chapter04/input_data (TPC-DS store_sales, not partitioned).
 * Record key: (ss_item_sk, ss_ticket_number).
 * Hudi 1.2.1, Spark 3.5. Docs: https://hudi.apache.org/docs/syncing_metastore/
 *
 * Prerequisite: a Hive Metastore listening on thrift://localhost:9083. For a local test you
 * can start a standalone metastore from the Hive 2.3.9 jars that ship with Spark. Save this as
 * hive-site.xml in a new directory and run the java command from that directory:
 *
 *   <configuration>
 *     <property><name>javax.jdo.option.ConnectionURL</name>
 *       <value>jdbc:derby:;databaseName=metastore_db;create=true</value></property>
 *     <property><name>datanucleus.schema.autoCreateAll</name><value>true</value></property>
 *     <property><name>hive.metastore.schema.verification</name><value>false</value></property>
 *   </configuration>
 *
 *   java -cp ".:$SPARK_HOME/jars/*" org.apache.hadoop.hive.metastore.HiveMetaStore -p 9083
 *
 * Launch spark-shell with the command in chapter04/README.md, then:
 *   scala> :paste /path/to/hudiinaction/chapter04/metasync/01_hms_sync.scala
 */

import org.apache.spark.sql.SaveMode

val inputPath = "/tmp/input_data"
val basePath  = "file:///tmp/sales_hudi_hms"
val dbName    = "default"
val tableName = "sales_hudi"

val raw = spark.read.parquet(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")): _*)

// ---------- Write-time sync ----------
val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk,ss_ticket_number",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  // Run the meta sync after each successful commit
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.hive_sync.mode" -> "hms",
  "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://localhost:9083",
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table" -> tableName)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

/*
---------- Independent sync (no write) ----------
HiveSyncTool ships inside the Hudi Spark bundle, so spark-submit can run it with the Hive
client jars that come with Spark. run_sync_tool.sh in the Hudi repository runs the same class
but expects a local Hadoop and Hive installation (HADOOP_HOME, HIVE_HOME).

curl -fLO https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.1/hudi-spark3.5-bundle_2.12-1.2.1.jar
spark-submit --class org.apache.hudi.hive.HiveSyncTool \
  hudi-spark3.5-bundle_2.12-1.2.1.jar \
  --metastore-uris thrift://localhost:9083 \
  --database default \
  --table sales_hudi \
  --base-path file:///tmp/sales_hudi_hms \
  --sync-mode hms
*/

/*
---------- Read through the metastore ----------
Restart spark-shell with two extra settings so Spark uses the metastore as its catalog:
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083
Then:
  spark.sql("SHOW TABLES IN default").show(false)
  spark.sql("SELECT count(*) AS rows FROM default.sales_hudi").show(false)   // 2880404
*/
