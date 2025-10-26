/*
 * 01_hms_sync.scala — Sync a Hudi table to Hive Metastore (HMS)
 *
 * Dataset: TPC-DS 1GB store_sales parquet (no partitions)
 * Primary key: ss_item_sk
 *
 * What this script does
 * 1) (Optional) Quick HMS setup notes (Docker) — see comments below.
 * 2) Writes a COPY_ON_WRITE Hudi table at basePath.
 * 3) Performs WRITE-TIME meta sync to HMS (mode = "hms").
 * 4) Shows the CLI (independent) sync command in comments.
 *
 * Verified against Hudi 1.0.2 + Spark 3.5 bundle.
 * Docs: HMS sync — https://hudi.apache.org/docs/syncing_metastore/
 */

// ---------- (A) Spark shell launcher (copy/paste to terminal) ----------
// ./bin/spark-shell --driver-memory 4g --executor-memory 4g \
//  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
//  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
//  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
//  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
//  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

// ---------- (B) (Optional) Quick HMS setup (one-box dev) -----------------
/*
# Start a standalone Hive Metastore (Thrift @ 9083). For quick local tests you can use a
# pre-baked docker image (backed by a local Derby or Postgres). Example:

docker run -d --name hive-metastore -p 9083:9083 \
  -e METASTORE_DB_HOST=metastore-db \
  -e METASTORE_TYPE=derby \
  --restart unless-stopped tabulario/hive:3.1.3

# Create a database for your demo:
# (You can do this via beeline/jdbc as well)
*/

// ---------- (C) Paths & input ------------------------------------------
val inputPath = "/tmp/input_data/"             // folder with parquet store_sales
val basePath  = "file:///tmp/sales_hudi_hms"   // Hudi table path
val dbName    = "hudi_db"
val tableName = "sales_hudi"

// Read source; drop any stray _hoodie_* columns if present in the input parquet
val raw = spark.read.format("parquet").load(inputPath)
val df  = raw.drop(raw.columns.filter(_.startsWith("_hoodie_")):_*)

// ---------- (D) Write Hudi + HMS sync (write-time) ----------------------
import org.apache.spark.sql.SaveMode

val writeOpts = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
  "hoodie.datasource.write.recordkey.field" -> "ss_item_sk",
  "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
  "hoodie.datasource.write.operation" -> "bulk_insert",
  "hoodie.bulkinsert.shuffle.parallelism" -> "50",

  // Enable meta sync to Hive Metastore (HMS)
  "hoodie.datasource.meta.sync.enable" -> "true",
  "hoodie.datasource.hive_sync.enable" -> "true",
  "hoodie.datasource.hive_sync.mode" -> "hms",
  "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://localhost:9083",
  "hoodie.datasource.hive_sync.database" -> dbName,
  "hoodie.datasource.hive_sync.table" -> tableName
)

df.write.format("hudi").options(writeOpts).mode(SaveMode.Overwrite).save(basePath)

// ---------- (E) Verify from Spark SQL (bound via path or via Hive if you wired a catalog) -----
spark.read.format("hudi").load(basePath).createOrReplaceTempView("hudi_tbl")
spark.sql("select count(*) as cnt from hudi_tbl").show(false)

/*
---------- (F) Independent sync (CLI) — if you didn’t set write-time sync ----------
# Download run_sync_tool.sh from the Hudi release and run:
sh /path/to/run_sync_tool.sh \
  --metastore-uris thrift://localhost:9083 \
  --database-name hudi_db \
  --table-name sales_hudi \
  --base-path file:///tmp/sales_hudi_hms \
  --sync-mode hms
*/

/* Relaunch spark-shell with Hive catalog:
 * spark-shell \
 *  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
 *  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 *  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 *  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
 *  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
 *  --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
 *  --conf spark.sql.catalogImplementation=hive
 */

 // READ (via Hive catalog)
spark.sql(s"SHOW TABLES IN $dbName").show(false)
spark.sql(s"SELECT count(*) AS rows FROM $dbName.$tableName").show(false)
spark.sql(s"SELECT ss_item_sk, SUM(ss_net_paid) AS net_paid FROM $dbName.$tableName GROUP BY ss_item_sk LIMIT 10").show(false)
