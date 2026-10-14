// Chapter 9, "Choosing a base file format for AI workloads" - the same shape of table as
// ai_types.scala, but with hoodie.table.base.file.format=LANCE instead of the Parquet default.
//
// Two things this script checks against the real 1.2.0 bundle, not just the docs:
//   1. What happens to the column-stats and partition-stats metadata-table partitions when
//      the base file format is Lance (MetadataPartitionType gates both off for LANCE, per
//      hudi-common's MetadataPartitionType.java - this should be silent, not an error).
//   2. Whether hudi_vector_search() works against a Lance-backed table.
//
// Requires the external Lance Spark bundle in addition to the Hudi bundle, so this is run
// through scripts/run_lance.sh -> scripts/spark_shell_with_hudi_lance.sh, which adds
// org.lance:lance-spark-bundle-3.5_2.12:0.4.0 via --packages.

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/lance")
val tablePath = s"$basePath/support_tickets_lance"
val tableName = "support_tickets_lance"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

// ============================================================================
// 1. CREATE TABLE with hoodie.table.base.file.format = 'LANCE'
// ============================================================================
// Lance base files require the Spark record type; DefaultSparkRecordMerger is the
// merger implementation that routes through it (see hudi_sql_vector_blob_demo.py and
// TestLanceDataSource.scala, which both set this explicitly). Without it, Hudi's writer
// factory defaults to the AVRO record type, whose Lance writer is a stub.
spark.sql(s"""
  CREATE TABLE $tableName (
      ticket_id  STRING,
      subject    STRING,
      category   STRING,
      created_at BIGINT,
      embedding  VECTOR(8) COMMENT 'subject embedding, default FLOAT element type',
      thumbnail  BLOB       COMMENT 'small screenshot thumbnail, stored INLINE'
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'ticket_id',
      orderingFields = 'created_at',
      type = 'cow',
      'hoodie.table.base.file.format' = 'LANCE',
      'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'
  )
  LOCATION '$tablePath'
""")
println(s"Created LANCE-backed table $tableName at $tablePath")

val referenceStructType = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"
def inlineBlob(bytesHex: String): String =
  s"named_struct('type','INLINE','data',cast(unhex('$bytesHex') as binary),'reference',cast(null as $referenceStructType))"

val tickets = Seq(
  ("T-2001", "App crashes on login",        "bug",      2001L, Array(0.9f, 0.1f, 0.0f, 0.0f, 0.2f, 0.0f, 0.1f, 0.0f)),
  ("T-2002", "Password reset email missing","account",  2002L, Array(0.1f, 0.9f, 0.1f, 0.0f, 0.0f, 0.1f, 0.0f, 0.0f)),
  ("T-2003", "Double-charged on invoice",    "billing",  2003L, Array(0.0f, 0.1f, 0.9f, 0.1f, 0.0f, 0.0f, 0.0f, 0.1f)),
  ("T-2004", "App crashes on Android 14",    "bug",      2004L, Array(0.85f,0.15f,0.0f, 0.05f,0.15f,0.0f, 0.1f, 0.0f))
)
val insertRows = tickets.map { case (id, subject, category, ts, emb) =>
  val embLit = "array(" + emb.map(v => s"cast($v as float)").mkString(",") + ")"
  val thumbHex = f"${math.abs(id.hashCode) % 256}%02x" * 8
  s"('$id','$subject','$category',$ts,$embLit,${inlineBlob(thumbHex)})"
}
spark.sql(s"INSERT INTO $tableName VALUES ${insertRows.mkString(",\n")}")
println(s"Inserted ${tickets.size} rows into the LANCE table")

println("\n=== Query the Lance-backed table back through Hudi ===")
spark.sql(s"SELECT ticket_id, subject, category FROM $tableName ORDER BY ticket_id").show(false)

// ============================================================================
// 2. What happened to column-stats / partition-stats in the metadata table?
// ============================================================================
println("\n=== hoodie.properties: base file format ===")
scala.io.Source.fromFile(s"$tablePath/.hoodie/hoodie.properties").getLines()
  .filter(l => l.startsWith("hoodie.table.base.file.format") || l.startsWith("hoodie.table.metadata.partitions"))
  .foreach(println)

println("\n=== Metadata table partitions actually present on disk ===")
val metadataDir = new File(s"$tablePath/.hoodie/metadata")
if (metadataDir.exists()) {
  metadataDir.listFiles().filter(_.isDirectory).map(_.getName).sorted.foreach(p => println(s"  $p"))
} else {
  println("  (no .hoodie/metadata directory at all)")
}

println("\n=== Explicitly asking for column-stats / partition-stats index availability ===")
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.MetadataPartitionType
val metaClient = HoodieTableMetaClient.builder()
  .setConf(HadoopFSUtils.getStorageConf())
  .setBasePath(tablePath)
  .build()
val tableConfig = metaClient.getTableConfig
println("Base file format: " + tableConfig.getBaseFileFormat)
println("COLUMN_STATS partition available: " + tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS))
println("PARTITION_STATS partition available: " + tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.PARTITION_STATS))

// Try to build the column-stats index anyway, to see whether Hudi errors, warns, or
// silently ignores the request when the base file format is Lance.
println("\n=== Attempting a write with hoodie.metadata.index.column.stats.enable=true on the Lance table ===")
try {
  spark.sql(s"""
    INSERT INTO $tableName VALUES
      ('T-2005', 'Refund for duplicate charge', 'billing', 2005,
       array(cast(0.0 as float), cast(0.05 as float), cast(0.85 as float), cast(0.15 as float), cast(0.0 as float), cast(0.05 as float), cast(0.0 as float), cast(0.1 as float)),
       ${inlineBlob("ab" * 8)})
  """)
  println("Insert with default column-stats config succeeded (no exception).")
} catch {
  case e: Exception => println(s"Insert FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
val metaClient2 = HoodieTableMetaClient.builder()
  .setConf(HadoopFSUtils.getStorageConf())
  .setBasePath(tablePath)
  .build()
println("COLUMN_STATS partition available after that write: " +
  metaClient2.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS))

// ============================================================================
// 3. Does hudi_vector_search() work on a Lance-backed table?
// ============================================================================
println("\n=== hudi_vector_search() over the LANCE table ===")
val queryVec = "ARRAY(" + Array(0.88f, 0.12f, 0.0f, 0.0f, 0.18f, 0.0f, 0.1f, 0.0f).mkString(",") + ")"
try {
  val df = spark.sql(s"""
    SELECT ticket_id, subject, category, _hudi_distance
    FROM hudi_vector_search('$tablePath', 'embedding', $queryVec, 3, 'cosine')
    ORDER BY _hudi_distance
  """)
  df.show(false)
  println("hudi_vector_search() SUCCEEDED against the Lance-backed table.")
} catch {
  case e: Exception => println(s"hudi_vector_search() FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
