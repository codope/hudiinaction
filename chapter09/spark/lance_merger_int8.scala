// Chapter 9, "Choosing a base file format for AI workloads" - resolves two v2 TODOs:
//   1. Does a Lance table need a record-merger property at all, and if so, which config
//      key is canonical: the docs' 'hoodie.record.merger.impls', the companion's prior
//      'hoodie.datasource.write.record.merger.impls', or something else entirely?
//   2. Does a VECTOR(8, INT8) column fail on a Lance-backed table at write time?
//
// Source ground truth (release-1.2.0):
//   - hudi-common/.../HoodieReaderConfig.java:74-77 -
//       RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY       = "hoodie.write.record.merge.custom.implementation.classes"  (canonical)
//       RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY = "hoodie.datasource.write.record.merger.impls"          (registered alias, deprecated)
//   - hudi-client-common/.../HoodieWriteConfig.java:208-214 - RECORD_MERGE_IMPL_CLASSES
//       .withAlternatives(RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY)
//   - There is NO config key or alternative anywhere in release-1.2.0 spelled
//     'hoodie.record.merger.impls' (the chapter/docs spelling, missing "datasource.write.").
//     Grepped the whole tree; the only exact hits are the two keys above plus Flink's
//     unrelated 'record.merger.impls' (FlinkOptions.java:151, a different config namespace).

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/lance_merger_int8")

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

def dropAndCreate(tableName: String, path: String, extraProps: String): Unit = {
  spark.sql(s"DROP TABLE IF EXISTS $tableName")
  spark.sql(s"""
    CREATE TABLE $tableName (
      ticket_id STRING,
      subject   STRING,
      embedding VECTOR(8)
    ) USING hudi
    TBLPROPERTIES (
      primaryKey = 'ticket_id',
      type = 'cow',
      'hoodie.table.base.file.format' = 'LANCE'
      $extraProps
    )
    LOCATION '$path'
  """)
}

def tryInsert(tableName: String): String = {
  try {
    spark.sql(s"""
      INSERT INTO $tableName VALUES
        ('T-1', 'test ticket', array(cast(0.1 as float), cast(0.2 as float), cast(0.0 as float),
                                      cast(0.0 as float), cast(0.0 as float), cast(0.0 as float),
                                      cast(0.0 as float), cast(0.0 as float)))
    """)
    "SUCCEEDED"
  } catch {
    case e: Exception => s"FAILED: ${e.getClass.getName}: ${e.getMessage}"
  }
}

println("=== (1) No merger property at all ===")
dropAndCreate("lance_no_merger", s"$basePath/lance_no_merger", "")
println("Insert result: " + tryInsert("lance_no_merger"))

println("\n=== (2) 'hoodie.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger' (docs' spelling, chapter's exact key) ===")
dropAndCreate("lance_docs_key", s"$basePath/lance_docs_key",
  ", 'hoodie.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger'")
println("Insert result: " + tryInsert("lance_docs_key"))

println("\n=== (3) 'hoodie.datasource.write.record.merger.impls' = ... (deprecated alias key, what the companion run used previously) ===")
dropAndCreate("lance_deprecated_key", s"$basePath/lance_deprecated_key",
  ", 'hoodie.datasource.write.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger'")
println("Insert result: " + tryInsert("lance_deprecated_key"))

println("\n=== (4) 'hoodie.write.record.merge.custom.implementation.classes' = ... (canonical key per HoodieReaderConfig.java:74) ===")
dropAndCreate("lance_canonical_key", s"$basePath/lance_canonical_key",
  ", 'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger'")
println("Insert result: " + tryInsert("lance_canonical_key"))

println("\n=== (5) CHAPTER'S EXACT support_tickets_lance DDL, verbatim ===")
spark.sql("DROP TABLE IF EXISTS support_tickets_lance")
try {
  spark.sql(s"""
    CREATE TABLE support_tickets_lance (
      ticket_id STRING,
      subject   STRING,
      embedding VECTOR(8)
    ) USING hudi
    TBLPROPERTIES (
      primaryKey = 'ticket_id',
      type = 'cow',
      'hoodie.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger',
      'hoodie.table.base.file.format' = 'LANCE'
    )
    LOCATION '$basePath/support_tickets_lance'
  """)
  println("CREATE TABLE support_tickets_lance SUCCEEDED (property accepted syntactically).")
} catch {
  case e: Exception => println(s"CREATE TABLE support_tickets_lance FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
println("Insert result against support_tickets_lance (chapter's exact TBLPROPERTIES): " + tryInsert("support_tickets_lance"))

println("\n=== (6) VECTOR(8, INT8) column on a Lance-backed table ===")
spark.sql("DROP TABLE IF EXISTS lance_int8")
try {
  spark.sql(s"""
    CREATE TABLE lance_int8 (
      ticket_id STRING,
      embedding VECTOR(8, INT8)
    ) USING hudi
    TBLPROPERTIES (
      primaryKey = 'ticket_id',
      type = 'cow',
      'hoodie.write.record.merge.custom.implementation.classes' = 'org.apache.hudi.DefaultSparkRecordMerger',
      'hoodie.table.base.file.format' = 'LANCE'
    )
    LOCATION '$basePath/lance_int8'
  """)
  println("CREATE TABLE lance_int8 SUCCEEDED.")
  try {
    spark.sql("""
      INSERT INTO lance_int8 VALUES
        ('T-1', array(cast(10 as tinyint), cast(20 as tinyint), cast(0 as tinyint), cast(0 as tinyint),
                       cast(0 as tinyint), cast(0 as tinyint), cast(0 as tinyint), cast(0 as tinyint)))
    """)
    println("INSERT into VECTOR(8, INT8) Lance table SUCCEEDED.")
  } catch {
    case e: Exception => println(s"INSERT into VECTOR(8, INT8) Lance table FAILED: ${e.getClass.getName}: ${e.getMessage}")
  }
} catch {
  case e: Exception => println(s"CREATE TABLE lance_int8 FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("LANCE_MERGER_INT8_DONE")
System.exit(0)
