// Chapter 9, "Adding columns without turning anything on" - v2 TODO: confirm the DEFAULT
// (schema-on-read OFF) ADD COLUMNS path works on a table with VECTOR + BLOB columns. Per the
// prior pass's source reading, HoodieCatalog.alterTable (hudi-spark-datasource/hudi-spark-common/
// .../catalog/HoodieCatalog.scala:208-244) has an UNCONDITIONAL case for AddColumn/UpdateColumnType/
// UpdateColumnComment that does not route through Spark35ResolveHudiAlterTableCommand's
// schema-on-read guard at all -- so ADD COLUMNS should work here even though schema-on-read
// itself cannot be turned on for a VECTOR-bearing table (draft ~L1088). This script does NOT
// set hoodie.schema.on.read.enable anywhere (session or table property).

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/add_column_default_path")
val tablePath = s"$basePath/support_tickets"
val tableName = "support_tickets_default_addcol"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

println(s"spark.sql.catalogImplementation = ${spark.conf.get("spark.sql.catalogImplementation")}")

spark.sql(s"""
  CREATE TABLE $tableName (
      ticket_id  STRING,
      subject    STRING,
      created_at BIGINT,
      embedding  VECTOR(8) COMMENT 'subject embedding',
      thumbnail  BLOB       COMMENT 'small screenshot thumbnail, stored INLINE'
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'ticket_id',
      orderingFields = 'created_at',
      type = 'cow'
  )
  LOCATION '$tablePath'
""")

val referenceStructType = "struct<external_path:string,offset:bigint,length:bigint,managed:boolean>"
def inlineBlob(bytesHex: String): String =
  s"named_struct('type','INLINE','data',cast(unhex('$bytesHex') as binary),'reference',cast(null as $referenceStructType))"

spark.sql(s"""
  INSERT INTO $tableName VALUES
    ('T-1', 'App crashes on login', 1000,
     array(cast(0.9 as float), cast(0.1 as float), cast(0.0 as float), cast(0.0 as float), cast(0.2 as float), cast(0.0 as float), cast(0.1 as float), cast(0.0 as float)),
     ${inlineBlob("aabbccdd")})
""")
println("=== Baseline schema (schema-on-read NOT enabled anywhere, table holds VECTOR + BLOB) ===")
spark.table(tableName).printSchema()

println("\n=== ALTER TABLE ... ADD COLUMNS (priority STRING), default path (no schema-on-read) ===")
try {
  spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (priority STRING)")
  println("ADD COLUMNS SUCCEEDED.")
  spark.table(tableName).printSchema()
  spark.sql(s"""
    INSERT INTO $tableName VALUES
      ('T-2', 'Refund request', 2000,
       array(cast(0.0 as float), cast(0.9 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float)),
       ${inlineBlob("11223344")}, 'high')
  """)
  println("Insert using the new column succeeded. Reading back:")
  spark.sql(s"SELECT ticket_id, subject, priority, length(read_blob(thumbnail)) AS thumb_bytes FROM $tableName ORDER BY ticket_id").show(false)
} catch {
  case e: Exception =>
    println(s"ADD COLUMNS FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("ADD_COLUMN_DEFAULT_PATH_DONE")
System.exit(0)
