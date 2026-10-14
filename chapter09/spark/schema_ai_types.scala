// Chapter 9, "how the section 1 types behave under evolution" - tries schema-on-read
// (hoodie.schema.on.read.enable=true) operations (add column, rename column) on a table
// that also holds VECTOR and BLOB columns, and records whether it works or the exact error.
//
// Reuses the ai_types.scala table shape (a small support-ticket corpus) so the types under
// test are the same ones introduced in that example.

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/schema_ai_types")
val tablePath = s"$basePath/support_tickets"
val tableName = "ch9_schema_ai_types"

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

// Follow-up fix: also set this at SESSION level, before the ALTER statements, matching the
// coordinator's reading of schema_evolution.md's recipe (the table property alone was used in
// the first pass; both are set here so the example demonstrates the documented recipe exactly,
// even though the table property should already be sufficient per the same doc's own words,
// "needs to be set on the writer config (Datasource) or table property (SQL)").
println("=== SET hoodie.schema.on.read.enable = true (session level, before any ALTER) ===")
spark.sql("SET hoodie.schema.on.read.enable=true")

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
      type = 'cow',
      'hoodie.schema.on.read.enable' = 'true'
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
println("=== Baseline schema (schema-on-read enabled, table holds VECTOR + BLOB) ===")
spark.table(tableName).printSchema()

// ============================================================================
// 1. ADD COLUMN
// ============================================================================
println("\n=== ALTER TABLE ... ADD COLUMNS (priority STRING) ===")
try {
  spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (priority STRING)")
  println("ADD COLUMNS SUCCEEDED.")
  spark.table(tableName).printSchema()
  spark.sql(s"INSERT INTO $tableName VALUES ('T-2', 'Refund request', 2000, " +
    "array(cast(0.0 as float), cast(0.9 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float), cast(0.0 as float)), " +
    s"${inlineBlob("11223344")}, 'high')")
  println("Insert after ADD COLUMNS succeeded. Rows:")
  spark.sql(s"SELECT ticket_id, subject, priority FROM $tableName ORDER BY ticket_id").show(false)
} catch {
  case e: Exception =>
    println(s"ADD COLUMNS FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

// ============================================================================
// 2. RENAME COLUMN
// ============================================================================
println("\n=== ALTER TABLE ... RENAME COLUMN embedding TO subject_embedding ===")
try {
  spark.sql(s"ALTER TABLE $tableName RENAME COLUMN embedding TO subject_embedding")
  println("RENAME COLUMN SUCCEEDED.")
  spark.table(tableName).printSchema()
  spark.sql(s"SELECT ticket_id, subject_embedding FROM $tableName ORDER BY ticket_id").show(false)
} catch {
  case e: Exception =>
    println(s"RENAME COLUMN FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

// ============================================================================
// 3. RENAME COLUMN on the BLOB column, for completeness
// ============================================================================
println("\n=== ALTER TABLE ... RENAME COLUMN thumbnail TO thumbnail_bytes ===")
try {
  spark.sql(s"ALTER TABLE $tableName RENAME COLUMN thumbnail TO thumbnail_bytes")
  println("RENAME COLUMN (BLOB) SUCCEEDED.")
  spark.table(tableName).printSchema()
} catch {
  case e: Exception =>
    println(s"RENAME COLUMN (BLOB) FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
