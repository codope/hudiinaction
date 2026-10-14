// Follow-up: schema-on-read CONTROL, scalar-only columns, session-level
// SET hoodie.schema.on.read.enable=true before the ALTER statements (per the coordinator's
// reading of schema_evolution.md), plus the Hive-metastore compatibility-check override from
// the same doc's "Disabling the Hive metastore column type compatibility check" section
// (passed at launch time via spark_shell_with_hudi_schema_evolution.sh, not settable from
// inside the session since Spark's embedded metastore reads it at metastore-client
// construction time).
//
// If ADD COLUMNS / RENAME COLUMN / ALTER COLUMN TYPE all work here on a plain scalar table,
// that isolates whether schema_ai_types.scala's original ADD COLUMNS failure was really about
// VECTOR/BLOB, or just the generic metastore compatibility check (which this table doesn't
// otherwise need, since it never reorders columns -- ADD COLUMNS defaults to appending at the
// end, so this control should not even need the override; kept anyway for a fair comparison).

import java.io.File

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/schema_on_read_control")
val tablePath = s"$basePath/control_table"
val tableName = "ch9_schema_control"

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))
spark.sql(s"DROP TABLE IF EXISTS $tableName")

println("=== SET hoodie.schema.on.read.enable = true (session level, before any ALTER) ===")
spark.sql("SET hoodie.schema.on.read.enable=true")

spark.sql(s"""
  CREATE TABLE $tableName (
      id         STRING,
      name       STRING,
      version_id INT
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'id',
      orderingFields = 'id',
      type = 'cow'
  )
  LOCATION '$tablePath'
""")
spark.sql(s"INSERT INTO $tableName VALUES ('r1', 'alice', 1)")
println("Baseline schema:")
spark.table(tableName).printSchema()

println("\n=== ALTER TABLE ... ADD COLUMNS (extra STRING) ===")
try {
  spark.sql(s"ALTER TABLE $tableName ADD COLUMNS (extra STRING)")
  println("ADD COLUMNS SUCCEEDED.")
  spark.sql(s"INSERT INTO $tableName VALUES ('r2', 'bob', 2, 'new-field')")
  spark.sql(s"SELECT * FROM $tableName ORDER BY id").show(false)
} catch {
  case e: Exception => println(s"ADD COLUMNS FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== ALTER TABLE ... ALTER COLUMN version_id TYPE BIGINT (int -> long) ===")
try {
  spark.sql(s"ALTER TABLE $tableName ALTER COLUMN version_id TYPE BIGINT")
  println("ALTER COLUMN TYPE SUCCEEDED.")
  spark.table(tableName).printSchema()
} catch {
  case e: Exception => println(s"ALTER COLUMN TYPE FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== ALTER TABLE ... RENAME COLUMN name TO full_name ===")
try {
  spark.sql(s"ALTER TABLE $tableName RENAME COLUMN name TO full_name")
  println("RENAME COLUMN SUCCEEDED.")
  spark.table(tableName).printSchema()
} catch {
  case e: Exception => println(s"RENAME COLUMN FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
