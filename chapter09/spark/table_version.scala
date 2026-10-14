// Chapter 9, "Upgrading and downgrading the table format" - create a table at an older
// table version, show hoodie.table.version, then upgrade (auto-upgrade on write, and the
// upgrade_table procedure) and downgrade (the downgrade_table procedure, and by-writer
// downgrade), showing the version after each step. Valid table versions at 1.2.0: SIX(6),
// EIGHT(8), NINE(9) (HoodieTableVersion.java; NINE is HoodieTableVersion.current(), the
// default hoodie.write.table.version).
//
// v2, follow-up fix: the v1 "no auto-upgrade on write" conclusion was an artifact of using
// a CATALOG table (CREATE TABLE ... TBLPROPERTIES('hoodie.write.table.version'='6')).
// ProvidesHoodieConfig.combineOptions merges the catalog table's persisted TBLPROPERTIES back
// into every later SQL write to that table (tier 4 in its documented precedence order), so
// the writer's *target* version stayed pinned at 6 on every subsequent write, masking
// auto-upgrade entirely -- the writer never actually asked for version 9. This v2 uses the
// plain DataFrame API against raw paths (no catalog, no persisted-property reinjection) so
// each write's version intent is exactly what that call's own .option(...) says.
//
// Source for auto-upgrade: HoodieWriteConfig.AUTO_UPGRADE_VERSION
// (hoodie.write.auto.upgrade, default true) and BaseHoodieWriteClient.tryUpgrade
// (hudi-client/hudi-client-common/.../BaseHoodieWriteClient.java:1565-1590), which calls
// UpgradeDowngrade.needsUpgrade(config.getWriteVersion()) -- note this checks needsUpgrade
// ONLY, not needsDowngrade, so a lower hoodie.write.table.version than the table's current
// on-disk version does NOT go through this same automatic path on a plain write.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/table_version")

def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))
Seq("cli_upgrade").foreach(t => spark.sql(s"DROP TABLE IF EXISTS ch9_tv_$t"))

def tableVersion(tablePath: String): String =
  scala.io.Source.fromFile(s"$tablePath/.hoodie/hoodie.properties").getLines()
    .find(_.startsWith("hoodie.table.version="))
    .getOrElse("hoodie.table.version=<not found>")

def writeDf(path: String, rows: Seq[(String, Double)], extraOpts: Map[String, String], mode: SaveMode): Unit = {
  val df = rows.toDF("id", "fare")
  var writer = df.write.format("hudi")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", new File(path).getName)
  extraOpts.foreach { case (k, v) => writer = writer.option(k, v) }
  writer.mode(mode).save(path)
}

// ============================================================================
// (a) df.write.format("hudi") with hoodie.write.table.version=6, to a NEW path.
// ============================================================================
val pathA = s"$basePath/a_v6"
println("=== (a) DataFrame write, hoodie.write.table.version=6, new path ===")
writeDf(pathA, Seq(("r1", 12.0)), Map("hoodie.write.table.version" -> "6"), SaveMode.Overwrite)
println(tableVersion(pathA))

// ============================================================================
// (b) Second write to the SAME path, NO hoodie.write.table.version option (targets 9,
//     the default / HoodieTableVersion.current()).
// ============================================================================
println("\n=== (b) Second write to the SAME path, no version option (writer targets 9 by default) ===")
try {
  writeDf(pathA, Seq(("r2", 18.0)), Map.empty, SaveMode.Append)
  println("Write SUCCEEDED.")
} catch {
  case e: Exception => println(s"Write FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
println(tableVersion(pathA))

// ============================================================================
// (c) Repeat (a) on a FRESH path, then write with hoodie.write.auto.upgrade=false and no
//     version option: does the table stay at 6 (auto-upgrade suppressed), and does the
//     write still succeed?
// ============================================================================
val pathC = s"$basePath/c_v6_noautoupgrade"
println("\n=== (c) Fresh path at version 6, then a write with hoodie.write.auto.upgrade=false ===")
writeDf(pathC, Seq(("r1", 12.0)), Map("hoodie.write.table.version" -> "6"), SaveMode.Overwrite)
println("After initial write: " + tableVersion(pathC))
try {
  writeDf(pathC, Seq(("r2", 18.0)), Map("hoodie.write.auto.upgrade" -> "false"), SaveMode.Append)
  println("Write with hoodie.write.auto.upgrade=false SUCCEEDED.")
} catch {
  case e: Exception => println(s"Write with hoodie.write.auto.upgrade=false FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
println("After that write: " + tableVersion(pathC))

// ============================================================================
// (d) On a v9 table (default), write with hoodie.write.table.version=8: does the table
//     get auto-downgraded to 8, or does the write fail? Exact message either way.
// ============================================================================
val pathD = s"$basePath/d_v9_then_write_v8"
println("\n=== (d) Fresh v9 (default) table, then a write with hoodie.write.table.version=8 ===")
writeDf(pathD, Seq(("r1", 12.0)), Map.empty, SaveMode.Overwrite)
println("After initial write (default version): " + tableVersion(pathD))
try {
  writeDf(pathD, Seq(("r2", 18.0)), Map("hoodie.write.table.version" -> "8"), SaveMode.Append)
  println("Write with hoodie.write.table.version=8 SUCCEEDED.")
} catch {
  case e: Throwable =>
    println(s"Write with hoodie.write.table.version=8 FAILED.")
    println(s"Exception class:   ${e.getClass.getName}")
    println(s"Exception message: ${e.getMessage}")
    var cause = e.getCause
    while (cause != null) {
      println(s"  caused by: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
    }
}
println("After that write: " + tableVersion(pathD))

// ============================================================================
// 2. A second table, upgraded and downgraded explicitly via CALL upgrade_table /
//    CALL downgrade_table (these require a Spark-catalog table identifier, not a path;
//    kept from v1 since these are genuinely catalog-only procedures, not a DataFrame path).
// ============================================================================
val cliPath = s"$basePath/cli_upgrade"
spark.sql(s"""
  CREATE TABLE ch9_tv_cli_upgrade (
      id   STRING,
      fare DOUBLE
  ) USING hudi
  TBLPROPERTIES (
      primaryKey = 'id',
      'hoodie.write.table.version' = '6'
  )
  LOCATION '$cliPath'
""")
spark.sql("INSERT INTO ch9_tv_cli_upgrade VALUES ('r1', 12.0)")
println(s"\n=== Catalog table created with hoodie.write.table.version=6 ===")
println(tableVersion(cliPath))

println("\n=== CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'EIGHT') ===")
try {
  spark.sql("CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'EIGHT')").show(false)
} catch {
  case e: Exception => println(s"upgrade_table FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
println(tableVersion(cliPath))

println("\n=== CALL downgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'SIX') ===")
try {
  spark.sql("CALL downgrade_table(table => 'ch9_tv_cli_upgrade', to_version => 'SIX')").show(false)
} catch {
  case e: Exception => println(s"downgrade_table FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
println(tableVersion(cliPath))

println("\n=== CALL upgrade_table with a NUMERIC to_version ('8') instead of the enum name ===")
try {
  spark.sql("CALL upgrade_table(table => 'ch9_tv_cli_upgrade', to_version => '8')").show(false)
  println(tableVersion(cliPath))
} catch {
  case e: Exception => println(s"upgrade_table with numeric version FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

System.exit(0)
