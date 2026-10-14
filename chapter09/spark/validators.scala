// Chapter 9, "Validating data before it commits" - pre-commit SQL validators: a passing
// validation, a failing one, the exception it raises, and what the timeline looks like
// afterward. Also exercises the 1.2.0 addition `hoodie.precommit.validators.failure.policy`
// (FAIL vs WARN_LOG).

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/validators")

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

def timelineFiles(tablePath: String): Seq[String] = {
  val dir = new File(s"$tablePath/.hoodie/timeline")
  if (!dir.exists()) Seq.empty
  else dir.listFiles().map(_.getName).filterNot(_.endsWith(".crc")).sorted.toSeq
}

def seed(tablePath: String): Unit = {
  val rows = Seq(
    ("r1", "NYC", 12.0), ("r2", "NYC", 14.5), ("r3", "SF", 22.0),
    ("r4", "SF", 9.0), ("r5", "NYC", 31.0)
  ).toDF("id", "city", "fare")
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", new File(tablePath).getName)
    .mode(SaveMode.Overwrite)
    .save(tablePath)
}

// ============================================================================
// 1. PASSING validation: SqlQueryEqualityPreCommitValidator, an invariant unaffected
//    by the write (the set of distinct cities), plus SqlQuerySingleResultPreCommitValidator
//    checking the exact row count after the write.
// ============================================================================
val passTable = s"$basePath/pass"
seed(passTable)
println(s"\n=== PASS scenario: insert 2 more rows, no new city ===")
val moreRows = Seq(("r6", "NYC", 8.0), ("r7", "SF", 19.0)).toDF("id", "city", "fare")
try {
  moreRows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", new File(passTable).getName)
    .option("hoodie.precommit.validators",
      "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator," +
      "org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator")
    .option("hoodie.precommit.validators.equality.sql.queries",
      "select distinct city from <TABLE_NAME> order by city")
    .option("hoodie.precommit.validators.single.value.sql.queries",
      "select count(*) from <TABLE_NAME>#7")
    .mode(SaveMode.Append)
    .save(passTable)
  println("Commit SUCCEEDED: both validators passed (city set unchanged; row count = 7 as expected).")
} catch {
  case e: Exception => println(s"Commit FAILED unexpectedly: ${e.getClass.getName}: ${e.getMessage}")
}
println("Timeline after PASS scenario: " + timelineFiles(passTable).mkString(", "))

// ============================================================================
// 2. FAILING validation: the same equality validator, but this write DELETES every SF row.
//
// IMPORTANT, discovered via a real run: SqlQueryEqualityPreCommitValidator does NOT check
// symmetric equality of the before/after query results. Its actual check (source:
// SqlQueryEqualityPreCommitValidator.validateUsingQuery) is
//   prevRows.intersect(newRows).count() == prevRows.count()
// i.e. "every row present BEFORE must still be present AFTER" -- a one-directional subset
// check. Adding a brand-new city (e.g. LA) does NOT fail it, because {NYC, SF} is still a
// subset of {LA, NYC, SF}. The only way to fail it is to make something from the BEFORE
// result disappear, which is what this scenario does: it deletes every SF row, so `select
// distinct city` goes from {NYC, SF} to {NYC} and the subset check breaks.
// ============================================================================
val failTable = s"$basePath/fail"
seed(failTable)
println(s"\n=== FAIL scenario: DELETE every SF row (makes a previously-present city vanish) ===")
println("Timeline before the failing write: " + timelineFiles(failTable).mkString(", "))
val deleteSfRows = Seq(("r3", "SF", 22.0), ("r4", "SF", 9.0)).toDF("id", "city", "fare")
try {
  deleteSfRows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "delete")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", new File(failTable).getName)
    .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
    .option("hoodie.precommit.validators.equality.sql.queries",
      "select distinct city from <TABLE_NAME> order by city")
    .mode(SaveMode.Append)
    .save(failTable)
  println("Commit SUCCEEDED (unexpected -- validator should have failed).")
} catch {
  case e: Exception =>
    println(s"Commit FAILED as expected.")
    println(s"Exception class:   ${e.getClass.getName}")
    println(s"Exception message: ${e.getMessage}")
    var cause = e.getCause
    while (cause != null) {
      println(s"  caused by: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
    }
}
println("Timeline after the failing write: " + timelineFiles(failTable).mkString(", "))
println("Row count after the failed write (should still be 5, the seed count): " +
  spark.read.format("hudi").load(failTable).count())

// ============================================================================
// 3. The 1.2.0 addition: hoodie.precommit.validators.failure.policy=WARN_LOG
//    Same failing validator, but configured to warn instead of blocking the commit.
// ============================================================================
val warnTable = s"$basePath/warn_policy"
seed(warnTable)
println(s"\n=== WARN_LOG policy scenario: same failing (SF-deleting) validator, but failure.policy=WARN_LOG ===")
try {
  deleteSfRows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "delete")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", new File(warnTable).getName)
    .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
    .option("hoodie.precommit.validators.equality.sql.queries",
      "select distinct city from <TABLE_NAME> order by city")
    .option("hoodie.precommit.validators.failure.policy", "WARN_LOG")
    .mode(SaveMode.Append)
    .save(warnTable)
  println("Commit SUCCEEDED under WARN_LOG policy (validator failure only logged).")
} catch {
  case e: Exception =>
    println(s"Commit FAILED even under WARN_LOG: ${e.getClass.getName}: ${e.getMessage}")
}
println("Row count under WARN_LOG policy (should be 3 -- 5 seed rows minus the 2 deleted SF rows -- if the delete went through): " +
  spark.read.format("hudi").load(warnTable).count())

System.exit(0)
