package com.hudiinaction.chapter06.governance

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.SparkSession

object SavepointManager {

  private val RawTable = "hudi_payments_raw"
  private val SilverTable = "hudi_vendor_payouts"
  private val RawPath = "/tmp/hudi/raw/payments"
  private val SilverPath = "/tmp/hudi/silver/vendor_payouts"
  private val SavepointFile = "/tmp/hudi/chapter06/savepoints.env"

  def main(args: Array[String]): Unit = {
    val mode = if (args.nonEmpty) args(0) else "create"

    val spark = SparkSession.builder()
      .appName("Chapter06SavepointManager")
      .getOrCreate()

    registerTables(spark)

    mode match {
      case "create" =>
        val rawCommit = latestCommit(spark, RawTable)
        val silverCommit = latestCommit(spark, SilverTable)

        spark.sql(s"CALL create_savepoint(table => '$RawTable', commit_time => '$rawCommit')")
        spark.sql(s"CALL create_savepoint(table => '$SilverTable', commit_time => '$silverCommit')")

        persistSavepoints(rawCommit, silverCommit)

        println(s"Created raw savepoint   : $rawCommit")
        println(s"Created silver savepoint: $silverCommit")
        println(s"Saved metadata to       : $SavepointFile")

      case "restore" =>
        val (rawSavepoint, silverSavepoint) = loadSavepoints()

        spark.sql(
          s"CALL rollback_to_savepoint(table => '$RawTable', instant_time => '$rawSavepoint')"
        )
        spark.sql(
          s"CALL rollback_to_savepoint(table => '$SilverTable', instant_time => '$silverSavepoint')"
        )

        println(s"Restored raw    to $rawSavepoint")
        println(s"Restored silver to $silverSavepoint")

      case other =>
        throw new IllegalArgumentException(
          s"Unsupported mode '$other'. Use 'create' or 'restore'."
        )
    }
  }

  private def registerTables(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $RawTable
         |USING hudi
         |LOCATION '$RawPath'
         |""".stripMargin
    )

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $SilverTable
         |USING hudi
         |LOCATION '$SilverPath'
         |""".stripMargin
    )
  }

  private def latestCommit(spark: SparkSession, table: String): String = {
    val commitsDf = spark.sql(s"CALL show_commits(table => '$table')")
    val commitTimes = commitsDf.collect().map(_.getString(0)).filter(_ != null)

    if (commitTimes.isEmpty) {
      throw new IllegalStateException(s"No commits found for table $table")
    }

    commitTimes.sorted.last
  }

  private def persistSavepoints(rawCommit: String, silverCommit: String): Unit = {
    val path = Paths.get(SavepointFile)
    Files.createDirectories(path.getParent)

    val content =
      s"""RAW_SAVEPOINT=$rawCommit
         |SILVER_SAVEPOINT=$silverCommit
         |""".stripMargin

    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def loadSavepoints(): (String, String) = {
    val path = Paths.get(SavepointFile)
    if (!Files.exists(path)) {
      throw new IllegalStateException(
        s"Missing $SavepointFile. Run create mode first."
      )
    }

    val lines = Files.readAllLines(path, StandardCharsets.UTF_8).toArray.mkString("\n")
    val raw = lines.split("\n").find(_.startsWith("RAW_SAVEPOINT="))
      .map(_.stripPrefix("RAW_SAVEPOINT="))
      .getOrElse(throw new IllegalStateException("RAW_SAVEPOINT not found"))

    val silver = lines.split("\n").find(_.startsWith("SILVER_SAVEPOINT="))
      .map(_.stripPrefix("SILVER_SAVEPOINT="))
      .getOrElse(throw new IllegalStateException("SILVER_SAVEPOINT not found"))

    (raw, silver)
  }
}
