ThisBuild / organization := "com.hudiinaction"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "chapter06-marketplace-payout-lakehouse",
    Compile / fork := true,
    Test / fork := true,
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding",
      "utf8"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % Provided,
      "org.apache.hudi" %% "hudi-spark3.5-bundle" % "0.15.0" % Provided,
      "org.apache.hudi" %% "hudi-utilities-bundle" % "0.15.0" % Provided,
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
