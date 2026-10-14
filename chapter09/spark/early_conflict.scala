// Chapter 9, "Detecting conflicts before the write finishes" - early conflict detection
// under optimistic concurrency control (OCC), contrasted with plain OCC (conflict caught
// only at commit).
//
// Two writers in the SAME JVM (this spark-shell process), coordinated with
// InProcessLockProvider (safe for two writers inside one process on any filesystem,
// including object storage; it is NOT a substitute for a distributed lock across separate
// processes/hosts, where FileSystemBasedLockProvider or a cloud lock provider is needed
// instead -- and FileSystemBasedLockProvider itself is safe on local POSIX filesystems only,
// not on S3/GCS, per Hudi's own docs).
//
// Both writers upsert the SAME record keys, which live in the SAME single file (the initial
// load is coalesced to one file), so both target the same file group and a conflict is
// certain, not probabilistic. Writer A is slowed down with a per-row sleep UDF. Writer B does
// NOT start on a fixed timer: it polls the table's own timeline directory and only begins
// once Writer A's instant is visibly ".inflight", so the "B starts before A commits" ordering
// is enforced by the filesystem, not by guessing a sleep duration.
//
// v2 fixes applied after the v1 run showed no conflict in either scenario:
//   - hoodie.clean.failed.writes.policy (canonical key; v1 used the older alias
//     hoodie.cleaner.policy.failed.writes, which HoodieCleanConfig lists as an alternative
//     for the same ConfigProperty, so this was not actually the bug, but the canonical key
//     is used here to remove it as a variable).
//   - hoodie.write.markers.type explicitly set to DIRECT for the early-detection scenario
//     (Spark's actual default is TIMELINE_SERVER_BASED, whose detection strategy polls
//     asynchronously every hoodie.write.concurrency.async.conflict.detector.period_ms,
//     default 30000ms -- far longer than this example can wait. DIRECT markers use
//     SimpleDirectMarkerBasedDetectionStrategy, which checks synchronously.).
//   - the filesystem-polling start barrier described above, replacing a fixed 2s sleep.

import java.io.File
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/early_conflict")

def deleteRecursively(f: File): Unit = {
  if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
  f.delete()
}
deleteRecursively(new File(basePath))

implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

val slowIdentity = udf((s: String, ms: Long) => { Thread.sleep(ms); s })

def seedTable(tablePath: String): Unit = {
  val rows = (1 to 10).map(i => (s"id-$i", s"driver-$i", 10.0 + i, 1000L + i))
    .toDF("id", "driver", "fare", "ts")
    .coalesce(1) // force exactly one file group so both writers are guaranteed to collide
  rows.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "ts")
    .option("hoodie.table.name", new File(tablePath).getName)
    .option("hoodie.bulkinsert.shuffle.parallelism", "1")
    .mode(SaveMode.Overwrite)
    .save(tablePath)
}

def commonWriteOptions(tablePath: String, earlyDetection: Boolean, markerType: String): Map[String, String] = Map(
  "hoodie.datasource.write.operation" -> "upsert",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.table.ordering.fields" -> "ts",
  "hoodie.table.name" -> new File(tablePath).getName,
  "hoodie.upsert.shuffle.parallelism" -> "1",
  "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
  "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
  "hoodie.clean.failed.writes.policy" -> "LAZY",
  "hoodie.write.markers.type" -> markerType,
  "hoodie.write.concurrency.early.conflict.detection.enable" -> earlyDetection.toString
)

def timelineEntries(tablePath: String): Seq[String] = {
  val dir = new File(s"$tablePath/.hoodie/timeline")
  if (!dir.exists()) Seq.empty
  else dir.listFiles().map(_.getName).filterNot(_.endsWith(".crc")).sorted.toSeq
}

def rootCauseName(t: Throwable): String = {
  var cur = t
  while (cur.getCause != null && cur.getCause != cur) cur = cur.getCause
  cur.getClass.getName
}
def rootCauseMessage(t: Throwable): String = {
  var cur = t
  while (cur.getCause != null && cur.getCause != cur) cur = cur.getCause
  cur.getMessage
}

def runScenario(label: String, earlyDetection: Boolean, markerType: String): Unit = {
  val tag = s"${if (earlyDetection) "early" else "plain"}_${markerType.toLowerCase}"
  val tablePath = s"$basePath/$tag"
  seedTable(tablePath)
  println(s"\n############################################################")
  println(s"# Scenario: $label (early.detection=$earlyDetection, markers=$markerType)")
  println(s"############################################################")
  println(s"Timeline right after seed: ${timelineEntries(tablePath).mkString(", ")}")

  val opts = commonWriteOptions(tablePath, earlyDetection, markerType)

  def writerA(): (Boolean, Long, String) = {
    val t0 = System.nanoTime()
    try {
      val df = (1 to 10).map(i => (s"id-$i", s"driver-A-$i", 20.0 + i, 2000L + i)).toDF("id", "driver", "fare", "ts")
        .withColumn("driver", slowIdentity($"driver", org.apache.spark.sql.functions.lit(700L)))
        .coalesce(1)
      df.write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
      (true, (System.nanoTime() - t0) / 1000000, "")
    } catch {
      case e: Throwable =>
        (false, (System.nanoTime() - t0) / 1000000, s"${rootCauseName(e)}: ${rootCauseMessage(e)}")
    }
  }

  def writerB(): (Boolean, Long, String) = {
    // Poll the timeline directory instead of sleeping a fixed duration: only start once
    // Writer A's instant is visibly .inflight (i.e. A has begun writing but not committed).
    val deadline = System.currentTimeMillis() + 15000
    var sawInflight = false
    while (!sawInflight && System.currentTimeMillis() < deadline) {
      sawInflight = timelineEntries(tablePath).exists(_.endsWith(".inflight"))
      if (!sawInflight) Thread.sleep(20)
    }
    println(s"Writer B observed Writer A's instant inflight = $sawInflight; timeline now: ${timelineEntries(tablePath).mkString(", ")}")
    val t0 = System.nanoTime()
    try {
      val df = (1 to 10).map(i => (s"id-$i", s"driver-B-$i", 30.0 + i, 3000L + i)).toDF("id", "driver", "fare", "ts")
        .coalesce(1)
      df.write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
      (true, (System.nanoTime() - t0) / 1000000, "")
    } catch {
      case e: Throwable =>
        (false, (System.nanoTime() - t0) / 1000000, s"${rootCauseName(e)}: ${rootCauseMessage(e)}")
    }
  }

  val futA = Future(writerA())
  val futB = Future(writerB())
  val (okA, msA, errA) = Await.result(futA, 60.seconds)
  val (okB, msB, errB) = Await.result(futB, 60.seconds)

  println(f"Writer A: success=$okA%-5s elapsed=${msA}ms" + (if (!okA) s" error=$errA" else ""))
  println(f"Writer B: success=$okB%-5s elapsed=${msB}ms" + (if (!okB) s" error=$errB" else ""))
  println(s"Final timeline: ${timelineEntries(tablePath).mkString(", ")}")

  val finalCount = spark.read.format("hudi").load(tablePath).count()
  println(s"Final row count: $finalCount")
}

// --- Scenario 1: plain OCC, DIRECT markers, no early conflict detection ------------------
runScenario("OCC only, DIRECT markers", earlyDetection = false, markerType = "DIRECT")

// --- Scenario 2: OCC + early conflict detection, DIRECT markers (synchronous check) ------
runScenario("OCC + early conflict detection, DIRECT markers", earlyDetection = true, markerType = "DIRECT")

// --- Scenario 3: OCC + early conflict detection, TIMELINE_SERVER_BASED markers (async) ---
runScenario("OCC + early conflict detection, TIMELINE_SERVER_BASED markers", earlyDetection = true, markerType = "TIMELINE_SERVER_BASED")

System.exit(0)
