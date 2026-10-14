// Follow-up: does OCC's early conflict detection actually abort a doomed writer BEFORE it
// finishes writing, via the low-level SparkRDDWriteClient API? Writer1 is slowed down (a
// per-row sleep during its RDD write) so its marker for the target file group exists well
// before it finishes; Writer2 starts as soon as it observes Writer1's marker directory
// on disk, with hoodie.write.concurrency.early.conflict.detection.enable=true and DIRECT
// markers (synchronous check). Expect Writer2 to fail during its OWN upsert() call --
// i.e. before Writer1 has committed, or even finished writing -- with
// HoodieEarlyConflictDetectionException, not HoodieWriteConflictException.

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieAvroRecord, HoodieKey, HoodieRecord, HoodieTableType, WriteConcurrencyMode, HoodieFailedWritesCleaningPolicy}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaSparkContext

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/early_conflict_lowlevel_detection")
def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

val schemaStr =
  """{"type":"record","name":"rec","fields":[
    |{"name":"id","type":"string"},
    |{"name":"driver","type":"string"},
    |{"name":"fare","type":"double"},
    |{"name":"ts","type":"long"}
    |]}""".stripMargin
val schema = new Schema.Parser().parse(schemaStr)

def makeRecord(id: String, driver: String, fare: Double, ts: Long): HoodieRecord[HoodieAvroPayload] = {
  val gr = new GenericData.Record(schema)
  gr.put("id", id); gr.put("driver", driver); gr.put("fare", fare); gr.put("ts", ts)
  new HoodieAvroRecord(new HoodieKey(id, ""), new HoodieAvroPayload(org.apache.hudi.common.util.Option.of(gr)))
}

val jsc = new JavaSparkContext(spark.sparkContext)

HoodieTableMetaClient.newTableBuilder()
  .setTableType(HoodieTableType.COPY_ON_WRITE)
  .setTableName("early_conflict_lowlevel_detection")
  .setPayloadClass(classOf[HoodieAvroPayload])
  .setRecordKeyFields("id")
  .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), basePath)

def buildConfig(earlyDetection: Boolean): HoodieWriteConfig = HoodieWriteConfig.newBuilder()
  .withPath(basePath)
  .withSchema(schemaStr)
  .withParallelism(1, 1)
  .forTable("early_conflict_lowlevel_detection")
  .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
  .withCleanConfig(HoodieCleanConfig.newBuilder()
    .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
    .withAutoClean(false).build())
  .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build())
  .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
  .withMarkersType(MarkerType.DIRECT.name())
  .withEarlyConflictDetectionEnable(earlyDetection)
  .withLockConfig(HoodieLockConfig.newBuilder()
    .withLockProvider(classOf[InProcessLockProvider])
    .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy())
    .build())
  .build()

// --- Seed ---------------------------------------------------------------------------
val seedClient = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), buildConfig(false))
val seedInstant = seedClient.startCommit()
val seedRecords = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-$i", 10.0 + i, 1000L + i))
val seedRdd = jsc.parallelize(java.util.Arrays.asList(seedRecords: _*), 1)
val seedStatuses = seedClient.upsert(seedRdd, seedInstant)
seedStatuses.collect()
seedClient.commit(seedInstant, seedStatuses)
seedClient.close()
println(s"Seed committed: $seedInstant")

def markerDirHasEntries(): Boolean = {
  val dir = new File(s"$basePath/.hoodie/.temp")
  dir.exists() && dir.listFiles() != null && dir.listFiles().exists(d => {
    val f = new File(d, "")
    d.isDirectory && d.listFiles() != null && d.listFiles().nonEmpty
  })
}

val writeConfig = buildConfig(true)
val client1 = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), writeConfig)
val client2 = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), writeConfig)

val executors = Executors.newFixedThreadPool(2)
@volatile var writer1Result = "not run"
@volatile var writer2Result = "not run"
@volatile var writer2StartMs = -1L
val startNanos = System.nanoTime()

val fut1 = executors.submit(new Runnable {
  override def run(): Unit = {
    try {
      val instant1 = client1.startCommit()
      println(s"[Writer1] started instant $instant1 at t=${(System.nanoTime()-startNanos)/1000000}ms")
      // Slow per-row: sleep 800ms per record during the map/write phase (after marker creation).
      val records = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-1-$i", 20.0 + i, 2000L + i))
      val rdd = jsc.parallelize(java.util.Arrays.asList(records: _*), 1)
        .map(r => { Thread.sleep(800); r })
      val statuses = client1.upsert(rdd, instant1)
      statuses.collect()
      println(s"[Writer1] data written for $instant1 at t=${(System.nanoTime()-startNanos)/1000000}ms")
      val committed = client1.commit(instant1, statuses)
      writer1Result = s"success (commit returned $committed) at t=${(System.nanoTime()-startNanos)/1000000}ms"
    } catch {
      case e: Throwable => writer1Result = s"FAILED: ${e.getClass.getName}: ${e.getMessage} at t=${(System.nanoTime()-startNanos)/1000000}ms"
    }
  }
})

val fut2 = executors.submit(new Runnable {
  override def run(): Unit = {
    try {
      // Poll for Writer1's marker directory to appear (proxy for "Writer1's marker for the
      // target file group exists"), then start immediately -- no fixed sleep.
      val deadline = System.currentTimeMillis() + 15000
      while (!markerDirHasEntries() && System.currentTimeMillis() < deadline) Thread.sleep(10)
      writer2StartMs = (System.nanoTime() - startNanos) / 1000000
      println(s"[Writer2] observed Writer1's marker directory at t=${writer2StartMs}ms, starting now")
      val instant2 = client2.startCommit()
      println(s"[Writer2] started instant $instant2 at t=${(System.nanoTime()-startNanos)/1000000}ms")
      val records = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-2-$i", 30.0 + i, 3000L + i))
      val rdd = jsc.parallelize(java.util.Arrays.asList(records: _*), 1)
      val statuses = client2.upsert(rdd, instant2) // expect this call itself to throw
      statuses.collect()
      writer2Result = s"data write SUCCEEDED (unexpected) at t=${(System.nanoTime()-startNanos)/1000000}ms"
    } catch {
      case e: Throwable =>
        writer2Result = s"FAILED during upsert() (early abort) at t=${(System.nanoTime()-startNanos)/1000000}ms: ${e.getClass.getName}: ${e.getMessage}"
    }
  }
})

fut1.get(60, TimeUnit.SECONDS)
fut2.get(60, TimeUnit.SECONDS)
executors.shutdown()

println(s"\nWriter1 result: $writer1Result")
println(s"Writer2 result: $writer2Result")

client1.close()
client2.close()
System.exit(0)
