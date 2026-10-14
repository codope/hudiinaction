// Follow-up: reproduce the OCC conflict using the LOW-LEVEL SparkRDDWriteClient API,
// mirroring the exact orchestration Hudi's own TestHoodieClientMultiWriter.
// testHoodieClientBasicMultiWriter uses (a CyclicBarrier making writer2's data-write phase
// happen concurrently with writer1's commit, then writer2 attempts its own commit only
// AFTER writer1's has already landed). The Spark Datasource API
// (df.write.format("hudi")) version of this test, tried three times with different marker
// types and a filesystem-polling start barrier, never reproduced a conflict; this checks
// whether the low-level client API does, to narrow down whether the issue is specific to
// the Datasource write path.

import java.io.File
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieAvroRecord, HoodieKey, HoodieRecord, HoodieTableType, WriteConcurrencyMode}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaSparkContext

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/early_conflict_lowlevel")
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
  val key = new HoodieKey(id, "")
  new HoodieAvroRecord(key, new HoodieAvroPayload(org.apache.hudi.common.util.Option.of(gr)))
}

val jsc = new JavaSparkContext(spark.sparkContext)

HoodieTableMetaClient.newTableBuilder()
  .setTableType(HoodieTableType.COPY_ON_WRITE)
  .setTableName("early_conflict_lowlevel")
  .setPayloadClass(classOf[HoodieAvroPayload])
  .setRecordKeyFields("id")
  .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), basePath)

def buildConfig(): HoodieWriteConfig = HoodieWriteConfig.newBuilder()
  .withPath(basePath)
  .withSchema(schemaStr)
  .withParallelism(1, 1)
  .forTable("early_conflict_lowlevel")
  .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
  .withCleanConfig(HoodieCleanConfig.newBuilder()
    .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
    .withAutoClean(false).build())
  .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build())
  .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
  .withMarkersType(MarkerType.DIRECT.name())
  .withLockConfig(HoodieLockConfig.newBuilder()
    .withLockProvider(classOf[InProcessLockProvider])
    .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy())
    .build())
  .build()

// --- Seed: 10 rows, single commit -----------------------------------------------------
val seedConfig = buildConfig()
val seedClient = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), seedConfig)
val seedInstant = seedClient.startCommit()
println(s"Seed instant: $seedInstant")
val seedRecords = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-$i", 10.0 + i, 1000L + i))
val seedRdd = jsc.parallelize(java.util.Arrays.asList(seedRecords: _*), 1)
val seedStatuses = seedClient.upsert(seedRdd, seedInstant)
seedStatuses.collect() // force execution
val seedCommitted = seedClient.commit(seedInstant, seedStatuses)
println(s"Seed committed = $seedCommitted")
seedClient.close()

// --- Two concurrent writers, low-level API, mirroring TestHoodieClientMultiWriter -----
val writeConfig = buildConfig()
val client1 = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), writeConfig)
val client2 = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), writeConfig)

val barrier = new CyclicBarrier(2)
val executors = Executors.newFixedThreadPool(2)
@volatile var writer1Result = "not run"
@volatile var writer2Result = "not run"

val fut1 = executors.submit(new Runnable {
  override def run(): Unit = {
    try {
      val instant1 = client1.startCommit()
      println(s"[Writer1] started instant $instant1")
      val records = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-1-$i", 20.0 + i, 2000L + i))
      val rdd = jsc.parallelize(java.util.Arrays.asList(records: _*), 1)
      val statuses = client1.upsert(rdd, instant1)
      statuses.collect()
      println(s"[Writer1] data written for $instant1, waiting at barrier #1")
      barrier.await(60, TimeUnit.SECONDS)
      val committed = client1.commit(instant1, statuses)
      println(s"[Writer1] commit($instant1) returned $committed")
      writer1Result = s"success (commit returned $committed)"
      barrier.await(60, TimeUnit.SECONDS)
    } catch {
      case e: Throwable => writer1Result = s"FAILED: ${e.getClass.getName}: ${e.getMessage}"
    }
  }
})

val fut2 = executors.submit(new Runnable {
  override def run(): Unit = {
    try {
      barrier.await(60, TimeUnit.SECONDS)
      val instant2 = client2.startCommit()
      println(s"[Writer2] started instant $instant2 (after writer1's data write, before writer1's commit lands)")
      val records = (1 to 10).map(i => makeRecord(s"id-$i", s"driver-2-$i", 30.0 + i, 3000L + i))
      val rdd = jsc.parallelize(java.util.Arrays.asList(records: _*), 1)
      val statuses = client2.upsert(rdd, instant2)
      statuses.collect()
      println(s"[Writer2] data written for $instant2, waiting at barrier #2 (writer1 should commit now)")
      barrier.await(60, TimeUnit.SECONDS)
      try {
        val committed = client2.commit(instant2, statuses)
        writer2Result = s"succeeded (unexpected) commit returned $committed"
      } catch {
        case e: Throwable =>
          writer2Result = s"FAILED as expected: ${e.getClass.getName}: ${e.getMessage}"
      }
    } catch {
      case e: Throwable => writer2Result = s"setup FAILED: ${e.getClass.getName}: ${e.getMessage}"
    }
  }
})

fut1.get(90, TimeUnit.SECONDS)
fut2.get(90, TimeUnit.SECONDS)
executors.shutdown()

println(s"\nWriter1 result: $writer1Result")
println(s"Writer2 result: $writer2Result")

client1.close()
client2.close()

val metaClient = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf()).setBasePath(basePath).build()
metaClient.reloadActiveTimeline()
println("\nFinal timeline instants: " + metaClient.getActiveTimeline.getInstants.toArray.mkString(", "))

System.exit(0)
