// Follow-up: exercising the 1.2.0 pre-write validator framework (hoodie.prewrite.validators /
// org.apache.hudi.client.validator.PreWriteValidator), distinct from the older pre-commit SQL
// validators. There is no shipped generic validator class for this interface (only
// StreamingOffsetValidator, which needs Kafka/Flink streaming offsets to be meaningful), so
// this defines a minimal custom validator inline via the spark-shell REPL and references it
// by its REPL-generated class name.

import java.io.File
import org.apache.spark.sql.SaveMode
import spark.implicits._

val basePath = sys.env.getOrElse("BASE_PATH", "/tmp/hudiinaction/chapter09/prewrite_validator")
def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

// A minimal PreWriteValidator: always fails, unconditionally, just to prove the hook fires.
class AlwaysFailPreWriteValidator extends org.apache.hudi.client.validator.PreWriteValidator {
  override def validate[T](instantTime: String,
                            writeOperationType: org.apache.hudi.common.model.WriteOperationType,
                            metaClient: org.apache.hudi.common.table.HoodieTableMetaClient,
                            writeConfig: org.apache.hudi.config.HoodieWriteConfig,
                            engineContext: org.apache.hudi.common.engine.HoodieEngineContext,
                            recordsOpt: org.apache.hudi.common.util.Option[org.apache.hudi.common.data.HoodieData[org.apache.hudi.common.model.HoodieRecord[T]]]): Unit = {
    println(s"AlwaysFailPreWriteValidator.validate() CALLED for instant=$instantTime op=$writeOperationType recordsPresent=${recordsOpt.isPresent}")
    throw new org.apache.hudi.exception.HoodieValidationException("AlwaysFailPreWriteValidator intentionally failing this write")
  }
}

val validatorClassName = classOf[AlwaysFailPreWriteValidator].getName
println(s"Registering pre-write validator class: $validatorClassName")

val df = Seq(("r1", "NYC", 12.0)).toDF("id", "city", "fare")
try {
  df.write.format("hudi")
    .option("hoodie.datasource.write.operation", "bulk_insert")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.table.ordering.fields", "id")
    .option("hoodie.table.name", "prewrite_test")
    .option("hoodie.prewrite.validators", validatorClassName)
    .mode(SaveMode.Overwrite)
    .save(basePath)
  println("Write SUCCEEDED (unexpected -- pre-write validator should have blocked it).")
} catch {
  case e: Throwable =>
    println(s"Write FAILED as expected.")
    println(s"Exception class:   ${e.getClass.getName}")
    println(s"Exception message: ${e.getMessage}")
    var cause = e.getCause
    while (cause != null) {
      println(s"  caused by: ${cause.getClass.getName}: ${cause.getMessage}")
      cause = cause.getCause
    }
}

System.exit(0)
