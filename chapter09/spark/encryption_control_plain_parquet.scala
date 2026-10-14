// Follow-up 1: plain Spark (no Hudi) control for the Parquet modular encryption recipe.
import org.apache.spark.sql.SaveMode
import spark.implicits._
import java.io.File

val basePath = "/tmp/hudiinaction/chapter09/encryption_control"
def deleteRecursively(f: File): Unit = { if (f.isDirectory) f.listFiles().foreach(deleteRecursively); f.delete() }
deleteRecursively(new File(basePath))

val hadoopConf = spark.sparkContext.hadoopConfiguration
println("=== Effective Hadoop crypto config seen by the driver ===")
Seq("parquet.crypto.factory.class","parquet.encryption.kms.client.class","parquet.encryption.footer.key","parquet.encryption.column.keys").foreach(k => println(s"$k = ${hadoopConf.get(k)}"))

val df = Seq(("r1","rider-A","NYC",12.5), ("r2","rider-B","SF",22.75)).toDF("id","rider","city","fare")
println("\n=== Plain df.write.parquet (no Hudi) with fare encrypted ===")
try {
  df.write.mode(SaveMode.Overwrite).parquet(basePath)
  println("Plain Spark Parquet write SUCCEEDED.")
} catch {
  case e: Throwable => println(s"Plain Spark Parquet write FAILED: ${e.getClass.getName}: ${e.getMessage}")
}

println("\n=== Reading it back WITH keys, same session ===")
try {
  spark.read.parquet(basePath).select("id","rider","city","fare").orderBy("id").show(false)
  println("Read WITH keys SUCCEEDED.")
} catch {
  case e: Throwable => println(s"Read WITH keys FAILED: ${e.getClass.getName}: ${e.getMessage}")
}
System.exit(0)
