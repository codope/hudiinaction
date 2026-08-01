/**
 * Apache Hudi Unstructured Data Quickstart
 *
 * Demonstrates storing and querying unstructured data (embeddings + binary blobs)
 * in a Hudi table using VECTOR and BLOB column types (Hudi 1.2+).
 *
 * This script covers:
 *   - Creating a table with VECTOR and BLOB columns
 *   - Inserting structured metadata alongside embeddings and raw bytes
 *   - Running vector similarity search with hudi_vector_search()
 *   - Materializing BLOBs with read_blob()
 *
 * Requirements:
 *   - Spark 3.5.x with Hudi 1.2.0+ bundle
 *   - Launch via: ./run_spark_shell.sh
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

val unstructuredBasePath = "/tmp/product_catalog"

// ============================================================================
// SECTION 1: CREATE A TABLE WITH VECTOR AND BLOB COLUMNS
// ============================================================================

// Create a product catalog table that stores images alongside their embeddings.
// VECTOR(512) holds a 512-dimensional embedding as a fixed-length array.
// BLOB holds raw binary data (e.g., image bytes) — Hudi stores small BLOBs
// inline in the Parquet file and larger ones as external references.
spark.sql(s"""
  CREATE TABLE IF NOT EXISTS product_catalog (
      product_id    STRING,
      name          STRING,
      category      STRING,
      price         DECIMAL(10, 2),
      embedding     VECTOR(512),
      image         BLOB,
      created_at    TIMESTAMP,
      PRIMARY KEY (product_id) NOT ENFORCED
  ) USING hudi
  TBLPROPERTIES (
      'type'         = 'cow',
      'primaryKey'   = 'product_id'
  )
  LOCATION '$unstructuredBasePath'
""")

// ============================================================================
// SECTION 2: INSERT SAMPLE DATA
// ============================================================================

// For demonstration, we insert rows with synthetic embeddings (random vectors)
// and a small placeholder for the BLOB column. In production, embeddings would
// come from a model (e.g., MobileNet, CLIP) and BLOBs from actual image files.

import org.apache.spark.sql.functions._

// Generate synthetic 512-dim embeddings
val syntheticEmbedding = array((1 to 512).map(_ => lit(scala.util.Random.nextFloat())): _*)

val products = Seq(
  ("P001", "Wireless Headphones", "electronics", 79.99),
  ("P002", "Running Shoes", "footwear", 129.95),
  ("P003", "Bluetooth Speaker", "electronics", 49.99),
  ("P004", "Hiking Boots", "footwear", 189.00),
  ("P005", "Laptop Stand", "accessories", 34.99)
).toDF("product_id", "name", "category", "price")

val productsWithEmbeddings = products
  .withColumn("embedding", syntheticEmbedding)
  .withColumn("image", lit(Array.fill[Byte](100)(0)))  // placeholder bytes
  .withColumn("created_at", current_timestamp())

productsWithEmbeddings.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field", "product_id")
  .option("hoodie.table.name", "product_catalog")
  .mode("Append")
  .save(unstructuredBasePath)

println(s"Inserted ${productsWithEmbeddings.count()} products with embeddings")

// ============================================================================
// SECTION 3: VECTOR SIMILARITY SEARCH
// ============================================================================

// Find the 3 products most similar to a query embedding using cosine similarity.
// hudi_vector_search() scans the embedding column and returns the top-K matches.
val results = spark.sql("""
  SELECT product_id, name, category, price
  FROM hudi_vector_search(
      'product_catalog',
      'embedding',
      ARRAY(""" + (1 to 512).map(_ => scala.util.Random.nextFloat().toString).mkString(", ") + """),
      3
  )
""")

println("Top-3 similar products:")
results.show(truncate = false)

// ============================================================================
// SECTION 4: MATERIALIZING BLOBS
// ============================================================================

// read_blob() transparently resolves BLOB data regardless of storage mode
// (inline or external reference). This query retrieves both metadata and
// the raw image bytes in a single pass.
val withBlobs = spark.sql("""
  SELECT product_id, name, read_blob(image) AS image_bytes
  FROM product_catalog
  WHERE category = 'electronics'
""")

withBlobs.select("product_id", "name").show()
println(s"Image bytes length for first result: ${withBlobs.first().getAs[Array[Byte]]("image_bytes").length}")

// ============================================================================
// CLEANUP (optional)
// ============================================================================

// spark.sql("DROP TABLE IF EXISTS product_catalog")
