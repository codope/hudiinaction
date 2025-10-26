# Chapter 4: Querying Hudi Tables & Catalog Syncing

This chapter covers various querying capabilities with Hudi tables and demonstrates how to sync Hudi metadata to different catalogs and query engines. We use the TPC-DS store_sales dataset for all examples.

## 🚀 What You'll Learn

This chapter demonstrates:

### **Part 1: Querying Hudi Tables**
- **Creating a Hudi table**: Initial loading of data using bulk_insert
- **Querying via Spark DataSource**: Reading Hudi table using spark.read.format("hudi")
- **Querying via Spark SQL**: Reading Hudi table using SQL queries
- **Querying via Spark Structured Streaming**: Reading a Hudi table using spark streaming

### **Part 2: Catalog Syncing & Multi-Engine Queries**
- **Hive Metastore (HMS) Sync**: Sync to Hive and query via HiveQL
- **AWS Glue Sync**: Sync to Glue Data Catalog and query via Athena
- **BigQuery Sync**: Sync to BigQuery and query via bq CLI or Spark BigQuery connector
- **DataHub Sync**: Sync metadata to DataHub for governance and lineage
- **XTable Sync**: Expose Hudi tables to Iceberg/Delta readers

## 📊 Sample Dataset

This chapter uses a sample of the store-sales dataset from tpc-ds benchmarking containing approximately 2.8M records. The data includes:

- **ss_item_sk** (unique identifier)
- **Item information** 
- **Customer information**
- **Address related info**
- **Price related stats**
- **Profit related stats**

## 🛠️ Setup Instructions

### 1. Download Sample Data
The input data is located in `chapter04/input_data/` directory (5 parquet files with ~2.8M records).

### 2. Update Configuration
Edit the Scala files to update paths according to your environment:
```scala
// Update in hudi_querying_tutorial.scala and metasync/*.scala
val inputPath = "/tmp/input_data/"  // Or path to chapter04/input_data/
val basePath  = "/tmp/hudi_demo_tbl"  // Or your preferred location
```

### 3. Start Spark Shell
```bash
spark-shell --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
            --driver-memory 4g --executor-memory 4g \
            --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
            --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
            --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
            --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

### 4. Run the Tutorial

**Part 1 - Basic Querying:**
Execute code from `hudi_querying_tutorial.scala` interactively by copying sections into spark-shell.

**Part 2 - Catalog Syncing:**
Choose the catalog sync script(s) relevant to your use case from the `metasync/` directory and follow the setup instructions in each script.

## 📝 Tutorial Steps

### Part 1: Basic Querying (`hudi_querying_tutorial.scala`)

#### Section 1: Data Loading with bulk_insert
- Load store_sales dataset from input_data
- Bulk import entire dataset into Hudi table (2.8M records → 50 data files)

#### Section 2: Querying via Spark DataSource
- Query the Hudi table using `spark.read.format("hudi")`
- Register as temp table and run SQL queries
- View execution stats in Spark UI

#### Section 3: Querying via Spark SQL
- Launch spark-sql session
- Create external table mapping to Hudi location
- Query using pure SQL syntax

#### Section 4: Querying via Spark Structured Streaming
- Set up streaming query with `readStream`
- Process micro-batches using `foreachBatch`
- Configure checkpoints and triggers

### Part 2: Catalog Syncing & Multi-Engine Queries (`metasync/*.scala`)

#### 01_hms_sync.scala - Hive Metastore Sync
- **Setup**: Docker-based HMS (Thrift @ 9083) or existing Hive setup
- **Write-time sync**: Configure `hoodie.datasource.hive_sync.mode=hms`
- **CLI sync**: Use `run_sync_tool.sh` for independent syncing
- **Query**: Via Spark with Hive catalog or Beeline

#### 02_glue_sync.scala - AWS Glue Data Catalog Sync
- **Setup**: EMR or Spark with AWS credentials, S3 bucket
- **Write-time sync**: Configure `hoodie.datasource.hive_sync.mode=glue`
- **CLI sync**: Use `AwsGlueCatalogSyncTool` or run_sync_tool.sh
- **Query**: Via AWS Athena (SQL or aws-cli)

#### 03_bigquery_sync.scala - Google BigQuery Sync
- **Setup**: GCP project, GCS bucket, service account credentials
- **Write-time sync**: Configure `hoodie.datasource.meta.sync.classes=BigQuerySyncTool`
- **Partitioning**: Example with ss_store_sk_part partition field
- **CLI sync**: Use java -cp with hudi-gcp-bundle
- **Query**: Via bq CLI or Spark BigQuery connector

#### 04_datahub_sync.scala - DataHub Metadata Sync
- **Setup**: Install DataHub OSS (`datahub quickstart`)
- **Write-time sync**: Configure `hoodie.datasource.meta.sync.classes=DataHubSyncTool`
- **CLI sync**: Use run_sync_tool.sh with --sync-mode datahub
- **Verify**: DataHub UI @ http://localhost:9002

#### 05_xtable_sync.scala - XTable (Iceberg/Delta) Sync
- **Setup**: Add XTable bundle to classpath
- **Write-time sync**: Configure `hoodie.datasource.meta.sync.classes=XTableSyncTool`
- **Target formats**: iceberg, delta (comma-separated)
- **CLI sync**: Use XTable utilities JAR independently
- **Query**: Via Iceberg/Delta readers on Spark

## 🎯 Expected Outcomes

After completing this chapter, you'll have:

### Part 1: Basic Querying
- A functioning Hudi table with 2.8M store_sales records
- Experience querying via Spark DataSource, Spark SQL, and Structured Streaming
- Understanding of different read patterns and use cases

### Part 2: Catalog Syncing
- Knowledge of how to sync Hudi metadata to various catalogs
- Ability to query Hudi tables from multiple query engines:
  - **Hive/HiveQL** (via HMS)
  - **AWS Athena** (via Glue)
  - **Google BigQuery** (via BigQuery connector)
  - **Iceberg/Delta readers** (via XTable)
- Understanding of metadata governance with DataHub
- Experience with both write-time and independent CLI-based syncing

## 📁 Files in This Chapter

### Main Tutorial
- **`hudi_querying_tutorial.scala`** - Complete tutorial demonstrating Spark DataSource, Spark SQL, and Structured Streaming queries

### Catalog Syncing Scripts (metasync/)
- **`01_hms_sync.scala`** - Hive Metastore sync with Docker setup instructions
- **`02_glue_sync.scala`** - AWS Glue Data Catalog sync with Athena queries
- **`03_bigquery_sync.scala`** - Google BigQuery sync with partitioned table example
- **`04_datahub_sync.scala`** - DataHub metadata sync for governance/lineage
- **`05_xtable_sync.scala`** - XTable sync to expose as Iceberg/Delta tables

### Data
- **`input_data/`** - Sample TPC-DS store_sales dataset (5 parquet files, ~2.8M records)

### Documentation
- **`README.md`** - This chapter guide

## 📚 Further Reading

### General Hudi Documentation
- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Hudi Configuration Guide](https://hudi.apache.org/docs/configurations/)
- [Spark-Hudi Integration](https://hudi.apache.org/docs/quick-start-guide/)
- [SQL Queries on Different Engines](https://hudi.apache.org/docs/sql_queries)

### Catalog Syncing Documentation
- [Hive Metastore Sync](https://hudi.apache.org/docs/syncing_metastore/)
- [AWS Glue Data Catalog Sync](https://hudi.apache.org/docs/syncing_aws_glue_data_catalog/)
- [Google BigQuery Sync](https://hudi.apache.org/docs/gcp_bigquery/)
- [DataHub Sync](https://hudi.apache.org/docs/syncing_datahub/)
- [XTable (Multi-Format) Sync](https://hudi.apache.org/docs/syncing_xtable/)
- [Apache XTable Project](https://xtable.apache.org/)

## 💡 Tips & Best Practices

1. **Choose the right sync method**: Use write-time sync for automatic updates or CLI sync for scheduled/manual updates
2. **Start with HMS**: If you're new to catalog syncing, start with Hive Metastore (easiest setup with Docker)
3. **Cloud-specific considerations**:
   - AWS: Use Glue for seamless Athena integration
   - GCP: Use BigQuery sync for cost-effective querying of GCS data
   - Multi-cloud: Consider XTable for maximum flexibility
4. **Governance**: Use DataHub to track lineage, schema evolution, and data quality
5. **Performance**: For large tables, enable partition pruning in your queries and catalogs
