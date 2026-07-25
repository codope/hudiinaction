# Chapter 2: Apache Hudi Essentials and Quickstart

This chapter walks through a complete Hudi pipeline end to end — creating tables, performing
upserts and deletes, querying data four ways, and running table maintenance — using the NYC
Taxi dataset.

## Quick Start

### Option A: Docker (recommended)

One command gets you a Jupyter notebook with Spark + Hudi pre-configured:

```bash
cd chapter02/docker
cp ../trips_0.gz data/ && gunzip data/trips_0.gz
docker compose up
```

Open http://localhost:8888 and run `hudi_quickstart_pyspark.ipynb`.

### Option B: Local Spark Shell

1. **Extract the sample data:**
   ```bash
   cd chapter02
   gunzip trips_0.gz
   ```

2. **Update paths** in `hudi_pipeline_quickstart.scala`:
   ```scala
   val inputPath = "/path/to/hudiinaction/chapter02/trips_0"
   val basePath  = "/tmp/trips_table"
   ```

3. **Start Spark Shell:**
   ```bash
   ./run_spark_shell.sh
   ```

4. **Run the tutorial:**
   ```scala
   :load hudi_pipeline_quickstart.scala
   ```

## Prerequisites

- Java SDK: 11 (LTS). Ensure `JAVA_HOME` is set.
- [Apache Spark 3.5.6](https://spark.apache.org/downloads.html) (pre-built with Hadoop 3.3)
- Apache Hudi 1.2.0 (pulled automatically via `--packages`; see
  [version compatibility](https://hudi.apache.org/docs/quick-start-guide))
- Hardware: 8 GB RAM and 10 GB free disk recommended
- OS: macOS, Linux, or WSL 2 on Windows

## Sample Dataset

NYC Taxi dataset sample (~1M trip records): trip IDs, vendor IDs, timestamps, distances,
fares, and coordinates. Included as `trips_0.gz`.

## What's Covered

| Section | Topic |
|---------|-------|
| 1 | Data loading and CoW table creation |
| 2 | Upsert operations and verification |
| 3 | Delete operations |
| 4 | Commit timeline and metadata exploration |
| 5 | Query types: snapshot, read-optimized, incremental, time-travel |
| 6 | Merge-on-Read (MoR) table operations |
| 7 | Table maintenance: compaction, clustering, cleaning |

## Files

| File | Description |
|------|-------------|
| `hudi_pipeline_quickstart.scala` | Complete Scala tutorial script |
| `docker/notebooks/hudi_quickstart_pyspark.ipynb` | PySpark Jupyter notebook (same examples) |
| `docker/` | Docker Compose setup (Spark + Hudi + Jupyter) |
| `run_spark_shell.sh` | Convenience script to launch spark-shell with Hudi |
| `trips_0.gz` | NYC Taxi dataset sample (compressed) |

## Troubleshooting

- **File Not Found:** Ensure `trips_0.gz` is extracted before running.
- **Memory Errors:** Increase driver memory with `--driver-memory 4g`.
- **Package Conflicts:** Use the exact Hudi bundle version for your Spark version.
