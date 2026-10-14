# Chapter 9: Advanced Lakehouse Capabilities

Chapter 9 is a survey of the capabilities a production lakehouse platform needs: semi-structured
and unstructured column types, indexing and similarity search, merge modes, concurrency control,
pre-commit validation and commit callbacks, schema evolution, table-version upgrade/downgrade, and
connecting a table to catalogs and other engines. This directory holds a runnable example for nearly
every one of those sections, verified against Hudi 1.2.1. A few examples back material that was
cut from the chapter (bootstrap, encryption, session options, Flink Source V2); they are listed
separately under **Examples not in the chapter**.

## Environment notes

- **Java 17** for every example (`export JAVA_HOME=$(/usr/libexec/java_home -v 17)` on macOS, or
  point at any JDK 17 install). Hudi 1.2.1 and Spark 3.5/4.1 both run on it.
- **No Docker needed** for any Spark example. `docker-compose.yml` remains as an optional container
  for the bootstrap example only, if you would rather not install Spark locally.
- **`--driver-class-path`, for the pre-commit validator examples only.** `SqlQueryEqualityPreCommitValidator`
  and friends run on the JVM's common `ForkJoinPool`, whose worker threads default their context
  classloader to the system classloader rather than the `MutableURLClassLoader` a plain
  `spark-shell --packages ...` populates — so a `--packages`-only session throws
  `ClassNotFoundException: HoodieCatalog` partway through. `scripts/spark_shell_with_hudi_driver_cp.sh`
  puts the resolved Hudi bundle jar on `--driver-class-path` as well, which fixes it. Use it (via
  `scripts/run_validators_driver_cp.sh`) instead of the plain launcher for the validator examples.
- **The `parquet-variant` jar, for VARIANT upserts on Spark 4.1 with Hudi 1.2.0 only.** Hudi 1.2.1
  shades it into `hudi-spark4.1-bundle_2.13`, so the launcher's extra jar is harmless there. On 1.2.0,
  Hudi's upsert path reads existing
  base files through `parquet-avro`'s `AvroReadSupport`, which reflectively loads
  `org.apache.parquet.variant.VariantConverters$ParentConverter` once a `VARIANT` field is present —
  a class neither Spark 4.1's own distribution nor `hudi-spark4.1-bundle_2.13` ships.
  `scripts/spark4_shell_with_hudi_variant.sh` adds `org.apache.parquet:parquet-variant:1.16.0` via
  `--packages` to fix it; every VARIANT example uses that launcher.
- **The Parquet test jar, for `InMemoryKMS`.** The encryption recipe's mock KMS client,
  `org.apache.parquet.crypto.keytools.mocks.InMemoryKMS`, ships only in
  `parquet-hadoop-1.13.1-tests.jar` (the `tests` classifier), not in the main `parquet-hadoop` jar
  Spark bundles and not resolvable through a plain `--packages group:artifact:version` coordinate.
  `scripts/spark_shell_with_hudi_encryption.sh` downloads that test jar once (cached under
  `~/spark-extra-jars/`) and adds it via `--jars`.
- **`spark.sql.catalogImplementation=in-memory`**, wherever a SQL write would otherwise trigger
  Hudi's default Hive-metastore sync against a metastore that does not exist (spark-shell defaults
  the flag to `"hive"`, which turns on sync for *any* SQL write regardless of which catalog actually
  created the table). Used by the schema-evolution and Polaris "working recipe" examples; noted per
  example below where it mattered.

## Versions

| Component | Version |
|-----------|---------|
| Apache Hudi | 1.2.1 |
| Apache Spark (most examples) | 3.5.6, Scala 2.12 |
| Apache Spark (VARIANT examples) | 4.1.3, Scala 2.13 |
| Apache Flink (RLI / Source V2 examples) | 1.20.3 |
| Apache Polaris (catalog examples) | 1.3.0-incubating server; **1.4.0** Spark client |

Override with `HUDI_VERSION`/`SPARK_BUNDLE` (per-launcher) if testing another combination.

## Example set

One row per example. "Prereqs" beyond the base Hudi bundle and a local Spark/Flink/Polaris install
are called out explicitly; see **Environment notes** above for what each one is for.

| Script | What it shows | Chapter section | Prereqs |
|---|---|---|---|
| `run_ai_types.sh` | `VECTOR` element types (`FLOAT`/`INT8`), `BLOB` INLINE/OUT_OF_LINE, `hudi_vector_search`/`_batch` (cosine/l2/dot_product) | *Storing semi-structured and unstructured data*, *Representing embeddings with VECTOR*, *Searching by similarity* | Spark 3.5 |
| `run_blob_support_tickets.sh` | The chapter's exact reduced `support_tickets` BLOB snippet, verbatim | *Storing binary payloads with BLOB* | Spark 3.5 |
| `run_variant_events.sh` | `VARIANT` column, COW + MOR, `parse_json`/`variant_get`, upsert-to-NULL known issue | *Holding semi-structured records with VARIANT* | Spark 4.1, `parquet-variant` jar |
| `run_variant_pushdown_mor.sh` | The `pushVariantIntoScan` MOR read issue and its workaround (run after `run_variant_events.sh`) | *Holding semi-structured records with VARIANT* | Spark 4.1, `parquet-variant` jar |
| `run_variant_inspect_parquet_schema.sh` | Inspects a COW base file's Parquet footer to see how a `VARIANT` column is physically laid out | *Holding semi-structured records with VARIANT* | Spark 4.1, `parquet-variant` jar |
| `run_variant_lance.sh` | Confirms `VARIANT` on a Lance-backed table fails with `HoodieNotSupportedException` | *Choosing a base file format for AI workloads* | Spark 4.1, `parquet-variant` jar, Lance Spark bundle |
| `run_lance.sh` | `hoodie.table.base.file.format=LANCE`, column/partition stats silently absent, vector search on Lance | *Choosing a base file format for AI workloads* | Spark 3.5, Lance Spark bundle |
| `run_lance_merger_int8.sh` | Resolves the record-merger config key a Lance table needs (canonical vs. deprecated vs. nonexistent), confirms `INT8` vectors fail on Lance | *Choosing a base file format for AI workloads* | Spark 3.5, Lance Spark bundle |
| `run_indexes.sh` | `CREATE`/`SHOW`/`DROP`/`REFRESH INDEX`, secondary/expression/bloom-filter indexes on a `rides` table | *Managing indexes with SQL*, *Looking up records by a non-key column*, *Indexing a derived value* | Spark 3.5, `--driver-memory 2g` |
| `run_index_pruning_evidence.sh` | Real scan-metric evidence (`numFiles`) that data skipping prunes files for a secondary-index and an expression-index predicate | *Looking up records by a non-key column*, *Indexing a derived value* | Spark 3.5 |
| `run_index_pruning_rides.sh` | The chapter's exact two pruning queries against the `rides`/`ride_id` naming | *Looking up records by a non-key column*, *Indexing a derived value* | Spark 3.5, `--driver-memory 2g` |
| `run_early_conflict.sh` | Datasource-API optimistic concurrency control (OCC) baseline, two writers in one JVM | *Detecting conflicts before the write finishes* | Spark 3.5 |
| `run_early_conflict_lowlevel.sh` | The same conflict via the low-level `SparkRDDWriteClient` API, mirroring Hudi's own multi-writer test | *Detecting conflicts before the write finishes* | Spark 3.5 |
| `run_early_conflict_lowlevel_detection.sh` | Early conflict detection aborting a doomed writer before it finishes, via markers | *Detecting conflicts before the write finishes* | Spark 3.5 |
| `run_lock_audit.sh` | Confirms `StorageBasedLockProvider` does not support a local `file://` path (source + exact exception) | *Choosing where the lock lives* | Spark 3.5 |
| `run_validators.sh` | Pre-commit SQL validators (pass/fail/`WARN_LOG`); plain launcher, hits the classloader bug | *Validating data before it commits* | Spark 3.5 |
| `run_validators_driver_cp.sh` | Same validators, fixed via `--driver-class-path` | *Validating data before it commits* | Spark 3.5, `--driver-class-path` launcher |
| `run_prewrite_validator.sh` | A custom `PreWriteValidator` defined live in spark-shell; documents why that always fails (`ReflectionUtils.getClass`/`Class.forName` cannot see a REPL-local class) | *Validating data before it commits* | Spark 3.5 |
| `run_callbacks.sh` | Post-commit HTTP callback to a local receiver, capturing the real JSON payload | *Notifying downstream systems after a commit* | Spark 3.5, `scripts/http_callback_receiver.py` running |
| `run_schema_on_read_control.sh` | Schema-on-read `ADD COLUMNS`/`ALTER COLUMN TYPE`/`RENAME COLUMN` on a scalar-only control table | *Enabling schema-on-read for everything else* | Spark 3.5, schema-evolution launcher |
| `run_schema_ai_types.sh` | Same operations on a `VECTOR`+`BLOB` table with schema-on-read on: `ADD COLUMNS` fails on the pre-existing `VECTOR` column | *Enabling schema-on-read for everything else* | Spark 3.5, schema-evolution launcher |
| `run_add_column_default_path.sh` / `run_add_column_default_path_inmemory.sh` | The *default* (schema-on-read off) `ADD COLUMNS` path succeeding on a `VECTOR`+`BLOB` table | *Adding columns without turning anything on* | Spark 3.5, schema-evolution launcher (first) or `catalogImplementation=in-memory` (second) |
| `run_table_version.sh` | Table version 6/8/9, auto-upgrade, `upgrade_table`/`downgrade_table` procedures, the CLI equivalents | *Upgrading and downgrading the table format* | Spark 3.5 |
| `run_flink_start_cluster.sh` / `run_flink_stop_cluster.sh` | Starts/stops a local Flink 1.20 standalone cluster | (infra for the two below) | Flink 1.20 — **not started by default; start/stop explicitly** |
| `run_flink_rli_example.sh` | Record-level index from a Flink job, `index.type=RECORD_LEVEL_INDEX`/`GLOBAL_RECORD_LEVEL_INDEX`, streaming vs. batch runtime mode | *Enforcing key uniqueness inside a partition or across the table* | Flink 1.20 cluster running |
| `run_polaris_start_server.sh` / `run_polaris_bootstrap.sh` | Starts a local Polaris server; creates the catalog, principal, role, namespace via REST | (infra for the Polaris examples) | Polaris 1.3.0-incubating server — **not started by default; start explicitly** |
| `run_polaris_hudi_table_working.sh` | The **confirmed working** recipe: `CREATE TABLE`/`INSERT`/`SELECT` through Polaris (client 1.4.0 + `catalogImplementation=in-memory`) | *Registering tables through Apache Polaris* | Polaris server running, Polaris Spark client 1.4.0 |
| `run_polaris_hudi_table_repro_bare_name.sh` | **Repro only.** The doc's bare unqualified `CREATE TABLE mydb.t` never reaches Polaris at all (client 1.3.0) | *Registering tables through Apache Polaris* | Polaris server running |
| `run_polaris_hudi_table_repro_none_get.sh` | **Repro only.** A fully-qualified `polaris_catalog.mydb.t` reaches Polaris but crashes with `NoSuchElementException: None.get` (client 1.3.0 bug) | *Registering tables through Apache Polaris* | Polaris server running |
| `run_polaris_hudi_table_repro_hivesync.sh` | **Repro only.** Client bumped to 1.4.0 fixes `CREATE TABLE`, but `INSERT` fails with `HoodieMetaSyncException` under the default `hive` catalog implementation | *Registering tables through Apache Polaris* | Polaris server running, client 1.4.0 |
| `run_polaris_hudi_table_repro_hivesync_override.sh` | **Repro only.** Confirms a per-table `hoodie.datasource.hive_sync.enable=false` does *not* avoid the above | *Registering tables through Apache Polaris* | Polaris server running, client 1.4.0 |

## Layout

```
chapter09/
  docker-compose.yml                    optional Spark container (bootstrap example only)
  scripts/
    spark_shell_with_hudi.sh            base Spark 3.5 launcher
    spark_shell_with_hudi_driver_cp.sh  + --driver-class-path (pre-commit validators)
    spark_shell_with_hudi_encryption.sh + Parquet test jar + crypto confs
    spark_shell_with_hudi_lance.sh      + Lance Spark bundle
    spark_shell_with_hudi_schema_evolution.sh  + Hive metastore compat-check override
    spark4_shell_with_hudi_variant.sh   Spark 4.1 launcher + parquet-variant jar
    spark_shell_with_hudi_polaris.sh    Spark 3.5 + Polaris Spark catalog client
    run_*.sh                            one per example, see the table above
    generate_source_data.sh / verify_bootstrap.sh / show_layout.sh / teardown.sh   bootstrap-specific
    http_callback_receiver.py           local HTTP receiver for run_callbacks.sh
  spark/
    *.scala                             one source file per example, matching script names
  flink/
    ...                                 Flink job sources for the RLI / Source V2 examples
```

## Running it

The Spark 3.5 examples that need no external service (Flink, Polaris) run standalone:

```bash
cd chapter09
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
./scripts/run_all.sh
```

`run_all.sh` runs a sensible subset in order: bootstrap, then the AI/vector/BLOB types, indexing,
encryption, concurrency, validators/callbacks, schema evolution, table version, and session
options. It does **not** start Flink or Polaris, and does not run the Spark 4.1 VARIANT examples
(different Spark major version) — run those separately:

```bash
./scripts/run_variant_events.sh
./scripts/run_variant_pushdown_mor.sh   # after run_variant_events.sh
./scripts/run_variant_inspect_parquet_schema.sh
./scripts/run_variant_lance.sh
```

Flink examples (start the cluster first, stop it when done):

```bash
./scripts/run_flink_start_cluster.sh
./scripts/run_flink_rli_example.sh
./scripts/run_flink_source_v2.sh
./scripts/run_flink_stop_cluster.sh
```

Polaris examples (start the server and bootstrap the catalog first):

```bash
./scripts/run_polaris_start_server.sh
./scripts/run_polaris_bootstrap.sh      # prints POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET to export
export POLARIS_CLIENT_ID=... POLARIS_CLIENT_SECRET=...
./scripts/run_polaris_hudi_table_working.sh
```

### Examples not in the chapter

These back sections that were cut from the chapter. They still run, and are kept for reference.

| Script | What it shows | Former section | Prereqs |
|---|---|---|---|
| `run_bootstrap.sh` (+ `generate_source_data.sh`, `verify_bootstrap.sh`, `show_layout.sh`) | Adopting existing Parquet as a Hudi table, `BootstrapRegexModeSelector`, metadata-only vs. full-record | *Running a bootstrap*, *Living with a bootstrapped table* | Spark 3.5 |
| `run_encryption.sh` | Parquet modular encryption on COW (full round-trip) and MOR (base file encrypted, log file plaintext) | *Encrypting columns at rest* | Spark 3.5, Parquet test jar |
| `run_encryption_control.sh` | Plain `df.write.parquet()` control test isolating the `InMemoryKMS` classpath gap from a Hudi-specific bug | *Encrypting columns at rest* | Spark 3.5, Parquet test jar |
| `run_encryption_column_stats.sh` | Confirms the metadata table's `column_stats` partition stores plaintext min/max for an encrypted column | *Encrypting columns at rest* | Spark 3.5, Parquet test jar |
| `run_session_options.sh` | `SET spark.hoodie.*`, the chapter's exact `trips_by_city` snippet (and the column-order trap it hits), precedence vs. table property, DataFrame-writer scope | *Setting Hudi options for a whole Spark session* | Spark 3.5 |
| `run_flink_source_v2.sh` | Flink's FLIP-27 Source V2 opt-in (`read.source-v2.enabled`), batch and streaming reads | *Reading with Flink's Source V2* | Flink 1.20 cluster running |

Paths default to `/tmp/hudiinaction/chapter09/<example>`. Most scripts respect a `BASE_PATH`
environment variable to relocate them.

## Bootstrap example in detail (not in the chapter)

The bootstrap example (`generate_source_data.sh` → `run_bootstrap.sh` → `verify_bootstrap.sh` →
`show_layout.sh`) is the chapter's *Running a bootstrap* / *Living with a bootstrapped table*
walkthrough: adopting a directory of existing Parquet as a Hudi table without rewriting it, using
`BootstrapRegexModeSelector` to apply `METADATA_ONLY` to older partitions and `FULL_RECORD` to
newer ones.

### What this shows

- A bootstrap that references existing files in place rather than copying them
- `BootstrapRegexModeSelector` applying `METADATA_ONLY` to partitions matching
  `trip_date=202[0-4]-.*` and `FULL_RECORD` to everything newer
- The skeleton files, the bootstrap index under `.hoodie/.aux/.bootstrap/`, and the source
  files the metadata-only partitions still depend on
- The Hudi metadata columns being available on rows whose data never moved

### What to look for

The difference between the two modes is in what each base file contains, which
`verify_bootstrap.sh` prints by reading the files directly, without going through Hudi:

```
METADATA_ONLY base file: _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key,
                         _hoodie_partition_path, _hoodie_file_name
FULL_RECORD base file:   _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key,
                         _hoodie_partition_path, _hoodie_file_name, trip_id, rider_id,
                         city, fare, updated_at, trip_date
```

The skeleton holds the five Hudi metadata columns and nothing else. Its data columns are
still in the source Parquet, and the query that returned all eight trips stitched the two
halves together. The full-record partitions carry everything and no longer refer to the
source.

Two further things the run shows. The metadata-only partitions commit at instant
`00000000000001` and the full-record ones at `00000000000002`, so a mixed-mode bootstrap
lands as two commits rather than one. And `hoodie.properties` records
`hoodie.bootstrap.base.path` along with `hoodie.bootstrap.index.type=HFILE`, which is the
dependency *Living with a bootstrapped table* warns about: delete or move the source
directory and the metadata-only partitions stop reading, while the full-record ones keep
working.

The run also demonstrates the derivation rule from *Deriving the merge mode from the ordering
field*. Nothing in the job sets a merge mode, yet the `hoodie.properties` it writes contains
`hoodie.record.merge.mode=EVENT_TIME_ORDERING`, because an ordering field is configured. Drop
`hoodie.table.ordering.fields` and Hudi would resolve `COMMIT_TIME_ORDERING` instead.

File sizes are not the thing to look at here. Both kinds of base file come out around 424 KB
in this run, because fixed per-file overhead dwarfs eight rows. What metadata-only saves is
the rewrite itself, and that shows at real volumes rather than on a toy dataset.

Two warnings during the query are expected and harmless:

```
WARN HFileBootstrapIndexReader: No value found for partition key (trip_date=2025-02-14)
WARN HFileBootstrapIndexReader: No value found for partition key (trip_date=2025-06-30)
```

Those are the full-record partitions, which have no bootstrap index entry because they do
not reference anything.

### Two constraints the example ran into

Both are real at Hudi 1.2.0, and both are set in `bootstrap_trips.scala`:

- **A bootstrap cannot build the column-stats index.**
  `SparkBootstrapCommitActionExecutor.updateColumnsToIndexForColumnStats` throws
  `HoodieNotSupportedException: col stats is not supported with bootstrap operation`, and
  `hoodie.metadata.index.column.stats.enable` defaults to true on Spark, so the job needs it
  set to false.
- **A full-record bootstrap needs an ordering field.**
  `SparkFullBootstrapDataProviderBase` reads the ordering field for every source row and
  passes it to `HoodieAvroUtils.getNestedFieldValAsString` without a null check, so a job
  with no ordering field fails with `NullPointerException: Cannot invoke "String.split"
  because "fieldName" is null`. The metadata-only half succeeds first, which makes this look
  like a partial failure. The script sets `hoodie.table.ordering.fields` to `updated_at`.

`hoodie.bootstrap.parallelism` is also lowered from its default of 1500. For the metadata-only
half that default is only a ceiling, so five partitions get five tasks either way, but the
full-record half passes it through as the bulk-insert shuffle parallelism, where eight rows
would otherwise be shuffled into 1500 partitions.

### Differences from the chapter's snippet

The chapter prints the same job with `s3://` paths, since that is where the problem lives in
practice. These scripts use local directories so the example runs on a laptop. They also
write from `spark.emptyDataFrame` rather than a populated DataFrame: a bootstrap reads its
data from `hoodie.bootstrap.base.path`, so the DataFrame passed to the writer carries no
rows.

Three options are set here that the chapter's snippet does not print, each explained in a
comment in `bootstrap_trips.scala`: `hoodie.metadata.index.column.stats.enable=false` and
`hoodie.table.ordering.fields=updated_at`, which the run requires for the reasons above, and
`hoodie.bootstrap.parallelism=4`, which just keeps a laptop run quick. Every other Hudi
option matches the chapter.

## Cleaning up

```bash
./scripts/teardown.sh          # removes /tmp/hudiinaction/chapter09
docker compose down -v         # if you used the container
```

## Verification

`/Users/saga/workspace/codope/hudi_packt/ch9_v2_run_log_snippets.md` (outside this repo) records,
for every numbered TODO in the chapter draft, the exact statement run, the exact unedited output,
and whether it matched the draft's printed text. `ch9_v2_run_log_spark35.md` and
`ch9_v2_run_log_spark4_flink_polaris.md` hold the earlier, broader verification passes this example
set grew out of.
