# Rollback, savepoint, and restore with the Hudi CLI

These are the chapter's Hudi CLI steps. `spark/07_recovery.scala` runs the same steps through
Spark SQL procedures, which need no extra install.

## Starting the CLI

The CLI is published as `hudi-cli-bundle`. Its launcher script lives in the Hudi source tree:

```bash
git clone --depth 1 --branch release-1.2.1 https://github.com/apache/hudi.git /tmp/hudi-src
mkdir -p /tmp/hudi_bundles && cd /tmp/hudi_bundles
curl -fLO https://repo1.maven.org/maven2/org/apache/hudi/hudi-cli-bundle_2.12/1.2.1/hudi-cli-bundle_2.12-1.2.1.jar
curl -fLO https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.1/hudi-spark3.5-bundle_2.12-1.2.1.jar

export SPARK_HOME=/path/to/spark-3.5.x-bin-hadoop3
export JAVA_HOME=/path/to/jdk-11-or-17
export CLI_BUNDLE_JAR=/tmp/hudi_bundles/hudi-cli-bundle_2.12-1.2.1.jar
export SPARK_BUNDLE_JAR=/tmp/hudi_bundles/hudi-spark3.5-bundle_2.12-1.2.1.jar
/tmp/hudi-src/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
```

On first run the script downloads two `jakarta.el` jars into `packaging/hudi-cli-bundle/auxlib`
with `wget`, which macOS does not ship; install it (`brew install wget`) or download those jars
into that folder yourself.

The commands below use the table that `07_recovery.scala` creates; run that script first, or
point `--path` at your own table.

## Rolling back a commit

```
hudi-cli> connect --path /tmp/hudiinaction/chapter05/recovery/events_recovery
hudi-cli> commits show --desc true --limit 1
hudi-cli> commit rollback --commit <instant_time> --sparkMaster local[2]
```

## Creating a savepoint

```
hudi-cli> connect --path /tmp/hudiinaction/chapter05/recovery/events_recovery
hudi-cli> commits show
hudi-cli> savepoint create --commit <instant> --sparkMaster local[2]
```

## Restoring to a savepoint

Pause all writers to the table first.

```
hudi-cli> connect --path /tmp/hudiinaction/chapter05/recovery/events_recovery
hudi-cli> savepoints show
hudi-cli> savepoint rollback --savepoint <savepoint_instant> --sparkMaster local[2]
```

`savepoint create`, `savepoint rollback`, and `commit rollback` launch a Spark job, so they use
`SPARK_HOME` and take `--sparkMaster`.
