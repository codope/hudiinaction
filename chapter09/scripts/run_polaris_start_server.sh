#!/usr/bin/env bash
set -euo pipefail

# Starts a local Apache Polaris 1.3.0-incubating server with in-memory persistence.
# Requires Java 21 (only Java 17/11/8 are on PATH on this machine; point POLARIS_JAVA_HOME
# at a JDK 21 install, e.g. the Corretto 21 tarball extracted under ~/polaris/jdk21/).
#
# Two flags below are NOT documented in Hudi's or Polaris's own getting-started docs, and were
# only found by reading Polaris 1.3.0's ProductionReadinessChecks.class bytecode after the
# server refused to start / refused catalog operations:
#   - polaris.readiness.ignore-severe-issues=true
#       Without it, the server flatly refuses to start at all once FILE is added to
#       SUPPORTED_CATALOG_STORAGE_TYPES ("Severe production readiness issues detected,
#       startup aborted!").
#   - polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true
#       Without it, the server starts, but every catalog request against a FILE-backed
#       catalog fails at runtime with "File IO implementation
#       'org.apache.iceberg.hadoop.HadoopFileIO' (storage type 'FILE') is considered
#       insecure and must not be used" - a *separate* check from the startup gate above,
#       not covered by ignore-severe-issues.
# Polaris 1.3.0 treats local-filesystem catalogs as strictly test-only and makes you opt in
# twice. There is no in-between: either both flags are set, or FILE-backed catalogs do not
# work at all on this release.

POLARIS_HOME="${POLARIS_HOME:-$HOME/polaris/polaris-bin-1.3.0-incubating}"
POLARIS_JAVA_HOME="${POLARIS_JAVA_HOME:-$HOME/polaris/jdk21/amazon-corretto-21.jdk/Contents/Home}"
POLARIS_PORT="${POLARIS_PORT:-8181}"
POLARIS_LOG="${POLARIS_LOG:-/tmp/hudiinaction/chapter09/polaris_server.log}"

if [ ! -x "$POLARIS_JAVA_HOME/bin/java" ]; then
  echo "No JDK 21 at $POLARIS_JAVA_HOME. Set POLARIS_JAVA_HOME to a JDK 21 install (Polaris 1.3.0 requires Java 21)." >&2
  exit 1
fi
if [ ! -x "$POLARIS_HOME/bin/server" ]; then
  echo "No Polaris server at $POLARIS_HOME/bin/server." >&2
  exit 1
fi

if curl -sf "http://localhost:${POLARIS_PORT}/api/catalog/v1/oauth/tokens" -d "grant_type=client_credentials&client_id=x&client_secret=y" >/dev/null 2>&1; then
  : # any response (even 400/401) from that path means something is listening; fall through
fi
if lsof -ti:"$POLARIS_PORT" >/dev/null 2>&1; then
  echo "Something is already listening on port $POLARIS_PORT; assuming Polaris is up."
  exit 0
fi

mkdir -p "$(dirname "$POLARIS_LOG")"
echo "Starting Polaris server ($POLARIS_HOME) on port $POLARIS_PORT with in-memory persistence ..."
JAVA_HOME="$POLARIS_JAVA_HOME" POLARIS_JAVA_OPTS='-Dpolaris.readiness.ignore-severe-issues=true -Dpolaris.persistence.type=in-memory -Dquarkus.http.port='"$POLARIS_PORT"' -Dpolaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["S3","AZURE","GCS","FILE"] -Dpolaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
  nohup "$POLARIS_HOME/bin/server" > "$POLARIS_LOG" 2>&1 &
echo "Polaris server pid $!"

for i in $(seq 1 20); do
  if grep -q "root principal credentials" "$POLARIS_LOG" 2>/dev/null; then
    break
  fi
  sleep 1
done

echo "--- root principal credentials (realm POLARIS, in-memory, regenerated every restart) ---"
grep "root principal credentials" "$POLARIS_LOG" || echo "(not printed yet, check $POLARIS_LOG)"
echo "REST endpoint: http://localhost:${POLARIS_PORT}/api/catalog  (management API under /api/management/v1)"
