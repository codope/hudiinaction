#!/usr/bin/env bash
# Shared helpers for the Chapter 7 scripts.
# Each SQL file runs in its own SQL Client session, and Flink's default catalog
# is in memory, so a file that uses tables declared elsewhere lists those files
# on a "-- requires:" line. run_sql prepends them before submitting.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CH_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE=(docker compose -f "$CH_DIR/docker-compose.yml")

# run_sql <file> [detached]
run_sql() {
  local file="$1" mode="${2:-}" name required combined
  name="$(basename "$file")"
  required="$(sed -n 's/^-- requires: *//p' "$CH_DIR/sql/$name")"
  mkdir -p "$CH_DIR/sql/.run"
  combined="$CH_DIR/sql/.run/$name"
  : > "$combined"
  for dep in $required; do cat "$CH_DIR/sql/$dep" >> "$combined"; echo >> "$combined"; done
  cat "$CH_DIR/sql/$name" >> "$combined"
  echo "=== $name ${mode:+($mode)}"
  # Run as the flink user: the SQL Client can create files under a new table's
  # .hoodie directory, and the Flink processes (user flink) must be able to write there.
  if [ "$mode" = "detached" ]; then
    "${COMPOSE[@]}" exec -T -u flink jobmanager /opt/flink/bin/sql-client.sh \
      -D execution.attached=false -f "/opt/sql/.run/$name"
  else
    "${COMPOSE[@]}" exec -T -u flink jobmanager /opt/flink/bin/sql-client.sh -f "/opt/sql/.run/$name"
  fi
}

# cancel_jobs <substring of the job name>   (no argument: cancel every running job)
cancel_jobs() {
  local pattern="${1:-}"
  curl -sf http://localhost:8081/jobs/overview | python3 -c '
import json, sys
pattern = sys.argv[1]
for j in json.load(sys.stdin)["jobs"]:
    if j["state"] == "RUNNING" and pattern in j["name"]:
        print(j["jid"], j["name"])' "$pattern" |
  while read -r jid jname; do
    echo "  cancelling $jname"
    curl -sf -X PATCH "http://localhost:8081/jobs/$jid?mode=cancel" >/dev/null
  done
}

# Flink commits to Hudi on each checkpoint (every 30 s in docker-compose.yml).
wait_for_commits() {
  local seconds="${1:-75}"
  echo "  waiting ${seconds}s for checkpoints to commit..."
  sleep "$seconds"
}
