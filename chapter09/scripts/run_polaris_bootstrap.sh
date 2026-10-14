#!/usr/bin/env bash
set -euo pipefail

# Creates a Polaris catalog backed by local disk, a principal + principal role + catalog role
# with CATALOG_MANAGE_CONTENT, and the "mydb" namespace the CREATE TABLE examples use - all via
# Polaris's REST API, since there is no SQL DDL path to do this from Spark. Requires
# ./scripts/run_polaris_start_server.sh to already be running, and its printed root credentials.
#
# Prints POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET for the new "hudi_writer" principal at the
# end; export those before running scripts/run_polaris_hudi_table*.sh.

: "${POLARIS_ROOT_CLIENT_ID:?Set POLARIS_ROOT_CLIENT_ID from the run_polaris_start_server.sh output}"
: "${POLARIS_ROOT_CLIENT_SECRET:?Set POLARIS_ROOT_CLIENT_SECRET from the run_polaris_start_server.sh output}"

POLARIS_URI_BASE="${POLARIS_URI_BASE:-http://localhost:8181}"
CATALOG_NAME="${CATALOG_NAME:-hudi_catalog}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-/tmp/hudiinaction/chapter09/polaris_warehouse}"
mkdir -p "$WAREHOUSE_DIR"

TOKEN=$(curl -sf -X POST "$POLARIS_URI_BASE/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_CLIENT_ID}&client_secret=${POLARIS_ROOT_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

echo "=== create catalog $CATALOG_NAME (storageType FILE, local disk) ==="
curl -s -X POST "$POLARIS_URI_BASE/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"catalog\": {\"name\": \"$CATALOG_NAME\", \"type\": \"INTERNAL\", \"properties\": {\"default-base-location\": \"file://${WAREHOUSE_DIR}\"}, \"storageConfigInfo\": {\"storageType\": \"FILE\", \"allowedLocations\": [\"file://${WAREHOUSE_DIR}\"]}}}"
echo

echo "=== create principal hudi_writer ==="
PRINCIPAL_JSON=$(curl -s -X POST "$POLARIS_URI_BASE/api/management/v1/principals" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"principal": {"name": "hudi_writer"}}')
echo "$PRINCIPAL_JSON"
HUDI_CLIENT_ID=$(echo "$PRINCIPAL_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['credentials']['clientId'])")
HUDI_CLIENT_SECRET=$(echo "$PRINCIPAL_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['credentials']['clientSecret'])")

echo "=== create + wire principal role / catalog role / grant ==="
curl -s -X POST "$POLARIS_URI_BASE/api/management/v1/principal-roles" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "hudi_writer_role"}}' >/dev/null
curl -s -X PUT "$POLARIS_URI_BASE/api/management/v1/principals/hudi_writer/principal-roles" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "hudi_writer_role"}}' >/dev/null
curl -s -X POST "$POLARIS_URI_BASE/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "hudi_catalog_admin"}}' >/dev/null
curl -s -X PUT "$POLARIS_URI_BASE/api/management/v1/principal-roles/hudi_writer_role/catalog-roles/${CATALOG_NAME}" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "hudi_catalog_admin"}}' >/dev/null
curl -s -X PUT "$POLARIS_URI_BASE/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/hudi_catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' >/dev/null

echo "=== create namespace mydb directly against Polaris ==="
# Discovered by running the doc's own example: `CREATE DATABASE IF NOT EXISTS mydb` from Spark
# only creates the namespace in the local Hive/session catalog, never in Polaris, and Hudi's
# Polaris delegation only overrides CREATE TABLE, not CREATE DATABASE. Without this call the
# CREATE TABLE examples cannot register in Polaris (namespace must already exist).
curl -s -X POST "$POLARIS_URI_BASE/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"namespace": ["mydb"]}'
echo

echo "=== export these before running scripts/run_polaris_hudi_table*.sh ==="
echo "export POLARIS_CLIENT_ID=$HUDI_CLIENT_ID"
echo "export POLARIS_CLIENT_SECRET=$HUDI_CLIENT_SECRET"
