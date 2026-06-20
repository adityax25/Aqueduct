#!/usr/bin/env bash
#
# Registers (or updates) the Debezium Postgres connector with Kafka Connect.
# Idempotent: a PUT to the connector config endpoint creates it if missing and
# updates it otherwise.
set -euo pipefail
cd "$(dirname "$0")"

CONNECTOR="aqueduct-postgres-connector"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "Registering ${CONNECTOR} ..."
curl -sS -X PUT "${CONNECT_URL}/connectors/${CONNECTOR}/config" \
  -H "Content-Type: application/json" \
  -d @connector-postgres.json
echo
echo
echo "Status:"
curl -sS "${CONNECT_URL}/connectors/${CONNECTOR}/status"
echo
