#!/usr/bin/env bash
# ============================================================================
# Runs the Yami DB fitness diagnostics and writes a clean report.
#
# Usage:
#   ./run_diagnostics.sh [SCHEMA_NAME]
#
# - Prompts for the DB password locally (never echoed, never stored).
# - If you don't pass a SCHEMA_NAME, it first lists schemas and exits so you
#   can re-run with the right one.
# - Tries your local mysql client first; if the MySQL 8 auth plugin rejects it,
#   re-run with USE_DOCKER=1 to use a Dockerized mysql:8 client instead:
#       USE_DOCKER=1 ./run_diagnostics.sh yami
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"

HOST="yami2016-2018.cv5z0utbbmv6.us-east-2.rds.amazonaws.com"
PORT=3306
USER_NAME="${DB_USER:-ann}"
CA="./global-bundle.pem"
SCHEMA="${1:-}"
OUT="diagnostics_report.txt"

if [[ ! -f "$CA" ]]; then
  echo "ERROR: $CA not found in $(pwd)" >&2; exit 1
fi

read -r -s -p "MySQL password for ${USER_NAME}@${HOST}: " DB_PASS; echo

run_mysql() {  # args: extra sql passed on stdin
  if [[ "${USE_DOCKER:-0}" == "1" ]]; then
    docker run --rm -i -v "$(pwd)/global-bundle.pem:/ca.pem:ro" mysql:8 \
      mysql -h "$HOST" -P "$PORT" -u "$USER_NAME" -p"$DB_PASS" \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=/ca.pem --connect-timeout=15 "$@"
  else
    MYSQL_PWD="$DB_PASS" mysql -h "$HOST" -P "$PORT" -u "$USER_NAME" \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca="$CA" --connect-timeout=15 "$@"
  fi
}

# Connectivity check and schema discovery.
if [[ -z "$SCHEMA" ]]; then
  echo "No schema given. Listing non-system schemas (pick one and re-run):"
  echo "SELECT schema_name FROM information_schema.schemata \
        WHERE schema_name NOT IN ('mysql','information_schema','performance_schema','sys');" \
    | run_mysql --table
  echo
  echo "Re-run:  ./run_diagnostics.sh <SCHEMA_NAME>"
  exit 0
fi

echo "Running diagnostics against schema '$SCHEMA' ..."
{
  echo "Yami DB diagnostics: schema=$SCHEMA ($(date))"
  echo "============================================================"
  run_mysql --table "$SCHEMA" < diagnostics.sql
} | tee "$OUT"

echo
echo "Report written to: $(pwd)/$OUT"
echo "Paste its contents back into the chat."
