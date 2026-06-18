#!/usr/bin/env bash
#
# Migrates the source tables from the upstream MySQL database into the local
# PostgreSQL CDC source using pgloader (run as a container, nothing to install).
# The MySQL password is read interactively and never written to disk or history.
set -euo pipefail
cd "$(dirname "$0")"

MYSQL_USER="read_full"
MYSQL_HOST="yami2016-2018.cv5z0utbbmv6.us-east-2.rds.amazonaws.com"
MYSQL_DB="yami_data"

# pgloader reaches PostgreSQL through the port published to the host, which keeps
# this independent of the local compose project/network name.
PG_TARGET="postgresql://cdc:cdc_pw@host.docker.internal:5432/yami"

read -r -s -p "MySQL password for ${MYSQL_USER}@${MYSQL_HOST}: " MYSQL_PASSWORD; echo

LOAD_FILE="$(mktemp)"
trap 'rm -f "$LOAD_FILE"' EXIT

# The load command migrates only the transactional core tables (the im_* dimension
# tables are skipped) and lands them in the default 'public' schema rather than one
# named after the source database.
cat > "$LOAD_FILE" <<EOF
LOAD DATABASE
     FROM mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:3306/${MYSQL_DB}
     INTO ${PG_TARGET}

 WITH include drop, create tables, create indexes, reset sequences,
      workers = 4, concurrency = 1

 SET maintenance_work_mem to '256MB', work_mem to '128MB'

 INCLUDING ONLY TABLE NAMES MATCHING 'order_info', 'order_goods', 'goods_info'

 ALTER SCHEMA '${MYSQL_DB}' RENAME TO 'public';
EOF

docker run --rm \
  -v "${LOAD_FILE}:/tmp/yami.load:ro" \
  dimitri/pgloader:latest \
  pgloader --verbose /tmp/yami.load
