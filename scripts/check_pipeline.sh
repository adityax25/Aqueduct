#!/usr/bin/env bash
# Health check for the Aqueduct pipeline.
# Reports whether the Flink job is running and whether its results are current,
# which together distinguish a live pipeline from a stalled one.

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-aqueduct-postgres}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR="${CONNECTOR:-aqueduct-postgres-connector}"

psql_q() {
    docker exec "$PG_CONTAINER" psql -U cdc -d yami -tAc "$1"
}

echo "=== Debezium connector ==="
curl -s "$CONNECT_URL/connectors/$CONNECTOR/status" |
    python3 -c 'import sys,json; d=json.load(sys.stdin); print("connector:", d["connector"]["state"]); [print("task %s:" % t["id"], t["state"]) for t in d["tasks"]]'

echo
echo "=== Flink job ==="
curl -s "$FLINK_URL/jobs/overview" |
    python3 -c '
import sys, json
jobs = json.load(sys.stdin)["jobs"]
if not jobs:
    print("NO JOB RUNNING. Results will be stale until the pipeline is resubmitted.")
for j in jobs:
    t = j["tasks"]
    print("state=%s tasks=%d/%d running, %d failed" % (j["state"], t["running"], t["total"], t["failed"]))
    print("uptime=%d minutes" % (j["duration"] // 60000))
'

echo
echo "=== Sink table row counts ==="
docker exec "$PG_CONTAINER" psql -U cdc -d yami -c "
SELECT 'enriched_line_items' AS sink_table, count(*) AS rows FROM enriched_line_items
UNION ALL SELECT 'revenue_by_category', count(*) FROM revenue_by_category
UNION ALL SELECT 'revenue_by_window',   count(*) FROM revenue_by_window
UNION ALL SELECT 'pricing_anomalies',   count(*) FROM pricing_anomalies;"

echo "=== Result freshness ==="
# The newest window boundary is the clearest liveness signal: it advances only
# while Flink is actively processing, so a stale value means a stalled pipeline.
NEWEST=$(psql_q "SELECT COALESCE(max(window_end)::text, 'none') FROM revenue_by_window;")
AGE=$(psql_q "SELECT COALESCE(round(extract(epoch FROM now() - max(window_end)))::text, 'n/a') FROM revenue_by_window;")
echo "newest window: $NEWEST (${AGE}s old)"

echo
echo "=== Revenue by category ==="
docker exec "$PG_CONTAINER" psql -U cdc -d yami -c "SELECT * FROM revenue_by_category ORDER BY revenue DESC;"

echo "=== Five most recent windows ==="
docker exec "$PG_CONTAINER" psql -U cdc -d yami -c "
SELECT window_start, window_end, line_items, round(revenue::numeric, 2) AS revenue
FROM revenue_by_window ORDER BY window_start DESC LIMIT 5;"
