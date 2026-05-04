#!/bin/bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
BEARER_TOKEN="${BEARER_TOKEN:-eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NzY4NDE2OTIsImV4cCI6MTc3Njg0ODg5Miwic3ViIjoiNzRjOTg5OTgtMmMxYi00MTQ0LTk2N2YtNTU0ODk3Y2YyYWNlIiwiaXNfYWRtaW4iOnRydWV9.uR0_IyTEcZC8VIeik_cON-B1NZScMgi5PnleaLm0nQc}"

PG_CONTAINER="${PG_CONTAINER:-orchestrator-db}"
PG_USER="${POSTGRES_USER:-postgres}"
PG_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
PG_DB="${POSTGRES_DB:-chaos_platform}"

PHASE="FAULT"
METRIC_NAMES='["probe_success"]'

if [[ "$BEARER_TOKEN" == "<PASTE_BEARER_TOKEN_HERE>" ]]; then
  echo "ERROR: BEARER_TOKEN is not set. Edit the script or export BEARER_TOKEN=<token>." >&2
  exit 1
fi

QUERY="SELECT
  id,
  target_namespace,
  to_char(started_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"+0000\"'),
  to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"+0000\"')
FROM chaos_experiments
WHERE started_at IS NOT NULL
  AND completed_at IS NOT NULL
ORDER BY created_at ASC;"

rows=$(docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
  psql -U "$PG_USER" -d "$PG_DB" -At -F'|' -c "$QUERY")

if [[ -z "$rows" ]]; then
  echo "No chaos experiments with both started_at and completed_at found."
  exit 0
fi

total=$(echo "$rows" | wc -l | tr -d ' ')
echo "Found $total experiment(s) to process."

i=0
while IFS='|' read -r id ns start end; do
  i=$((i + 1))
  [[ -z "$id" ]] && continue

  payload=$(cat <<EOF
{
  "experiment_id": "$id",
  "phase": "$PHASE",
  "namespace": "$ns",
  "start": "$start",
  "end": "$end",
  "metric_names": $METRIC_NAMES
}
EOF
)

  response=$(curl -sS -o /tmp/fetch_metrics_body.$$ -w '%{http_code}' \
    -X POST "$API_BASE_URL/api/v1/metrics/fetch" \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload" || echo "000")

  body=$(cat /tmp/fetch_metrics_body.$$ 2>/dev/null || echo "")
  rm -f /tmp/fetch_metrics_body.$$

  echo "[$i/$total] $id $ns $start -> $end :: $response"
  if [[ "$response" != 2* ]]; then
    echo "  ! response body: $body"
  fi
done <<< "$rows"

echo "Done."
