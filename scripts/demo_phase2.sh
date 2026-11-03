#!/usr/bin/env bash
# Phase 2 manual demo: one station, one job, a duplicate, and recorded-once.
set -euo pipefail

STATION="${STATION:-bake}"
RUN_ID="${RUN_ID:-demo-$(date +%s)}"

cat <<GUIDE
Phase 2 demo (station=$STATION run_id=$RUN_ID)
  1. Announce the run to the station (run.started -> idle).
  2. Send one job (unit_time_s=5 default, qty=1).
  3. Send the same job again with the same --event-id (duplicate).
  4. Replay station.events: expect RECORDED once and DUPLICATE ignored once.
Requires: docker compose --profile line up -d --wait (station-bake service runs, started with --profile line).
GUIDE

echo "== 1. start-run"
uv run python -m pizza.tools.send_job start-run --station "$STATION" --run-id "$RUN_ID"

echo "== 2. first job"
OUT="$(uv run python -m pizza.tools.send_job job --station "$STATION" --run-id "$RUN_ID")"
echo "$OUT"
EVENT_ID="$(echo "$OUT" | sed -n 's/.*event_id=\([0-9a-f-]*\).*/\1/p')"
ORDER_ID="$(echo "$OUT" | sed -n 's/.*order_id=\([^ ]*\).*/\1/p')"

echo "== 3. duplicate job (same event id $EVENT_ID)"
uv run python -m pizza.tools.send_job job --station "$STATION" --run-id "$RUN_ID" \
  --order-id "$ORDER_ID" --event-id "$EVENT_ID"

echo "== 4. recorder (stops after 20 s idle)"
uv run python -m pizza.recorder --run-id "$RUN_ID" --idle-timeout-s 20

cat <<REMINDER
Watch topic station.events in Kafka UI: http://127.0.0.1:8080
Redelivery demo: send a job, then 'docker compose kill station-bake' mid-job,
then 'docker compose --profile line up -d station-bake' and watch op.started redelivered=true.
REMINDER
