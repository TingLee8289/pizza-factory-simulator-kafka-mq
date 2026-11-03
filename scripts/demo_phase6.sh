#!/usr/bin/env bash
# Phase 6 manual demo: breakdowns, rework + DLQ, stocker/BANK and chaos kill.
set -euo pipefail

RUN_ID="demo6-$(date +%s)"
SEND="uv run python -m pizza.tools.send_job"

cat <<GUIDE
Phase 6 demo (run_id=$RUN_ID)
Requires the line running: docker compose --profile line up -d --wait
Stop it with:              docker compose --profile line down
Dashboard: http://127.0.0.1:8000
GUIDE

EXTRA=()
if [ "${RANDOM_BREAKDOWNS:-0}" = "1" ]; then
  EXTRA+=(--random-breakdowns)
fi

$SEND start-run --run-id "$RUN_ID" \
  --seed "${SEED:-42}" --arrival-rate "${ARRIVAL_RATE:-0.05}" --max-orders "${MAX_ORDERS:-12}" \
  ${EXTRA[@]+"${EXTRA[@]}"}

cat <<REMINDER
Try these against run $RUN_ID:
  Manual breakdown (bake held down for 20 s, job pauses and resumes):
    $SEND break --station bake --run-id $RUN_ID --duration-s 20
    $SEND repair --station bake --run-id $RUN_ID
  Poison order (fails, retries via TTL, then dispatch.dlq and lifecycle failed):
    $SEND order --run-id $RUN_ID --fail-step spin --poison
    Watch queues station.spin.retry and dispatch.dlq at http://localhost:15672
  Stocker / BANK (use an order id from the dashboard):
    $SEND bank --run-id $RUN_ID --order-id <order-id> --reason customer
    $SEND release --run-id $RUN_ID --order-id <order-id>
  Chaos kill and redelivery:
    bash scripts/chaos_kill.sh bake
  Random breakdowns on start: RANDOM_BREAKDOWNS=1 bash scripts/demo_phase6.sh
REMINDER
