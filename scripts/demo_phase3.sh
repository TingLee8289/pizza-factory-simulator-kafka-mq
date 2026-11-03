#!/usr/bin/env bash
# Phase 3 manual demo: full seven-station line driven by the generator and scheduler.
set -euo pipefail

RUN_ID="demo3-$(date +%s)"

cat <<GUIDE
Phase 3 demo (run_id=$RUN_ID)
Requires the line running: docker compose --profile line up -d --wait
Stop it with:              docker compose --profile line down
GUIDE

uv run python -m pizza.tools.send_job start-run --run-id "$RUN_ID" \
  --seed "${SEED:-42}" --arrival-rate "${ARRIVAL_RATE:-0.05}" --max-orders "${MAX_ORDERS:-12}"

cat <<REMINDER
Default arrival rate 0.05/s = one order per 20 s, so the bake queue visibly builds.
Overload preset: ARRIVAL_RATE=0.1 bash scripts/demo_phase3.sh
Open Kafka UI http://127.0.0.1:8080 and watch:
  order.lifecycle  (created, released, in_process, completed)
  station.events   (op.dispatched, op.started, op.completed)
Open RabbitMQ http://127.0.0.1:15672 and watch queue station.bake.jobs.
Re-run with the same SEED (SEED=42) to see the same order ids and products (ORDER-04).
REMINDER
