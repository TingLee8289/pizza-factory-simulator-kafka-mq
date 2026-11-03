#!/usr/bin/env bash
# Phase 6 chaos: SIGKILL a station container mid-job, restart it, verify redelivery.
# Usage: bash scripts/chaos_kill.sh [station]   (default: bake)
set -euo pipefail

STATION="${1:-bake}"
case "$STATION" in
  spin|sauce|cheese|toppings|bake|slice|package) ;;
  *)
    echo "unknown station: allowed are spin sauce cheese toppings bake slice package" >&2
    exit 2
    ;;
esac

RUN_ID="chaos-$(date +%s)"
DOWN_S="${CHAOS_DOWN_S:-5}"

cat <<GUIDE
Phase 6 chaos kill (station=$STATION, run_id=$RUN_ID)
Requires the line running: docker compose --profile line up -d --wait
Dashboard: http://127.0.0.1:8000   RabbitMQ: http://127.0.0.1:15672
GUIDE

if [ "${CHAOS_WAIT_FOR_ENTER:-0}" = "1" ]; then
  echo "Submit work (e.g. an order) and wait until $STATION shows busy, then press ENTER."
  read -r _
else
  echo "Publishing a long job directly to $STATION so it is busy when killed ..."
  uv run python -m pizza.tools.send_job start-run --station "$STATION" --run-id "$RUN_ID"
  uv run python -m pizza.tools.send_job job --station "$STATION" --run-id "$RUN_ID" \
    --unit-time-s "${CHAOS_JOB_S:-20}"
  sleep 3
fi

echo "Killing station-$STATION ..."
docker compose --profile line kill "station-$STATION"
sleep "$DOWN_S"
echo "Starting station-$STATION again ..."
docker compose --profile line start "station-$STATION"

cat <<VERIFY
Verify redelivery:
  RabbitMQ UI (queue station.$STATION.jobs): the message shows redelivered, then drains.
  Kafka UI http://127.0.0.1:8080 topic station.events: a second op.started with
    redelivered=true (attempt >= 2) and exactly one op.completed for the order.
VERIFY
