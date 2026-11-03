#!/usr/bin/env bash
# Phase 4 manual demo: live KPIs vs IE theory on the compose line.
#
# What to look for in the printed snapshot:
#   - bottleneck becomes "bake" once bake utilization >= 80% (after a few confirmations)
#   - capacity_ratio (measured throughput / theoretical capacity) approaches 1.0
#     (about 100 percent) as the run proceeds; early values are biased low by pipeline fill
#   - theory block: capacity_per_s, raw_process_time_s, critical_wip, weights_source
#   - little.status is "pass" only once the run is drained (wip_now == 0); under the
#     sustained overload used here it is not expected to pass while orders are in flight
set -euo pipefail

RUN_ID="demo4-$(date +%s)"
SNAPSHOT_WAIT_S="${SNAPSHOT_WAIT_S:-180}"

echo "Bringing up infra, rebuilding the image and declaring topology (incl. kpi.control)"
docker compose up -d --wait
docker compose build
# Idempotent; migrates an existing broker volume. If init fails with a queue argument
# mismatch, reset with: docker compose down -v   (assumption A3)
docker compose run --rm init || {
  echo "init failed. If this is a queue argument mismatch run: docker compose down -v" >&2
  exit 1
}

docker compose --profile line up -d --wait

# Overload: default recipes, bake capacity about 0.0385 orders/s, so 0.05/s overloads bake.
uv run python -m pizza.tools.send_job start-run --run-id "$RUN_ID" \
  --seed "${SEED:-42}" --arrival-rate 0.05 --max-orders "${MAX_ORDERS:-40}"

echo "Run $RUN_ID started; collecting the latest kpi.snapshots message for ${SNAPSHOT_WAIT_S}s"
sleep "${SNAPSHOT_WAIT_S}"

RUN_ID="$RUN_ID" uv run python - <<'PY'
import json
import os
import time
import uuid

from confluent_kafka import Consumer

from pizza.common.config import Settings

run_id = os.environ["RUN_ID"]
consumer = Consumer(
    {
        "bootstrap.servers": Settings().kafka_bootstrap,
        "group.id": f"demo4-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)
consumer.subscribe(["kpi.snapshots"])
last = None
idle_until = time.monotonic() + 10
while time.monotonic() < idle_until:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    try:
        body = json.loads(msg.value())
    except ValueError:
        continue
    if body.get("run_id") == run_id:
        last = body
        idle_until = time.monotonic() + 3
consumer.close()

if last is None:
    print("no kpi snapshot found for", run_id)
else:
    keys = (
        "run_id", "warming_up", "orders_total", "orders_completed", "wip_now",
        "throughput_per_s", "capacity_ratio", "bottleneck", "bottleneck_candidate",
        "cycle_time", "theory", "little",
    )
    print(json.dumps({k: last.get(k) for k in keys}, indent=2))
    print("station utilization:",
          {s: v.get("utilization") for s, v in last.get("stations", {}).items()})
PY

cat <<REMINDER
Stop the line with: docker compose --profile line down
Dashboards: Kafka UI http://127.0.0.1:8080 (topic kpi.snapshots), RabbitMQ http://127.0.0.1:15672
REMINDER
