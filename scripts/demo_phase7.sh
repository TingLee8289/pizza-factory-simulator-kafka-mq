#!/usr/bin/env bash
# Phase 7 manual demo: dispatch rule switching, FIFO/EDD/SPT comparison and replay.
set -euo pipefail

cat <<GUIDE
Phase 7 demo: dispatch rules (FIFO/EDD/SPT) and comparison
Requires the line running: docker compose --profile line up -d --wait
Stop it with:              docker compose --profile line down
Dashboard: http://127.0.0.1:8000

1) Open http://127.0.0.1:8000 and click "Compare".
   A full 3-round comparison at the shipped defaults (rate 2.0/min, k 1.5,
   N 30 orders) takes about 50-55 min of wall-clock time (real-time
   simulation, no simulated clock). For a quick demo, run the whole stack
   with scaled recipes first so each round finishes in well under a minute:

     RECIPES_DIR=\$(mktemp -d)
     uv run python -c "
import sys
from pizza.common.recipes import load_all_recipes
dest = sys.argv[1]
for station, table in load_all_recipes().items():
    lines = [f'station = \"{station}\"', '', '[unit_time_s]']
    lines += [f'{p} = {v * 0.05!r}' for p, v in table.items()]
    open(f'{dest}/{station}.toml', 'w', encoding='utf-8').write('\n'.join(lines) + '\n')
" "\$RECIPES_DIR"
     RECIPES_DIR="\$RECIPES_DIR" docker compose --profile line up -d --wait

   Then in the dashboard, click "Compare" with a small n_orders (e.g. 5-8)
   and a higher arrival_rate_per_min so the round is short.

2) Switch the dispatch rule mid-run (outside a comparison, while a run is
   active): use the rule selector on the dashboard, or from the CLI:

     curl -s -X POST http://127.0.0.1:8000/api/rule -H "content-type: application/json" -d '{"rule": "spt"}'

   Watch the backlog ordering at each station change on the next dispatch;
   no restart is needed (SCHED-03). Blocked (409) while a comparison is
   running (Reset/Rule are locked for the whole comparison).

3) After a comparison finishes, verify a round's stored KPI snapshot by
   replaying Kafka from scratch on a brand-new consumer group (never writes
   anything back, D-13):

     uv run python -m pizza.kpi.recompute --run-id <id>
     uv run python -m pizza.kpi.recompute --run-id <id> --json

4) Export the comparison table as CSV:

     curl -s http://127.0.0.1:8000/api/comparison.csv -o comparison.csv

Rule-vs-fact pairs for this phase are documented in docs/messaging-roles.md
(## Phase 7 dispatch rules and comparison).
GUIDE
