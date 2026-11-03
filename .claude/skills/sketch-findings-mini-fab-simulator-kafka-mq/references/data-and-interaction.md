# Data & Interaction Mapping

## Design Decisions

- **The scene is event-driven, the sketch fakes the events.** `sources/.../index.html` runs a seeded discrete simulation (7 stations, FIFO/SPT/EDD, breakdowns) and prints `station.events`-style lines. In the real build the same state transitions come from SSE.
- **Event -> visual mapping (use this when wiring real data):**

| Real event / snapshot | Scene reaction | HUD reaction |
|---|---|---|
| `order_accepted` | spawn pizza group at the intake gate, ease to station 1 queue | WIP +1, feed line |
| `op_started` (station) | lot moves to the station slot, status light green, progress bar runs | detail card "Current lot", stepper active step |
| `op_completed` | lot eases to next station's queue slot, layer added to the pizza | stepper check + time |
| `station_down` / `station_up` | light blinks red / returns to normal, Bake glow off | status chip, toast "X is DOWN" |
| `order_completed` | pizza rides to the truck and shrinks away | throughput, cycle time |
| `wip.snapshots` | authoritative queue length per station (reconcile queue slots) | queue badges (red at 4+) |
| `kpi.snapshots` | - | KPI cards, Little's Law chip |

- **Station stages:** `dough, sauce, cheese, toppings, bake, slice, package` with unit times 1.2 / 1.0 / 1.0 / 1.6 / 2.6 / 1.0 / 1.4 s per pizza; a lot's time is `unit * qty` (lots of 1-3 pizzas so SPT and EDD can differ from FIFO). Bake is the bottleneck (about 97% utilization at a 6 s mean arrival interval).
- **KPIs shown (computed in the sketch the way the KPI service should):** throughput = pizzas completed in a 60 s window per minute; WIP = lots not finished; cycle time = mean born-to-finished of lots completed in the window; bottleneck = station with highest 30 s EMA utilization.
- **Little's Law check is a first-class HUD element:** `L` = time-weighted average WIP over the window, `λ` = lots completed per second, `W` = mean cycle time; show `L` vs `λ·W` and the percentage gap (green < 15%, amber otherwise; "warming up…" until 3 completions). This keeps the project's core value (KPIs match IE theory) visible in the UI.
- **Controls that must exist:** dispatch rule (FIFO/SPT/EDD -> RabbitMQ `control` command), arrival interval slider and `+ Rush lot` (-> `workorders`), Break/Repair a station (-> `control` fail/repair), simulation speed is a client-only toy in the sketch; in the real app it would only apply to replay.
- **Lists:** Stations tab shows utilization bar + queue count + status chip; Lots tab shows WIP lots with progress and current station, search filters by lot id, `Enter` selects the first match, `/` focuses search.
- **Camera helpers:** `Focus` (station) and `Follow` (lot) pan/zoom the camera to the selection.

## Build notes

- Sketch uses the UMD `three.min.js` (r150.1, deprecated) so it opens from `file://`. The production build should use a vendored ES module (current Three.js) plus an `importmap`, served by FastAPI static files; still no Node build.
- Keep 3D and HUD state in one plain JS store updated from SSE; render the 3D every frame and the HUD at ~5 Hz.
- For replay/comparison (Phase 7), the same store can be driven by a recorded event list instead of live SSE.

## What to Avoid

- Computing KPIs inside the browser for the real app: they must come from `kpi.snapshots` so numbers match the KPI service and the theory tests (the sketch computes them only to be self-contained).
- Making the browser the source of truth for station state or queues; use `wip.snapshots` to correct drift.
- Hiding the Little's Law gap when it is large; showing it is the point.

## Origin
Synthesized from sketches: 001
Source files available in: sources/001-factory-scene-style/
