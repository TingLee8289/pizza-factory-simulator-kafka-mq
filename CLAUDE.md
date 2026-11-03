<!-- GSD:project-start source:PROJECT.md -->
## Project

**Pizza Factory Simulator (pizza-factory-simulator-kafka-mq)**

一個以童謠 "Pizza Party" 為靈感的披薩工廠即時模擬器,是個人學習專案,用來實際練習 Industrial Engineering (IE)、Production Control (PC) 排程派工、Kafka 事件流、MQ (RabbitMQ) 指令佇列四塊概念。製程依序為七站:spin the dough → spread the sauce → shred the cheese → spread the toppings → bake the pizza → slice the pizza → package。使用者是開發者本人,透過 Web 儀表板觀察產線並驗證 KPI。

**Core Value:** 用真實的 Kafka + RabbitMQ 訊息流驅動一條可觀察的披薩產線,並且儀表板上的 KPI(throughput、WIP、cycle time、瓶頸)能和 IE 理論值對得上。

### Constraints

- **Tech stack**: Python + Apache Kafka + RabbitMQ — 使用者明確指定,且為學習目標本身
- **Runtime**: 本機 Docker Compose — 免雲端成本,方便重現與練習
- **Simulation mode**: 即時(wall-clock)模擬 — 站點為獨立服務,用真實時間模擬加工,貼近分散式系統行為
- **Scope**: v1 僅單一線性產線、固定 cycle time — 控制複雜度,後續擴充
- **UI**: Web 儀表板 — 使用者要在瀏覽器中觀察與驗證 KPI
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Recommended Stack
### Core Runtime
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Python | 3.13 (`python:3.13-slim-bookworm`) | All services | All recommended libs support it (aio-pika needs >=3.11, fastapi >=3.10). 3.13 has prebuilt wheels for confluent-kafka and orjson. Avoid 3.14 free-threading experiments. |
| uv | 0.12.x | Dependency mgmt, lockfile, Docker installs | Fast, single lockfile (uv.lock) for a multi-service monorepo; use one workspace/shared package. |
| asyncio | stdlib | Concurrency model for every service | Stations spend their time in `await asyncio.sleep(cycle_time)`; async makes "one station = one event loop" trivial and lets the failure simulator run concurrently. |
### Messaging
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Apache Kafka (KRaft, no ZooKeeper) | `apache/kafka:4.2.2` (single node, official image) | Station event stream + WIP snapshots | Official ASF image, KRaft-only in 4.x, no Bitnami dependency (Bitnami free images were withdrawn in 2025; do not use). 4.3.x was still rc on Docker Hub at research time; bump when GA. Single combined broker/controller node, replication factor 1. |
| RabbitMQ | `rabbitmq:4.2-management-alpine` (4.3 also available) | Work orders + dispatch commands (ack/retry/DLQ) | Management UI on :15672 is valuable for a learning project (see queues, unacked, DLQ). Use quorum queues (4.x default-friendly, native `x-delivery-limit` gives retry cap + DLX for free). |
| confluent-kafka | 2.16.0 | Kafka producer/consumer | librdkafka-based, the reference client, Python >=3.8. Chosen per PROJECT.md. Use via thin async wrapper (poll in executor / dedicated thread) rather than aiokafka — see Alternatives. |
| aio-pika | 10.1.0 | RabbitMQ client | Asyncio-native, robust reconnect (`connect_robust`), publisher confirms, easy `message.process()` ack/nack/reject. Matches asyncio service model. |
### Web / Dashboard
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| FastAPI | 0.142.x | Dashboard backend: REST (place order, switch rule, toggle breakdown) + live push | Async-native, shares event loop with aio-pika and Kafka consumer tasks; Pydantic models double as API schema. |
| uvicorn | 0.54.x (`uvicorn[standard]`) | ASGI server | Standard. Run with 1 worker (in-memory KPI state is single-process). |
| SSE (via `sse-starlette` or plain `StreamingResponse`) | latest | Server -> browser KPI push | Dashboard is one-way push; SSE auto-reconnects, no protocol code, plays nicely with proxies. Use REST POST for commands. Use WebSocket only if you later need bidirectional. |
| Vanilla HTML + htmx-free JS, served by FastAPI static files | n/a | Frontend | Single-user localhost tool: no Node build step in Docker Compose. One `index.html` + ES modules. Keeps focus on Kafka/MQ. |
| uPlot (or Chart.js 4.5.x) | Chart.js 4.5.1 (npm verified); uPlot not verified | Time-series charts (throughput, WIP, cycle time) | Chart.js via CDN/vendored file is the lowest-friction choice and fine at ~1 Hz updates. Pick uPlot only if you need >10k points. ECharts 6.x is overkill. Vendor the JS file (no CDN dependency at runtime). |
### Stream Processing (KPI computation)
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Plain consumer loop in a dedicated `kpi-service` | n/a (confluent-kafka) | Fold station events into KPIs (throughput, WIP, cycle time, utilization, bottleneck) | KPIs here are tumbling/sliding-window aggregates over a few events/sec. A hand-written consumer with in-memory state + periodic snapshot publish is ~200 lines, teaches consumer groups/offsets (a stated learning goal), and is unit-testable as pure functions. Keep the KPI math in a pure module (`fold(state, event) -> state`) so tests replay event fixtures against Little's Law. |
### Schema / Serialization
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Pydantic v2 | 2.13.x | Message models (events, orders, commands), validation | One model definition shared by producers, consumers, API. Use discriminated union on `event_type`. Version field (`schema_version`) in every envelope. |
| JSON via orjson | 3.13.x | Wire format on both brokers | Human-readable in Kafka UI/RabbitMQ management, zero schema-registry infra. Envelope: `event_id` (UUID), `event_type`, `ts` (UTC ISO8601 / epoch ms), `station`, `order_id`, `schema_version`. |
| (Defer) Avro/Protobuf + Schema Registry | fastavro 1.13.1 | Optional later milestone | Real value, but adds a container and ceremony; not needed for v1 goals. |
### Config / Observability
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pydantic-settings | 2.15.x | Env-var config per service (broker URLs, cycle times, MTBF/MTTR, dispatch rule) | Always; Compose `environment:` maps directly. Keep station cycle times and failure params in one `line.yaml`/env so tests can shrink them. |
| structlog | 26.1.x | JSON logs with `order_id`/`station` bound context | Always; makes tracing an order across 7 services via `docker compose logs` possible. |
| Kafka UI (`provectuslabs/kafka-ui` or `kafbat/kafka-ui`) | latest tag, pin | Browse topics/consumer lag | Dev-only Compose profile; strongly aids learning. |
| prometheus-client 0.26 | optional | Service metrics | Skip for v1; dashboard KPIs come from Kafka. |
### Testing / Tooling
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pytest | 9.1.x | Test runner | Always |
| pytest-asyncio (or anyio plugin) | latest | Async tests | Always (version not verified) |
| testcontainers[kafka,rabbitmq] | 4.15.x | Integration tests against real brokers | A few end-to-end tests: retry -> DLQ path, event replay. Alternative: reuse the Compose stack via a `pytest` marker. |
| ruff | 0.16.x | Lint + format | Always |
| mypy/pyright | latest | Typing | Optional |
## Topology Choices (stack-level decisions)
- **Kafka topics:** `station.events` (key = `order_id` so one order's events stay ordered; 3-6 partitions fine), `wip.snapshots` (key = station; consider `cleanup.policy=compact`), `kpi.snapshots` optional. Each consumer uses its own `group.id`; the dashboard/KPI service demonstrates replay with `auto.offset.reset=earliest`.
- **RabbitMQ:** direct/topic exchange `line`, one durable quorum queue per station (`station.<name>.in`), a retry strategy of `x-delivery-limit` + dead-letter exchange `line.dlx` -> `station.<name>.dlq`. Manual ack after processing completes; `prefetch_count=1` per station to model single-server stations. Publisher confirms on.
- **Stations:** one image, `STATION_NAME` env var selects behavior; seven Compose services from the same Dockerfile. Next station gets work via RabbitMQ (dispatch command), emits Kafka events as a side effect. Emit Kafka after ack-safe boundary and make consumers idempotent on `event_id` (RabbitMQ redelivery produces duplicates).
- **Timing:** use `time.monotonic()` for durations and wall-clock UTC timestamps in events; compute cycle time from event timestamps, not from consumer receive time.
## Alternatives Considered
| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Kafka client | confluent-kafka | kafka-python | Largely unmaintained/slow-moving; pure Python and weaker semantics. Do not use. |
| Kafka client | confluent-kafka | aiokafka 0.14.0 | Genuinely asyncio-native and viable (Python >=3.10). Choose it if the executor wrapper feels clumsy; but confluent-kafka is what PROJECT.md specifies, has better docs and transfers to real jobs. MEDIUM-confidence tradeoff. |
| RabbitMQ client | aio-pika | pika 1.4.4 | `BlockingConnection` blocks the loop and heartbeats suffer during long processing/sleep; `SelectConnection` callbacks are awkward. pika is fine only for sync, simple scripts. Note: pika last release is old (1.4.4) |
| Stream processing | Plain consumer | Faust / faust-streaming | Faust is abandoned; faust-streaming is community-maintained with spotty releases. Overkill. |
| Stream processing | Plain consumer | Quix Streams | Reasonable (Kafka-native Python), but adds abstraction that hides the offsets/windowing you want to learn. Revisit in a later milestone. |
| Stream processing | Plain consumer | Kafka Streams / ksqlDB / Flink | JVM, extra containers; out of language scope. |
| Kafka image | apache/kafka | bitnami/kafka | Bitnami free image catalog was discontinued/moved to legacy in 2025; unreliable pulls. |
| Kafka image | apache/kafka | confluentinc/cp-kafka | Heavier, fine but unnecessary. |
| Push transport | SSE | WebSocket | Needs more code for no gain on one-way data. Use WS only if bidirectional streaming appears. |
| Frontend | Static HTML + Chart.js | React/Vite, Grafana | Build step + extra container; Grafana can't easily host manual order entry and rule toggles. Reconsider if the UI grows substantially (a later milestone). |
| Serialization | JSON + Pydantic | Avro + Schema Registry | Extra infra; defer. |
| Task queue | RabbitMQ directly | Celery | Hides the ack/retry/DLQ mechanics that are the learning goal. |
| Simulation | wall-clock asyncio | SimPy | Explicitly out of scope (no simulated clock). Might be useful as an offline "theory oracle" to cross-check KPIs in tests, though. |
| Config | pydantic-settings | python-dotenv/os.environ | No validation/typing. |
## Docker Compose Notes
- `apache/kafka` single node needs correct listeners: an internal listener (`kafka:9092`) for containers and an external one (`localhost:9094`) for host tools; set `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, and RF=1 for `__consumer_offsets`/transaction state. Misconfigured advertised listeners is the #1 time sink.
- Add `healthcheck` on Kafka (`kafka-broker-api-versions.sh`) and RabbitMQ (`rabbitmq-diagnostics -q ping`); use `depends_on: condition: service_healthy`.
- Create topics and RabbitMQ topology (exchanges/queues/DLX) idempotently from an `init` one-shot service or from each service on startup (declare is idempotent if arguments match; quorum-queue argument changes require deleting the queue).
- Use named volumes for broker data so `docker compose down -v` is the explicit reset.
- Windows host: run via Docker Desktop/WSL2; keep line endings LF for shell scripts (`.gitattributes`).
## Installation
# Core
# Dev
## Confidence
| Item | Confidence | Basis |
|------|------------|-------|
| Library versions, image tags | HIGH | Queried PyPI/npm/Docker Hub 2026-10-08 |
| Python 3.13, aio-pika, confluent-kafka, FastAPI+SSE, plain consumer for KPIs | MEDIUM-HIGH | Version-verified; fit rationale is engineering judgment |
| apache/kafka 4.2.2 listener env vars | MEDIUM | Training knowledge; verify against the official image docs when writing Compose |
| RabbitMQ quorum queue `x-delivery-limit` + DLX semantics | MEDIUM | Training knowledge; verify in RabbitMQ 4.x docs in the messaging phase |
| Bitnami image status | MEDIUM | Training knowledge; not re-verified |
| uPlot version, pytest-asyncio version | LOW | Not verified |
## Sources
- PyPI JSON API (confluent-kafka, aiokafka, aio-pika, pika, fastapi, uvicorn, pydantic, pydantic-settings, pytest, testcontainers, structlog, orjson, fastavro, prometheus-client, uv, ruff), queried 2026-10-08
- npm registry (chart.js 4.5.1, echarts 6.1.0), queried 2026-10-08
- Docker Hub API (apache/kafka, library/rabbitmq, library/python tags), queried 2026-10-08
- .planning/PROJECT.md
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

- **Sketch findings for mini-fab-simulator-kafka-mq** (design decisions, CSS patterns, visual direction) → `Skill("sketch-findings-mini-fab-simulator-kafka-mq")`

## Diagram Rules

- 繪製流程圖、架構圖或循序圖(sequence diagram)之前,必須先呼叫 `Skill("diagram-design")`。
- 不得自行手寫 SVG / Mermaid / HTML 圖表。
- 唯一例外:使用者明確要求時。

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
