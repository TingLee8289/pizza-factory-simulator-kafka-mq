"""Dashboard runtime: replay, Kafka tails, KPI relay, panel poller, tick loop (D-01..D-03).

The dashboard is a read model. It rebuilds line state by an earliest replay of
``station.events`` + ``order.lifecycle`` (newest run only), then tails live. KPI
snapshots computed by the kpi-service are relayed untouched; no KPI math
happens in this module (D-01).

All state mutation happens on the event loop thread under one asyncio.Lock;
KafkaTail hands messages over via run_coroutine_threadsafe and the callbacks
never raise into the Kafka thread (T-05-36).

Research defaults documented here:
- Reset returns the UI to "No active run" (default 1).
- The run seed is lost on a dashboard restart; ``adopt`` leaves it unknown (default 3).
- A pause lost on station restart is accepted (default 4).
- An offline station stays a grey chip (default 5).

Known v1 limitation (accepted, no new fact added): after Reset, a dashboard
restart replays Kafka, finds no reset fact, and re-adopts the newest run from
Kafka, so the previous run reappears instead of "No active run".
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI

from pizza.common.bootstrap import verify_kafka, verify_rabbit
from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    ComparisonResult,
    EstimateWip,
    HostSleepDetected,
    KpiSnapshot,
    Message,
    StationStatus,
    TimeScaleChanged,
)
from pizza.common.kafka_io import make_producer, produce_fact
from pizza.common.rabbit_io import connect
from pizza.common.recipes import load_all_recipes
from pizza.common.rpc import RpcBusy, RpcClient, RpcTimeout
from pizza.common.topology import (
    EX_CONTROL,
    EX_WORKORDERS,
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    TOPIC_TIMESCALE,
)
from pizza.dashboard.comparison import ComparisonRunner, comparison_rows
from pizza.dashboard.control import ConflictError, RunController, purge_line_queues
from pizza.dashboard.hub import Hub
from pizza.dashboard.orders import ManualOrderClient, estimate_order, place_manual_order
from pizza.dashboard.panel import PanelPoller
from pizza.dashboard.settings import DashboardSettings, get_dashboard_settings
from pizza.dashboard.state import (
    LineState,
    active_lots,
    event_payload,
    fold,
    line_snapshot,
    lot_detail,
    lot_status,
    new_line_state,
    new_run_state,
    recent_done,
)
from pizza.kpi.fold import newest_run_id
from pizza.kpi.recompute import build_cfg_and_recipes
from pizza.scheduler.replay import KafkaTail, replay_topics

_log = structlog.get_logger()

# sim.timescale rides along so the dashboard restores and follows the current speed.
LINE_TOPICS = [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE]
TIMESCALE_KEY = "line"
MAX_RUNS = 200
ESTIMATE_WIP_KEY = "scheduler.estimate_wip"
MAX_ETA_NOW = 200
# Station reports kept until the next start callback (about one per station per run start).
_EARLY_STATUS_CAP = 64
# A HostSleepDetected fact older than this is stale (late delivery / tail catch-up).
_HOST_SLEEP_MAX_AGE_S = 30.0


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class RuntimeDeps:
    """Injectable collaborators so tests never touch brokers."""

    replay: Callable[..., Any] = replay_topics
    tail_factory: Callable[..., Any] = KafkaTail
    connect: Callable[[str], Awaitable[Any]] = connect
    make_producer: Callable[[str], Any] = make_producer
    verify: Callable[["DashboardRuntime", Any], Awaitable[None]] | None = None
    panel: Any = None


async def _verify_brokers(runtime: "DashboardRuntime", connection: Any) -> None:
    await verify_rabbit(connection)
    admin = AdminClient({"bootstrap.servers": runtime.settings.kafka_bootstrap})
    await asyncio.to_thread(verify_kafka, admin)


class DashboardRuntime:
    def __init__(self, settings: DashboardSettings, deps: RuntimeDeps | None = None) -> None:
        self.settings = settings
        self.deps = deps or RuntimeDeps()
        self.hub = Hub(
            max_clients=settings.max_sse_clients,
            queue_size=settings.subscriber_queue_size,
            kpi_ring_size=settings.kpi_ring_size,
            panel_ring_size=settings.panel_ring_size,
        )
        self.state: LineState = new_line_state(None, adopt=True)
        # Station reports seen since the last start callback; see on_change / new_run_state.
        self._early_status: list[StationStatus] = []
        self.latest_kpi: KpiSnapshot | None = None
        self.latest_panel: dict[str, Any] | None = None
        self.replaying = True
        self.producer: Any = None
        self.time_scale: float = 1.0
        # Banner info after a host-sleep auto pause; cleared on resume/reset/start.
        self.host_sleep: dict[str, Any] | None = None
        self.connection: Any = None
        self.channel: Any = None
        self.order_client = ManualOrderClient(lambda: self.channel, self._get_control_exchange)
        # Separate RPC client for estimate dry-runs: bounded in-flight so a flood is rejected.
        self.eta_client = RpcClient(
            lambda: self.channel, self._get_control_exchange, max_inflight=4
        )
        self.controller = RunController(
            settings,
            self._get_control_exchange,
            self._purge,
            on_change=self.on_change,
            place_order_fn=self._place_order,
            estimate_order_fn=self._estimate_order,
            lot_state_fn=lambda oid: lot_status(self.state, oid),
        )
        self.comparison = ComparisonRunner(self.controller, on_change=self._on_comparison_change)
        # Run history (plan 08, D-12/Open Question 4): run_id -> row built from the
        # newest KpiSnapshot.run_params seen for that run, cap MAX_RUNS by eviction.
        self.runs: dict[str, dict[str, Any]] = {}
        # Lazy-cached (cfg, recipes) for the read-only recompute route (D-13/D-14).
        self._recompute_cfg: tuple[Any, Any] | None = None
        # Live recipe tables (station -> product -> seconds per pizza). Loaded on
        # first use and kept in step with edits made through the inspector.
        self._recipes: dict[str, dict[str, float]] | None = None
        self.recompute_sem = asyncio.Semaphore(1)
        self._lock = asyncio.Lock()
        self._pending_point: dict[str, Any] | None = None
        self._exchanges: dict[str, Any] = {}
        self._tails: list[Any] = []
        self._tick_task: asyncio.Task[None] | None = None
        self._panel_task: asyncio.Task[None] | None = None
        # Phase 7.1 D-06: volatile current-estimate cache (order_id -> ISO Z), polled
        # from the scheduler in the background and only merged into payloads.
        self.eta_now: dict[str, str] = {}
        self._eta_task: asyncio.Task[None] | None = None
        self._panel_stop = asyncio.Event()
        self._panel: Any = None

    # -- controller wiring ------------------------------------------------
    async def _exchange(self, name: str) -> Any:
        if name not in self._exchanges:
            self._exchanges[name] = await self.channel.get_exchange(name)
        return self._exchanges[name]

    async def _get_control_exchange(self) -> Any:
        return await self._exchange(EX_CONTROL)

    async def _get_workorders_exchange(self) -> Any:
        return await self._exchange(EX_WORKORDERS)

    async def _purge(self) -> Any:
        return await purge_line_queues(self.channel)

    async def _place_order(self, controller: Any, request: Any) -> dict[str, Any]:
        return await place_manual_order(
            controller,
            request,
            self.order_client,
            self.settings,
        )

    async def _estimate_order(self, controller: Any, request: Any) -> dict[str, Any]:
        return await estimate_order(controller, request, self.eta_client, self.settings)

    async def on_change(self, event: str, info: dict[str, Any]) -> None:
        """Controller callback; ``paused``/``resumed`` state itself comes from the facts."""
        if event in ("started", "reset", "resumed"):
            self.host_sleep = None
        if event in ("started", "reset"):
            async with self._lock:
                run_id = info.get("run_id") if event == "started" else None
                if run_id is None:
                    self.state = new_line_state(None, adopt=False)
                else:
                    self.state = new_run_state(run_id, self._early_status)
                self._early_status = []
                self.latest_kpi = None
                self._pending_point = None
                self.eta_now = {}
                self.hub.clear_rings()
        self.publish_tick()

    async def _on_comparison_change(self, _event: str, _status: dict[str, Any]) -> None:
        """ComparisonRunner callback; status is recomputed fresh so ``runs`` is included."""
        try:
            self.hub.publish("events", "comparison", self.comparison_status())
        except Exception:  # noqa: BLE001
            _log.exception("dashboard_comparison_publish_failed")

    def comparison_status(self) -> dict[str, Any]:
        """Comparison status enriched with the run history (for ``interrupted``, D-12)."""
        return self.comparison.status(self.runs)

    # -- run history (plan 08, D-12/Open Question 4) -----------------------
    def _update_run(self, snapshot: KpiSnapshot) -> None:
        """Record/refresh one run's history row from any KpiSnapshot (before the
        active run_id filter), so History survives a dashboard restart."""
        params = snapshot.run_params
        started_ts = snapshot.run_started_ts or snapshot.ts
        entry = {
            "run_id": snapshot.run_id,
            "rule": params.rule if params else None,
            "seed": params.seed if params else None,
            "n_orders": params.max_orders if params else None,
            "comparison_id": params.comparison_id if params else None,
            "round": params.round if params else None,
            "started_ts": _iso(started_ts),
            "has_snapshot": True,
        }
        if snapshot.run_id not in self.runs and len(self.runs) >= MAX_RUNS:
            del self.runs[next(iter(self.runs))]  # oldest inserted, evicted first
        self.runs[snapshot.run_id] = entry

    def runs_list(self, limit: int = 20) -> list[dict[str, Any]]:
        """At most ``limit`` runs, newest first (by ``started_ts``)."""
        items = sorted(self.runs.values(), key=lambda r: r["started_ts"] or "", reverse=True)
        return items[:limit]

    def recompute_cfg(self) -> tuple[Any, Any]:
        """Lazily build the KPI config; recipes always reflect live edits."""
        if self._recompute_cfg is None:
            self._recompute_cfg = build_cfg_and_recipes()
        cfg, _ = self._recompute_cfg
        return cfg, self.recipe_tables()

    def recipe_tables(self) -> dict[str, dict[str, float]]:
        """Current unit-time tables (station -> product -> seconds per pizza)."""
        if self._recipes is None:
            self._recipes = load_all_recipes(getattr(self.settings, "recipes_dir", None))
        return self._recipes

    async def set_recipe(self, name: str, request: Any) -> dict[str, Any]:
        """Publish a live recipe edit, then mirror it in this read model."""
        result = await self.controller.set_recipe(name, request)
        table = self.recipe_tables().get(name)
        if table is not None:
            table[request.product] = request.unit_time_s
        return result

    async def set_time_scale(self, scale: int) -> dict[str, Any]:
        """Broadcast a new simulation speed; state updates only after the publish succeeds."""
        fact = TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=scale)
        await asyncio.to_thread(produce_fact, self.producer, TOPIC_TIMESCALE, fact, TIMESCALE_KEY)
        self.time_scale = float(scale)
        return {"scale": scale}

    # -- Kafka callbacks (event loop thread, never raise) --------------------
    async def on_line_event(self, msg: Message) -> None:
        if isinstance(msg, TimeScaleChanged):
            self.time_scale = msg.scale  # global speed, not part of any run's line state
            return
        if isinstance(msg, HostSleepDetected):
            await self._on_host_sleep(msg)
            return
        try:
            async with self._lock:
                if isinstance(msg, StationStatus):
                    self._early_status = [*self._early_status[-(_EARLY_STATUS_CAP - 1) :], msg]
                self.state = fold(self.state, msg)
                payload = event_payload(msg)
                if payload is not None:
                    payload["seq"] = self.hub.seq + 1
                    self.hub.publish("events", "fact", payload)
        except Exception:  # noqa: BLE001 - never raise into the Kafka thread
            _log.exception("dashboard_on_line_event_failed")

    async def _on_host_sleep(self, msg: HostSleepDetected) -> None:
        """Auto-pause through the normal controller path after the host slept.

        Runs outside ``self._lock`` (controller.pause notifies on_change). Never raises.
        """
        try:
            if self.replaying or self.controller.state != "running":
                return
            if (datetime.now(UTC) - msg.ts).total_seconds() > _HOST_SLEEP_MAX_AGE_S:
                return
            if msg.run_id != self.controller.run_info().get("run_id"):
                return
            try:
                await self.controller.pause(slept_s=msg.slept_s)
            except ConflictError:
                # Already paused, or a comparison round locks the controls: only warn.
                _log.warning("dashboard_host_sleep_not_paused", run_id=msg.run_id)
                return
            self.host_sleep = {
                "slept_s": msg.slept_s,
                "detected_ts": _iso(msg.ts),
                "run_id": msg.run_id,
            }
            self.publish_tick()
        except Exception:  # noqa: BLE001 - never raise into the Kafka thread
            _log.exception("dashboard_host_sleep_failed")

    async def on_kpi(self, snapshot: Message) -> None:
        try:
            async with self._lock:
                if isinstance(snapshot, ComparisonResult):
                    await self.comparison.on_result(snapshot)
                    key = (snapshot.comparison_id, snapshot.round)
                    row = comparison_rows({key: snapshot})[0]
                    self.hub.publish("events", "comparison_result", row)
                    self.hub.publish("events", "comparison", self.comparison_status())
                    return
                if not isinstance(snapshot, KpiSnapshot):
                    return
                self._update_run(snapshot)
                if self.state.run_id is not None and snapshot.run_id != self.state.run_id:
                    return
                self.latest_kpi = snapshot
                self._pending_point = self.hub.add_kpi(snapshot)
        except Exception:  # noqa: BLE001
            _log.exception("dashboard_on_kpi_failed")

    def on_panel(self, panel: dict[str, Any]) -> None:
        self.latest_panel = panel
        series: dict[str, float | None] = {"rabbit.dlq": panel.get("rabbit", {}).get("dlq")}
        for name, q in panel.get("rabbit", {}).get("queues", {}).items():
            series[f"rabbit.{name}.ready"] = q.get("ready")
            series[f"rabbit.{name}.unacked"] = q.get("unacked")
        for group, lag in panel.get("kafka", {}).get("lag", {}).items():
            series[f"kafka.{group}.lag"] = lag
        self.hub.add_panel(series)

    # -- payloads ---------------------------------------------------------
    def _kpi_json(self) -> dict[str, Any] | None:
        if self.latest_kpi is None:
            return None
        return self.latest_kpi.model_dump(mode="json")

    def _merge_eta_now(self, lots: list[dict[str, Any]]) -> None:
        """Overlay the cached current estimate on in-flight lots (no I/O)."""
        for lot in lots:
            if lot["state"] != "done":
                eta = self.eta_now.get(lot["order_id"])
                if eta is not None:
                    lot["eta_now_ts"] = eta
                else:
                    lot.pop("eta_now_ts", None)

    async def _poll_eta_once(self) -> None:
        if not self.settings.eta_poll_enabled or self.replaying or self.controller.state == "none":
            return
        run_id = self.controller.run_info().get("run_id")
        if not run_id:
            return
        timeout_s = float(self.settings.eta_timeout_s)
        cmd = EstimateWip.make(
            run_id=run_id,
            machine_id="dashboard",
            deadline_ts=datetime.now(UTC) + timedelta(seconds=timeout_s),
        )
        reply = await self.eta_client.request(cmd, ESTIMATE_WIP_KEY, timeout_s)
        eta = reply.get("ok") is True and reply.get("eta")
        if not isinstance(eta, dict):
            return
        fresh: dict[str, str] = {}
        for key, value in eta.items():
            if isinstance(key, str) and isinstance(value, str):
                fresh[key] = value
                if len(fresh) >= MAX_ETA_NOW:
                    break
        self.eta_now = fresh

    def lot_detail(self, order_id: str) -> dict[str, Any] | None:
        return lot_detail(self.state, order_id, datetime.now(UTC))

    async def lot_eta(self, order_id: str) -> dict[str, Any]:
        """Live per-station ~IN/~OUT for one lot (on-demand RPC; volatile, never cached/SSE)."""
        unavailable: dict[str, Any] = {"available": False, "stations": {}}
        if not self.settings.eta_poll_enabled or self.replaying or self.controller.state == "none":
            return unavailable
        run_id = self.controller.run_info().get("run_id")
        if not run_id:
            return unavailable
        timeout_s = float(self.settings.eta_timeout_s)
        try:
            cmd = EstimateWip.make(
                run_id=run_id,
                machine_id="dashboard",
                order_id=order_id,
                deadline_ts=datetime.now(UTC) + timedelta(seconds=timeout_s),
            )
            reply = await self.eta_client.request(cmd, ESTIMATE_WIP_KEY, timeout_s)
        except (RpcTimeout, RpcBusy):
            return unavailable
        except Exception:  # noqa: BLE001 - an ETA hint must never 5xx
            _log.exception("dashboard_lot_eta_failed")
            return unavailable
        raw = reply.get("stations") if reply.get("ok") is True else None
        if not isinstance(raw, dict):
            return unavailable
        stations: dict[str, dict[str, str | None]] = {}
        for name in STATIONS:
            entry = raw.get(name)
            if not isinstance(entry, dict) or not isinstance(entry.get("out"), str):
                continue
            t_in = entry.get("in")
            stations[name] = {"in": t_in if isinstance(t_in, str) else None, "out": entry["out"]}
        return {"available": bool(stations), "stations": stations}

    async def _eta_poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.eta_poll_s)
            try:
                await self._poll_eta_once()
            except Exception:  # noqa: BLE001 - the poll loop never dies
                _log.exception("dashboard_eta_poll_failed")

    def snapshot_payload(self) -> dict[str, Any]:
        line = line_snapshot(self.state, datetime.now(UTC), summary=True)
        self._merge_eta_now(line["lots"])
        return {
            "seq": self.hub.seq,
            "server_ts": _now_iso(),
            "replaying": self.replaying,
            "run": self.controller.run_info(),
            "line": line,
            "kpi": self._kpi_json(),
            "history": list(self.hub.kpi_ring),
            "panel": self.latest_panel,
            "panel_history": {k: list(v) for k, v in self.hub.panel_rings.items()},
            "comparison": self.comparison_status(),
            "runs": self.runs_list(),
            "time_scale": self.time_scale,
            "host_sleep": self.host_sleep,
        }

    def tick_payload(self) -> dict[str, Any]:
        """Tick payload; hands the KPI chart point out exactly once (no duplicates)."""
        line = line_snapshot(self.state, datetime.now(UTC), summary=True)
        self._merge_eta_now(line["lots"])
        point, self._pending_point = self._pending_point, None
        return {
            "seq": self.hub.seq + 1,
            "server_ts": _now_iso(),
            "run": self.controller.run_info(),
            "paused": line["paused"],
            "rule": line["rule"],
            "stations": line["stations"],
            "lots": active_lots(line),
            "done_recent": recent_done(line, 20),
            "counts": line["counts"],
            "kpi": self._kpi_json(),
            "kpi_point": point,
            "panel": self.latest_panel,
            "time_scale": self.time_scale,
            "host_sleep": self.host_sleep,
        }

    def publish_tick(self) -> None:
        try:
            self.hub.publish("state", "tick", self.tick_payload())
        except Exception:  # noqa: BLE001
            _log.exception("dashboard_tick_failed")

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.state_interval_s)
            self.publish_tick()  # swallows its own errors: the loop never dies
            try:
                if self.comparison.state == "round_running":
                    run_id = self.controller.run_info().get("run_id")
                    counts = line_snapshot(self.state)["counts"]
                    if run_id is not None:
                        await self.comparison.on_progress(
                            run_id, counts["completed"], counts["failed"]
                        )
            except Exception:  # noqa: BLE001 - the tick loop never dies
                _log.exception("dashboard_comparison_progress_failed")

    # -- lifecycle --------------------------------------------------------
    async def startup(self) -> None:
        s = self.settings
        deps = self.deps
        self.connection = await deps.connect(s.rabbitmq_url)  # URL is never logged
        await (deps.verify or _verify_brokers)(self, self.connection)
        self.channel = await self.connection.channel()
        self.producer = deps.make_producer(s.kafka_bootstrap)

        # Each tail starts exactly where its own replay stopped (high watermarks).
        snap_msgs, kpi_high = await asyncio.to_thread(
            deps.replay,
            s.kafka_bootstrap,
            [TOPIC_KPI_SNAPSHOTS],
            s.replay_timeout_s,
            s.kafka_group_id,
        )
        facts, high = await asyncio.to_thread(
            deps.replay, s.kafka_bootstrap, LINE_TOPICS, s.replay_timeout_s, s.kafka_group_id
        )
        await self._recover(facts, snap_msgs)

        loop = asyncio.get_running_loop()
        self._tails = [
            deps.tail_factory(
                s.kafka_bootstrap,
                s.kafka_group_id,
                high,
                LINE_TOPICS,
                self.on_line_event,
                loop,
                thread_name="dashboard-tail",
            ),
            deps.tail_factory(
                s.kafka_bootstrap,
                s.kafka_group_id,
                kpi_high,
                [TOPIC_KPI_SNAPSHOTS],
                self.on_kpi,
                loop,
                thread_name="dashboard-kpi-tail",
            ),
        ]
        for tail in self._tails:
            tail.start()

        self._panel = deps.panel or PanelPoller(s)
        self._panel_task = asyncio.create_task(self._panel.run(self.on_panel, self._panel_stop))
        self._tick_task = asyncio.create_task(self._tick_loop())
        self._eta_task = asyncio.create_task(self._eta_poll_loop())
        self.replaying = False
        _log.info("dashboard_started", run_id=self.state.run_id)

    async def _recover(self, facts: list[Message], snapshots: list[Message]) -> None:
        run_id = newest_run_id(facts)
        async with self._lock:
            for fact in facts:
                if isinstance(fact, TimeScaleChanged):
                    self.time_scale = fact.scale  # replay is offset-ordered: last wins
            if run_id is not None:
                state = new_line_state(run_id, adopt=True)
                for fact in facts:
                    if getattr(fact, "run_id", None) == run_id:
                        state = fold(state, fact)
                self.state = state
                self.controller.adopt(run_id, line_snapshot(state)["paused"])
            # Rebuild the comparison result store and run history from every
            # snapshot (not just the adopted run); the runner's idle _comparison_id
            # never matches a stored result, so no hub publish fires here (D-12).
            for sn in snapshots:
                if isinstance(sn, ComparisonResult):
                    await self.comparison.on_result(sn)
                elif isinstance(sn, KpiSnapshot):
                    self._update_run(sn)
            mine = sorted(
                (
                    sn
                    for sn in snapshots
                    if isinstance(sn, KpiSnapshot) and run_id is not None and sn.run_id == run_id
                ),
                key=lambda sn: sn.ts,
            )
            for sn in mine:
                self.hub.add_kpi(sn)
            if mine:
                self.latest_kpi = mine[-1]
        _log.info("dashboard_rebuilt", run_id=run_id, lots=len(self.state.lots))

    async def shutdown(self) -> None:
        for tail in self._tails:
            await asyncio.to_thread(tail.stop)
        self._panel_stop.set()
        for task in (self._tick_task, self._eta_task, self._panel_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if self._panel is not None:
            with contextlib.suppress(Exception):
                await self._panel.aclose()
        for closer in (self.channel, self.connection):
            if closer is not None:
                with contextlib.suppress(Exception):
                    await closer.close()
        _log.info("dashboard_stopped")


def create_app(runtime: Any = None) -> FastAPI:
    """Build the FastAPI app; without an injected runtime the lifespan owns a real one."""
    from pizza.dashboard.routes import install

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if runtime is not None:
            app.state.runtime = runtime
            yield
            return
        real = DashboardRuntime(get_dashboard_settings())
        await real.startup()  # broker failure raises: the restart policy applies
        app.state.runtime = real
        try:
            yield
        finally:
            await real.shutdown()

    app = FastAPI(title="Pizza Factory Dashboard", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.runtime = runtime
    install(app)
    return app
