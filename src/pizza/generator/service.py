"""Generator service: start on run.started, publish seeded orders in wall-clock time.

Every handler path ends in ack, nack(requeue=True) or dead_letter_invalid (which
acks). the dead-letter-into-retry rejection variants are never used (see station service header).
"""

import asyncio
import contextlib
import dataclasses
import json
import math
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterator, Mapping
from datetime import datetime, timedelta
from typing import Any

import aio_pika
import structlog
from confluent_kafka.admin import AdminClient

from pizza.common.bootstrap import verify_kafka, verify_rabbit
from pizza.common.envelope import (
    GENERATOR_CONTROL_TYPES,
    EstimateEta,
    HostSleepDetected,
    InvalidEnvelope,
    ManualOrderRequest,
    OrderLifecycle,
    OrderSubmitted,
    RunParams,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    parse_message,
    utc_now,
)
from pizza.common.ids import make_order_id
from pizza.common.kafka_io import produce_fact
from pizza.common.pause import PauseGate
from pizza.common.rabbit_io import dead_letter_invalid, publish_command
from pizza.common.recipes import load_all_recipes, theoretical_total_s, validate_recipes
from pizza.common.rpc import RpcBusy, RpcClient, RpcTimeout
from pizza.common.sleepwatch import HostSleepDetector
from pizza.common.timescale import TimeScale, TimeScaleFeed
from pizza.common.topology import (
    EX_CONTROL,
    EX_WORKORDERS,
    Q_GENERATOR_CONTROL,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
)
from pizza.generator.plan import PlannedOrder, plan_orders

_log = structlog.get_logger()

MACHINE_ID = "generator"
INTAKE_KEY = "intake"
_REPLY_CACHE_CAP = 256


def _iso(value: Any) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Generator:
    """Single active run task; a new run.started replaces it (D-18)."""

    def __init__(
        self,
        producer: Any,
        settings: Any,
        channel: Any = None,
        exchange: Any = None,
        recipes: Mapping[str, Mapping[str, float]] | None = None,
        sleep: Callable[[float], Any] | None = None,
        clock: Callable[[], float] | None = None,
        estimator: Callable[..., Awaitable[dict[str, Any] | None]] | None = None,
        wall: Callable[[], datetime] | None = None,
    ) -> None:
        self.producer = producer
        self.settings = settings
        self.channel = channel
        self._exchange = exchange
        self._recipes = recipes
        # Order offsets, due leads and pause time are simulated seconds; the default
        # sleep/clock are the scaled ones (quick-et6). Tests may inject their own.
        self.timescale = TimeScale()
        self._sleep = sleep if sleep is not None else self.timescale.sleep
        self._clock = clock if clock is not None else self.timescale.sim_now
        # UTC wall clock used to stamp auto orders at their actual release. The sim
        # clock above is monotonic-based and stops while the host sleeps; the wall
        # clock does not, so timestamps must never be derived from the sim clock.
        self._wall = wall if wall is not None else utc_now
        self._timescale_feed: TimeScaleFeed | None = None
        # Host sleep watch uses the real monotonic clock and the wall clock, never the
        # scaled sim clock. Pausing is requested via the dashboard (RunPause broadcast).
        # Created lazily (first sample is the baseline) so the wall clock is not read
        # at construction time.
        self._sleepwatch: HostSleepDetector | None = None
        self._sleep_poll_s = float(getattr(settings, "sleep_poll_s", 1.0))
        self.current_run_id: str | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._inflight = 0
        self.gate = PauseGate(clock=self._clock, timescale=self.timescale)
        self._rate: float | None = None
        # One per-run seq counter shared by auto and manual orders (quick-wrj).
        self._next_seq = 0
        self._seed = 0
        self._manual_replies: OrderedDict[str, dict[str, Any]] = OrderedDict()
        # Phase 7.1: due source + buffer for ETA-derived due dates (set per run).
        self._due_mode = "eta"
        self._buffer_k = 1.0
        self._rpc: RpcClient | None = None
        self._estimator = estimator if estimator is not None else self._rpc_estimate

    def _alloc_seq(self) -> int:
        """Allocate the next order seq (sync: no await, so no race between tasks)."""
        self._next_seq += 1
        return self._next_seq

    def _enter(self) -> None:
        self._inflight += 1
        self._idle.clear()

    def _leave(self) -> None:
        self._inflight -= 1
        if self._inflight <= 0:
            self._inflight = 0
            self._idle.set()

    async def _nack_after_delay(self, message: Any) -> None:
        await self._sleep(self.settings.nack_delay_s)
        await message.nack(requeue=True)

    def _load_recipes(self) -> Mapping[str, Mapping[str, float]]:
        if self._recipes is None:
            recipes_dir = getattr(self.settings, "recipes_dir", None)
            recipes = load_all_recipes(recipes_dir if isinstance(recipes_dir, str) else None)
            validate_recipes(recipes)
            self._recipes = recipes
        return self._recipes

    # -- control ----------------------------------------------------------
    async def handle_control(self, message: Any) -> None:
        """Control-queue callback; never lets an exception escape."""
        self._enter()
        try:
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(
                    self.channel, message, f"invalid envelope: {exc}", Q_GENERATOR_CONTROL
                )
                return
            if not isinstance(parsed, GENERATOR_CONTROL_TYPES):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"unexpected message type {parsed.type}",
                    Q_GENERATOR_CONTROL,
                )
                return
            if isinstance(parsed, ManualOrderRequest):
                await self._handle_manual(parsed, message)
                return
            if isinstance(parsed, RunPause):
                self.gate.pause("run")
            elif isinstance(parsed, RunResume):
                self.gate.resume("run")
            elif isinstance(parsed, RunReset):
                await self._cancel_task()
                self.current_run_id = None
                self._rate = None
                self._next_seq = 0
                self._manual_replies.clear()
                self._due_mode = "eta"
                self._buffer_k = 1.0
                self.gate.clear()
            elif isinstance(parsed, SetArrivalRate):
                if self.current_run_id is not None:
                    self._rate = parsed.arrival_rate
                else:
                    _log.info("generator_set_rate_ignored_no_run")
            elif isinstance(parsed, RunStarted):
                await self._start_run(parsed, message)
                return
            await message.ack()
        except Exception:  # noqa: BLE001 - last resort so the consumer never stalls
            _log.exception("generator_handle_control_failed")
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("generator_nack_failed")
        finally:
            self._leave()

    async def _start_run(self, parsed: RunStarted, message: Any) -> None:
        if parsed.run_id == self.current_run_id:
            # Redelivery / duplicate run.started: orders are deterministic, so
            # restarting the same run would only duplicate them (T-03-11).
            _log.info("generator_duplicate_run_ignored", run_id=parsed.run_id)
            await message.ack()
            return
        params = parsed.params or RunParams()
        try:
            recipes = self._load_recipes()
            planned = plan_orders(
                params,
                parsed.ts,
                recipes,
                rate_source=lambda: self._rate if self._rate is not None else params.arrival_rate,
                priority_high_ratio=self.settings.priority_high_ratio,
                priority_high_value=self.settings.priority_high_value,
            )
        except ValueError as exc:  # RecipeError, unknown product (T-03-09)
            await dead_letter_invalid(
                self.channel, message, f"invalid run: {exc}", Q_GENERATOR_CONTROL
            )
            return
        await self._cancel_task()
        self._rate = None
        self.gate.clear()
        self.current_run_id = parsed.run_id
        self._next_seq = 0
        self._seed = params.seed
        self._due_mode = params.due_mode
        self._buffer_k = params.buffer_k
        self._manual_replies.clear()
        self._task = asyncio.create_task(self._run_orders(parsed.run_id, planned, seed=params.seed))
        _log.info("generator_run_started", run_id=parsed.run_id)
        await message.ack()

    async def _cancel_task(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # -- publishing -------------------------------------------------------
    async def _get_exchange(self) -> Any:
        if self._exchange is None:
            self._exchange = await self.channel.get_exchange(EX_WORKORDERS)
        return self._exchange

    def _delay(self, base: float, paused0: float, order: PlannedOrder) -> float:
        """Seconds until ``order`` is due, extended by time paused during this run (D-04).

        Pause time is measured relative to the run-start baseline ``paused0``
        because ``PauseGate.paused_seconds()`` is lifetime-cumulative (CR-01).
        """
        return base + order.offset_s + (self.gate.paused_seconds() - paused0) - self._clock()

    def _planned_release(self, order: PlannedOrder, paused0: float, lag0: float) -> datetime:
        """Planned release wall time: t0 + sim offset, shifted by run pause time (sim)
        and by the wall/sim lag accumulated through scale changes (zero at constant 1x).
        Host sleep is not in this shift, which is exactly what release lag measures."""
        shift = timedelta(
            seconds=(self.gate.paused_seconds() - paused0) + (self.timescale.wall_lag() - lag0)
        )
        return order.created_ts + shift

    @staticmethod
    def _note_release_lag(
        run_id: str, order: PlannedOrder, lag_s: float, warned_lag: float, warn_s: float
    ) -> float:
        """Log release lag; warn once per jump of more than ``warn_s``. Returns the new
        warning baseline (a recovered lag lowers it so the next jump warns again)."""
        _log.debug("generator_order_released", run_id=run_id, release_lag_s=lag_s)
        if lag_s - warned_lag > warn_s:
            _log.warning(
                "generator_release_lag",
                run_id=run_id,
                planned_offset_s=order.offset_s,
                release_lag_s=lag_s,
                jump_s=round(lag_s - warned_lag, 3),
            )
            return lag_s
        return min(warned_lag, max(0.0, lag_s))

    async def _run_orders(
        self, run_id: str, planned: Iterator[PlannedOrder], *, seed: int = 0
    ) -> None:
        base = self._clock()
        paused0 = self.gate.paused_seconds()
        lag0 = self.timescale.wall_lag()
        warn_s = float(getattr(self.settings, "release_lag_warn_s", 5.0))
        warned_lag = 0.0
        try:
            for order in planned:
                await self._sleep(max(0.0, self._delay(base, paused0, order)))
                await self.gate.wait_running()
                delay = self._delay(base, paused0, order)
                while delay > 0:
                    await self._sleep(delay)
                    await self.gate.wait_running()
                    delay = self._delay(base, paused0, order)
                # The sim offsets only pace releases. created_ts is the actual release
                # wall time (host sleep freezes the monotonic sim clock but not the
                # wall clock); the planned lead keeps its length, scaled to wall time.
                released = self._trunc(self._wall())
                lag_s = round(
                    (released - self._planned_release(order, paused0, lag0)).total_seconds(), 3
                )
                warned_lag = self._note_release_lag(run_id, order, lag_s, warned_lag, warn_s)
                lead = (order.due_ts - order.created_ts) / self.timescale.scale
                order = dataclasses.replace(order, created_ts=released, due_ts=released + lead)
                order = await self._with_eta(run_id, order)
                seq = self._alloc_seq()
                order = dataclasses.replace(order, seq=seq, order_id=make_order_id(seed, seq))
                await self._publish_with_retry(run_id, order, release_lag_s=lag_s)
            _log.info("generator_run_finished", run_id=run_id)
        except asyncio.CancelledError:
            _log.info("generator_run_cancelled", run_id=run_id)

    # -- host sleep watch ---------------------------------------------------
    async def _sleep_tick(self) -> None:
        """One sample; emit HostSleepDetected on a jump while a run is active and running.

        The generator does not pause itself: the dashboard turns the fact into the
        normal RunPause broadcast so every service and the controller state agree.
        """
        if self._sleepwatch is None:
            self._sleepwatch = HostSleepDetector(
                float(getattr(self.settings, "sleep_detect_s", 5.0)), wall=self._wall
            )
            return
        jump = self._sleepwatch.check()
        if jump is None or self.current_run_id is None or self.gate.is_paused:
            return
        run_id = self.current_run_id
        _log.warning("generator_host_sleep_detected", run_id=run_id, slept_s=jump)
        try:
            fact = HostSleepDetected.make(run_id=run_id, machine_id=MACHINE_ID, slept_s=jump)
            await asyncio.to_thread(
                produce_fact, self.producer, TOPIC_STATION_EVENTS, fact, MACHINE_ID
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - never kill the watch loop; no resend
            _log.exception("generator_host_sleep_publish_failed", run_id=run_id)

    async def _watch_host_sleep(self) -> None:
        await self._sleep_tick()  # establishes the baseline sample
        while True:
            await asyncio.sleep(self._sleep_poll_s)  # real time, not the pausable gate sleep
            try:
                await self._sleep_tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                _log.exception("generator_host_sleep_tick_failed")

    # -- ETA at release (Phase 7.1) ----------------------------------------
    async def _control_exchange(self) -> Any:
        return await self.channel.get_exchange(EX_CONTROL)

    async def _rpc_estimate(
        self,
        run_id: str,
        product: str,
        qty: int,
        priority: int,
        due_ts: datetime | None,
        buffer_k: float,
    ) -> dict[str, Any] | None:
        """Ask the scheduler for an ETA; None on any failure (caller falls back)."""
        timeout_s = float(getattr(self.settings, "eta_timeout_s", 1.5))
        try:
            if self._rpc is None:
                self._rpc = RpcClient(lambda: self.channel, self._control_exchange)
            cmd = EstimateEta.make(
                run_id=run_id,
                machine_id=MACHINE_ID,
                product=product,
                qty=qty,
                priority=priority,
                due_ts=due_ts,
                buffer_k=buffer_k,
                deadline_ts=utc_now() + timedelta(seconds=timeout_s),
            )
            reply = await self._rpc.request(cmd, "scheduler.estimate_eta", timeout_s)
        except RpcTimeout:
            _log.warning("generator_eta_unavailable", reason="timeout", run_id=run_id)
            return None
        except RpcBusy:
            _log.warning("generator_eta_unavailable", reason="busy", run_id=run_id)
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - ETA is best effort
            _log.warning("generator_eta_unavailable", reason=repr(exc), run_id=run_id)
            return None
        if reply.get("ok") is not True:
            _log.warning("generator_eta_unavailable", reason=str(reply.get("code")), run_id=run_id)
            return None
        return reply

    @staticmethod
    def _valid_estimate(est: Mapping[str, Any] | None) -> bool:
        """Reply fields must be finite non-negative numbers (T-07.1-19)."""

        def _ok(v: Any) -> bool:
            return (
                isinstance(v, int | float)
                and not isinstance(v, bool)
                and math.isfinite(v)
                and v >= 0
            )

        if not isinstance(est, Mapping) or not _ok(est.get("eta_lead_s")):
            return False
        leads = est.get("station_lead_s")
        if not isinstance(leads, Mapping):
            return False
        return all(isinstance(k, str) and _ok(v) for k, v in leads.items())

    @staticmethod
    def _trunc(value: datetime) -> datetime:
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    def _eta_fields(
        self, created: datetime, est: Mapping[str, Any]
    ) -> tuple[datetime, dict[str, datetime]]:
        eta_ts = self._trunc(created + timedelta(seconds=float(est["eta_lead_s"])))
        station_eta = {
            s: self._trunc(created + timedelta(seconds=float(v)))
            for s, v in est["station_lead_s"].items()
        }
        return eta_ts, station_eta

    async def _estimate(
        self,
        run_id: str,
        product: str,
        qty: int,
        priority: int,
        due_ts: datetime | None,
        buffer_k: float,
    ) -> dict[str, Any] | None:
        """Call the estimator once; validate; None means fall back to theory due."""
        try:
            est = await self._estimator(run_id, product, qty, priority, due_ts, buffer_k)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _log.warning("generator_eta_unavailable", reason=repr(exc), run_id=run_id)
            return None
        if est is None:
            return None
        if not self._valid_estimate(est):
            _log.warning("generator_eta_unavailable", reason="invalid_reply", run_id=run_id)
            return None
        return est

    async def _with_eta(self, run_id: str, order: PlannedOrder) -> PlannedOrder:
        """Freeze the ETA into an auto order and derive due from it (D-04/D-08/D-09)."""
        if self._due_mode != "eta":
            return order
        est = await self._estimate(
            run_id, order.product, order.qty, order.priority, None, self._buffer_k
        )
        if est is None:
            return order
        eta_ts, station_eta = self._eta_fields(order.created_ts, est)
        due_ts = self._trunc(
            order.created_ts + timedelta(seconds=float(est["eta_lead_s"]) * self._buffer_k)
        )
        return dataclasses.replace(order, due_ts=due_ts, eta_ts=eta_ts, station_eta=station_eta)

    async def _publish_with_retry(
        self, run_id: str, order: PlannedOrder, *, release_lag_s: float | None = None
    ) -> None:
        payload = {
            "order_id": order.order_id,
            "product": order.product,
            "qty": order.qty,
            "due_ts": order.due_ts,
            "created_ts": order.created_ts,
            # Unconditional: auto (source=None) orders carry a priority too (D-08).
            "priority": order.priority,
        }
        if order.source is not None:
            payload["source"] = order.source
        if order.eta_ts is not None:
            payload["eta_ts"] = order.eta_ts
            payload["station_eta"] = order.station_eta
        fact_done = False
        while True:
            try:
                if not fact_done:
                    fact = OrderLifecycle.make(
                        run_id=run_id,
                        machine_id=MACHINE_ID,
                        state="created",
                        release_lag_s=release_lag_s,
                        **payload,
                    )
                    await asyncio.to_thread(
                        produce_fact, self.producer, TOPIC_ORDER_LIFECYCLE, fact, order.order_id
                    )
                    fact_done = True
                command = OrderSubmitted.make(run_id=run_id, machine_id=MACHINE_ID, **payload)
                await publish_command(await self._get_exchange(), INTAKE_KEY, command)
                return
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - retry the same order, never skip a seq
                _log.exception("generator_publish_failed", order_id=order.order_id, seq=order.seq)
                await self._sleep(self.settings.publish_retry_s)

    # -- manual orders ----------------------------------------------------
    async def _reply(self, message: Any, body: dict[str, Any]) -> None:
        if not getattr(message, "reply_to", None):
            return
        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(body).encode(),
                    content_type="application/json",
                    correlation_id=message.correlation_id,
                ),
                routing_key=message.reply_to,
            )
        except Exception:  # noqa: BLE001 - order already published; never nack for a reply
            _log.exception("generator_manual_reply_failed")

    def _build_manual(
        self, cmd: ManualOrderRequest, est: dict[str, Any] | None = None
    ) -> PlannedOrder | dict[str, Any]:
        """Return the order to publish, or an error reply body. Consumes no seq on error."""
        if self.current_run_id is None or cmd.run_id != self.current_run_id:
            return {"ok": False, "code": "no_run", "detail": "cannot place order: no active run"}
        created_ts = utc_now()
        if cmd.deadline_ts < created_ts:
            return {"ok": False, "code": "expired", "detail": "order request expired"}
        try:
            if cmd.due_ts is not None:
                due_ts = cmd.due_ts.replace(microsecond=cmd.due_ts.microsecond // 1000 * 1000)
                if due_ts < created_ts:
                    raise ValueError("due_ts must not be earlier than now")
            elif est is not None:
                due = created_ts + timedelta(seconds=float(est["eta_lead_s"]) * cmd.buffer_k)
                due_ts = due.replace(microsecond=due.microsecond // 1000 * 1000)
            else:
                total = theoretical_total_s(self._load_recipes(), cmd.product, cmd.qty)  # type: ignore[arg-type]
                lead_s = total * getattr(self.settings, "due_k", 2.0) / self.timescale.scale
                due = created_ts + timedelta(seconds=lead_s)
                due_ts = due.replace(microsecond=due.microsecond // 1000 * 1000)
        except ValueError as exc:  # includes RecipeError / unknown product
            return {"ok": False, "code": "invalid", "detail": str(exc)}
        eta_ts = None
        station_eta = None
        if est is not None:
            eta_ts, station_eta = self._eta_fields(created_ts, est)
        seq = self._alloc_seq()
        return PlannedOrder(
            seq=seq,
            order_id=make_order_id(self._seed, seq),
            offset_s=0.0,
            created_ts=created_ts,
            product=cmd.product,
            qty=cmd.qty,
            due_ts=due_ts,
            source="manual",
            priority=cmd.priority,
            eta_ts=eta_ts,
            station_eta=station_eta,
        )

    async def _handle_manual(self, cmd: ManualOrderRequest, message: Any) -> None:
        key = str(cmd.event_id)
        cached = self._manual_replies.get(key)
        if cached is not None:
            await self._reply(message, cached)
            await message.ack()
            return
        est: dict[str, Any] | None = None
        if (
            self._due_mode == "eta"
            and self.current_run_id is not None
            and cmd.run_id == self.current_run_id
            and cmd.deadline_ts >= utc_now()
        ):
            est = await self._estimate(
                cmd.run_id, cmd.product, cmd.qty, cmd.priority, cmd.due_ts, cmd.buffer_k
            )
        built = self._build_manual(cmd, est)
        if isinstance(built, dict):
            await self._reply(message, built)
            await message.ack()
            return
        run_id = cmd.run_id
        await self._publish_with_retry(run_id, built)
        reply = {
            "ok": True,
            "order_id": built.order_id,
            "run_id": run_id,
            "due_ts": _iso(built.due_ts),
            "priority": built.priority,
            "eta_ts": _iso(built.eta_ts) if built.eta_ts else None,
        }
        self._manual_replies[key] = reply
        while len(self._manual_replies) > _REPLY_CACHE_CAP:
            self._manual_replies.popitem(last=False)
        await self._reply(message, reply)
        await message.ack()

    # -- lifecycle --------------------------------------------------------
    def stop(self) -> None:
        """Request graceful stop; safe from signal handlers and tests."""
        self._stopping.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def run(self, connection: Any) -> None:
        """Verify topology, consume generator.control, stop gracefully."""
        await verify_rabbit(connection)
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap})
        await asyncio.to_thread(verify_kafka, admin)

        channel = await connection.channel()
        self.channel = channel
        queue = await channel.get_queue(Q_GENERATOR_CONTROL, ensure=True)
        tag = await queue.consume(self.handle_control, no_ack=False)
        _log.info("generator_started")
        self._timescale_feed = TimeScaleFeed(
            self.settings.kafka_bootstrap, "generator", self.timescale
        )
        await self._timescale_feed.start()
        watch_task = asyncio.create_task(self._watch_host_sleep())

        await self._stopping.wait()

        _log.info("generator_stopping", run_id=self.current_run_id)
        watch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await watch_task
        await queue.cancel(tag)
        if self._timescale_feed is not None:
            await self._timescale_feed.stop()
        await self._cancel_task()
        await self._idle.wait()
        await channel.close()
        _log.info("generator_stopped", run_id=self.current_run_id)
