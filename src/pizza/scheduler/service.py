"""Scheduler effect shell around the pure SchedulerCore (SCHED-01/02, D-12..D-18).

Every handler path ends in exactly one of ack, nack(requeue=True) after a delay,
or dead_letter_invalid (which acks). rejection without requeue is never used
(see the station service header). All core mutations and their effects run under
one asyncio.Lock so two callers can never interleave.
"""

import asyncio
import contextlib
import dataclasses
import json
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any, NamedTuple

import aio_pika
import structlog
from confluent_kafka.admin import AdminClient

from pizza.common.bootstrap import verify_kafka, verify_rabbit
from pizza.common.constants import LINE_ID
from pizza.common.envelope import (
    SCHEDULER_CONTROL_TYPES,
    EstimateEta,
    EstimateWip,
    InvalidEnvelope,
    JobCommand,
    LotBank,
    LotBanked,
    LotHold,
    LotHoldCancel,
    LotRelease,
    LotReleased,
    LotScrap,
    LotSetPriority,
    LotStocked,
    LotUnstocked,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    OrderSubmitted,
    PriorityChanged,
    RecipeSnapshot,
    RuleChanged,
    RunPause,
    RunPaused,
    RunReset,
    RunResume,
    RunResumed,
    RunStarted,
    SetRecipe,
    SetRule,
    StationHeartbeat,
    StationStatus,
    parse_message,
    utc_now,
)
from pizza.common.kafka_io import produce_fact
from pizza.common.rabbit_io import dead_letter_invalid, publish_command
from pizza.common.recipes import load_all_recipes, validate_recipes
from pizza.common.timescale import TimeScale, TimeScaleFeed
from pizza.common.topology import (
    EX_DISPATCH,
    EX_DLX,
    EX_SCHEDULER_REWORK,
    Q_REWORK_RETURN,
    Q_SCHEDULER_CONTROL,
    Q_WORKORDERS_INTAKE,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    jobs_queue_name,
)
from pizza.scheduler.core import (
    Action,
    CoreConfig,
    Dispatch,
    EmitHold,
    EmitLifecycle,
    EmitLot,
    EmitStationStatus,
    OrderRecord,
    PublishDeadLetter,
    PublishRework,
    SchedulerCore,
    newest_run_id,
)
from pizza.scheduler.estimate import estimate_lot, estimate_new, estimate_wip
from pizza.scheduler.health import stale_stations
from pizza.scheduler.replay import KafkaTail, replay_topics
from pizza.scheduler.rework import DEFAULT_REWORK_LIMITS

_log = structlog.get_logger()

MACHINE_ID = "scheduler"
INTAKE_PREFETCH = 10
PENDING_CAP = 1000


class StartupHandles(NamedTuple):
    intake_queue: Any
    intake_tag: Any
    control_queue: Any
    control_tag: Any
    return_queue: Any
    return_tag: Any


class Scheduler:
    """Single logical state loop: intake + control + Kafka tail feed one core."""

    def __init__(
        self,
        producer: Any,
        settings: Any,
        channel: Any = None,
        dispatch_exchange: Any = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.producer = producer
        self.settings = settings
        self.channel = channel
        self._clock = clock
        self._exchanges: dict[str, Any] = {}
        if dispatch_exchange is not None:
            self._exchanges[EX_DISPATCH] = dispatch_exchange
        # Load recipes (empty dict if recipes_dir not set)
        recipes = load_all_recipes(getattr(settings, "recipes_dir", None))
        validate_recipes(recipes)
        self._recipes = recipes
        # quick-et6: bank_after_s is a simulated-seconds policy compared against wall
        # time, so it is divided by the live scale. Heartbeat staleness, sweep and
        # retry intervals and the rework hold TTL are infrastructure wall time and
        # are deliberately not scaled.
        self.timescale = TimeScale()
        self.timescale.on_change(self._on_scale_change)
        self._timescale_feed: TimeScaleFeed | None = None
        self.core = SchedulerCore(config=self._core_config())
        self._last_seen: dict[str, float] = {}
        self._pending: list[Action] = []
        self._sweeper: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._inflight = 0
        self._defer_counts: dict[Any, int] = {}
        self._retry_task: asyncio.Task[None] | None = None
        self._tail: KafkaTail | None = None

    def _core_config(self) -> CoreConfig:
        st = self.settings
        return CoreConfig(
            rework_limits=getattr(st, "rework_limits", DEFAULT_REWORK_LIMITS),
            queue_limit=getattr(st, "station_queue_limit", None),
            bank_after_s=getattr(st, "bank_after_s", 60.0) / self.timescale.scale,
            bank_wait_threshold=getattr(st, "bank_wait_threshold", None),
            recipes=self._recipes,
            rework_hold_grace_s=getattr(st, "rework_hold_grace_s", CoreConfig.rework_hold_grace_s),
        )

    def _on_scale_change(self, _scale: float) -> None:
        base = getattr(self.settings, "bank_after_s", 60.0)
        self.core.config = dataclasses.replace(
            self.core.config, bank_after_s=base / self.timescale.scale
        )

    def _seed_last_seen(self) -> None:
        now = self._clock()
        self._last_seen = {s: now for s in self.core.route}

    def _defer(self, action: Action) -> None:
        self._pending.append(action)
        if len(self._pending) > PENDING_CAP:
            _log.warning("scheduler_pending_overflow", dropped=type(self._pending[0]).__name__)
            del self._pending[0]

    # -- bookkeeping ------------------------------------------------------
    def _enter(self) -> None:
        self._inflight += 1
        self._idle.clear()

    def _leave(self) -> None:
        self._inflight -= 1
        if self._inflight <= 0:
            self._inflight = 0
            self._idle.set()

    async def _nack_after_delay(self, message: Any) -> None:
        await asyncio.sleep(self.settings.nack_delay_s)
        await message.nack(requeue=True)

    # -- effects ----------------------------------------------------------
    async def _emit_lifecycle(self, action: EmitLifecycle) -> None:
        rec = self.core.orders[action.order_id].record
        extra: dict[str, Any] = {}
        if action.state == "failed":
            extra = {"step": action.step, "visit": action.visit}
        fact = OrderLifecycle.make(
            run_id=self.core.current_run_id or "",
            machine_id=MACHINE_ID,
            ts=action.ts,
            state=action.state,
            order_id=rec.order_id,
            product=rec.product,
            qty=rec.qty,
            due_ts=rec.due_ts,
            created_ts=rec.created_ts,
            fail_step=rec.fail_step,
            fail_times=rec.fail_times,
            poison=rec.poison,
            priority=rec.priority,
            eta_ts=rec.eta_ts,
            station_eta=dict(rec.station_eta) if rec.station_eta else None,
            **extra,
        )
        await asyncio.to_thread(
            produce_fact, self.producer, TOPIC_ORDER_LIFECYCLE, fact, rec.order_id
        )

    async def _emit_run_fact(
        self, fact: RunPaused | RunResumed | RuleChanged | RecipeSnapshot
    ) -> None:
        await asyncio.to_thread(produce_fact, self.producer, TOPIC_STATION_EVENTS, fact, MACHINE_ID)

    async def _emit_order_fact(self, fact: PriorityChanged, order_id: str) -> None:
        await asyncio.to_thread(produce_fact, self.producer, TOPIC_ORDER_LIFECYCLE, fact, order_id)

    def _job_command(
        self, *, order_id: str, step: str, visit: int, qty: int, product: str, event_id: Any
    ) -> JobCommand:
        fail_times, poison = self.core.fail_flags(order_id, step)
        return JobCommand(
            event_id=event_id,
            run_id=self.core.current_run_id or "",
            line_id=LINE_ID,
            machine_id=step,
            ts=utc_now(),
            schema_version=1,
            order_id=order_id,
            step=step,
            visit=visit,
            qty=qty,
            recipe=product,
            product=product,
            fail_times=fail_times,
            poison=poison,
        )

    async def _emit_dispatched(self, action: Dispatch) -> None:
        fact = OpDispatched.make(
            run_id=self.core.current_run_id or "",
            machine_id=action.step,
            order_id=action.order_id,
            step=action.step,
            visit=action.visit,
            qty=action.qty,
            product=action.product,
            command_event_id=action.command_event_id,
            ready_ts=action.ready_ts,
        )
        await asyncio.to_thread(
            produce_fact, self.producer, TOPIC_STATION_EVENTS, fact, action.step
        )

    async def _publish_dispatch(self, action: Dispatch) -> None:
        command = self._job_command(
            order_id=action.order_id,
            step=action.step,
            visit=action.visit,
            qty=action.qty,
            product=action.product,
            event_id=action.command_event_id,
        )
        # (1) publisher-confirmed command, then (2) the op.dispatched fact (FR-SCHED-05).
        await publish_command(await self._get_exchange(), f"station.{action.step}", command)
        await self._emit_dispatched(action)

    async def _publish_rework(self, action: PublishRework) -> None:
        command = self._job_command(
            order_id=action.order_id,
            step=action.step,
            visit=action.visit,
            qty=action.qty,
            product=action.product,
            event_id=action.command_event_id,
        )
        # D-02/D-08: park on the hold queue; op.dispatched is emitted only at real dispatch.
        exchange = await self._get_exchange(EX_SCHEDULER_REWORK)
        await publish_command(exchange, f"hold.{action.step}", command)

    async def _publish_dead_letter(self, action: PublishDeadLetter) -> None:
        command = self._job_command(
            order_id=action.order_id,
            step=action.step,
            visit=action.visit,
            qty=action.qty,
            product=action.product,
            event_id=action.command_event_id,
        )
        headers = {
            "x-failed-reason": action.reason,
            "x-failed-step": action.step,
            "x-failed-visit": action.visit,
            "x-source-queue": jobs_queue_name(action.step),
        }
        await publish_command(await self._get_exchange(EX_DLX), "", command, headers=headers)

    async def _emit_lot(self, action: EmitLot) -> None:
        base: dict[str, Any] = {
            "run_id": self.core.current_run_id or "",
            "machine_id": MACHINE_ID,
            "ts": action.ts,
            "order_id": action.order_id,
            "station": action.station,
        }
        fact: LotStocked | LotUnstocked | LotBanked | LotReleased
        if action.fact == "stocked":
            fact = LotStocked.make(kind=action.kind, **base)
        elif action.fact == "unstocked":
            fact = LotUnstocked.make(**base)
        elif action.fact == "banked":
            fact = LotBanked.make(reason=action.reason, source=action.source, **base)
        else:
            fact = LotReleased.make(by=action.by, **base)
        await asyncio.to_thread(
            produce_fact, self.producer, TOPIC_ORDER_LIFECYCLE, fact, action.order_id
        )

    async def _emit_hold(self, action: EmitHold) -> None:
        fact = LotHold.make(
            run_id=self.core.current_run_id or "",
            machine_id=MACHINE_ID,
            ts=action.ts,
            order_id=action.order_id,
            station=action.station,
            state=action.state,
            kind=action.kind,
            reason=action.reason,
        )
        await asyncio.to_thread(
            produce_fact, self.producer, TOPIC_ORDER_LIFECYCLE, fact, action.order_id
        )

    async def _emit_station_status(self, action: EmitStationStatus) -> None:
        fact = StationStatus.make(
            run_id=self.core.current_run_id or "",
            machine_id=action.station,
            ts=action.ts,
            status=action.status,
        )
        await asyncio.to_thread(
            produce_fact, self.producer, TOPIC_STATION_EVENTS, fact, action.station
        )

    async def _get_exchange(self, name: str = EX_DISPATCH) -> Any:
        ex = self._exchanges.get(name)
        if ex is None:
            ex = await self.channel.get_exchange(name)
            self._exchanges[name] = ex
        return ex

    async def _execute(self, actions: list[Action]) -> None:
        """Run actions in order (lock held by the caller). Never raises."""
        failed_dispatch = False
        failed_pending = False
        for action in actions:
            if isinstance(action, EmitLifecycle):
                try:
                    await self._emit_lifecycle(action)
                except Exception:  # noqa: BLE001 - reconcile on next start repairs it
                    _log.exception("scheduler_lifecycle_emit_failed", order_id=action.order_id)
            elif isinstance(action, Dispatch):
                if failed_dispatch:
                    self.core.release_dispatch(action.step)
                    continue
                try:
                    await self._publish_dispatch(action)
                except Exception:  # noqa: BLE001 - restore credit, retry later
                    _log.exception(
                        "scheduler_dispatch_failed", order_id=action.order_id, station=action.step
                    )
                    self.core.release_dispatch(action.step)
                    failed_dispatch = True
            elif isinstance(action, PublishRework | PublishDeadLetter | EmitLot | EmitHold):
                try:
                    if isinstance(action, PublishRework):
                        await self._publish_rework(action)
                    elif isinstance(action, PublishDeadLetter):
                        await self._publish_dead_letter(action)
                    elif isinstance(action, EmitHold):
                        await self._emit_hold(action)
                    else:
                        await self._emit_lot(action)
                except Exception:  # noqa: BLE001 - keep and retry
                    _log.exception(
                        "scheduler_action_failed",
                        action=type(action).__name__,
                        order_id=action.order_id,
                    )
                    self._defer(action)
                    failed_pending = True
            elif isinstance(action, EmitStationStatus):
                try:
                    await self._emit_station_status(action)
                except Exception:  # noqa: BLE001 - best effort
                    _log.exception("scheduler_station_status_failed", station=action.station)
        if failed_dispatch or failed_pending:
            self._schedule_retry()

    def _schedule_retry(self) -> None:
        if self._retry_task is not None and not self._retry_task.done():
            return
        self._retry_task = asyncio.create_task(self._retry_dispatch())

    async def _retry_dispatch(self) -> None:
        await asyncio.sleep(self.settings.dispatch_retry_s)
        self._retry_task = None  # allow _execute to re-arm
        async with self._lock:
            pending, self._pending = self._pending, []
            await self._execute(pending)
            await self._execute(list(self.core.settle(utc_now())))

    async def drain_retries(self) -> None:
        """Wait for a pending dispatch retry (used by tests and shutdown)."""
        task = self._retry_task
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # -- intake -----------------------------------------------------------
    async def handle_intake(self, message: Any) -> None:
        """workorders.intake callback; never lets an exception escape."""
        self._enter()
        try:
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(
                    self.channel, message, f"invalid envelope: {exc}", Q_WORKORDERS_INTAKE
                )
                return
            if not isinstance(parsed, OrderSubmitted):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"unexpected message type {parsed.type}",
                    Q_WORKORDERS_INTAKE,
                )
                return
            if await self._take_order(message, parsed):
                await self._nack_after_delay(message)
        except Exception:  # noqa: BLE001 - last resort so the prefetch window never stalls
            _log.exception("scheduler_handle_intake_failed")
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("scheduler_nack_failed")
        finally:
            self._leave()

    async def _take_order(self, message: Any, order: OrderSubmitted) -> bool:
        """Apply one order under the lock. Returns True when the caller must nack."""
        async with self._lock:
            if order.run_id != self.core.current_run_id:
                count = self._defer_counts.get(order.event_id, 0)
                if count < self.settings.intake_defer_max:
                    self._defer_counts[order.event_id] = count + 1
                    return True
                self._defer_counts.pop(order.event_id, None)
                _log.warning("scheduler_intake_dropped_wrong_run", order_id=order.order_id)
                await message.ack()
                return False
            if order.order_id in self.core.orders:
                await message.ack()
                return False
            record = OrderRecord(
                order.order_id,
                order.product,
                order.qty,
                order.due_ts,
                order.created_ts,
                fail_step=order.fail_step,
                fail_times=order.fail_times,
                poison=order.poison,
                priority=order.priority,
                eta_ts=order.eta_ts,
                station_eta=order.station_eta,
            )
            actions = self.core.on_order(record, utc_now())
            released = actions[0]
            rest = actions[1:]
            try:
                assert isinstance(released, EmitLifecycle)
                await self._emit_lifecycle(released)
            except Exception:  # noqa: BLE001 - roll the order back, redeliver later
                _log.exception("scheduler_released_emit_failed", order_id=order.order_id)
                self.core.rollback_order(order.order_id)
                others = [a for a in rest if a.order_id != order.order_id]
                for a in rest:
                    if isinstance(a, Dispatch) and a.order_id == order.order_id:
                        self.core.release_dispatch(a.step)  # no-op after rollback
                await self._execute(others)
                return True
            await message.ack()  # released fact is flushed (D-16)
            await self._execute(list(rest))
            return False

    # -- rework return ----------------------------------------------------
    async def handle_rework_return(self, message: Any) -> None:
        """scheduler.rework.return callback: a held rework lot is ready again (D-04)."""
        self._enter()
        try:
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(
                    self.channel, message, f"invalid envelope: {exc}", Q_REWORK_RETURN
                )
                return
            if not isinstance(parsed, JobCommand):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"unexpected message type {parsed.type}",
                    Q_REWORK_RETURN,
                )
                return
            async with self._lock:
                actions = self.core.on_rework_ready(
                    parsed.order_id, parsed.step, parsed.visit, parsed.run_id, utc_now()
                )
                await self._execute(list(actions))
            await message.ack()
        except Exception:  # noqa: BLE001
            _log.exception("scheduler_rework_return_failed")
            with contextlib.suppress(Exception):
                await self._nack_after_delay(message)
        finally:
            self._leave()

    # -- control ----------------------------------------------------------
    async def handle_control(self, message: Any) -> None:
        """scheduler.control callback.

        Accepts run.started (select run), run.pause / run.resume (emit run.paused /
        run.resumed facts, idempotent), run.reset (fresh core), scheduler.set_rule
        (stored only, D-10), generator.set_rate (acked, ignored), lot.bank and
        lot.release (D-20.1, emit lot.banked / lot.released). Anything else is
        dead-lettered.
        """
        self._enter()
        try:
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(
                    self.channel, message, f"invalid envelope: {exc}", Q_SCHEDULER_CONTROL
                )
                return
            if not isinstance(parsed, SCHEDULER_CONTROL_TYPES):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"unexpected message type {parsed.type}",
                    Q_SCHEDULER_CONTROL,
                )
                return
            if isinstance(parsed, EstimateEta | EstimateWip):
                await self._answer_estimate(parsed, message)
                return
            async with self._lock:
                await self._apply_control(parsed)
            await message.ack()
        except Exception:  # noqa: BLE001
            _log.exception("scheduler_handle_control_failed")
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("scheduler_nack_failed")
        finally:
            self._leave()

    @staticmethod
    def _iso_z(value: Any) -> str:
        return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    async def _answer_estimate(self, parsed: EstimateEta | EstimateWip, message: Any) -> None:
        """Serve one estimate RPC; always acks (volatile estimate, never a Kafka fact)."""
        body: dict[str, Any]
        try:
            async with self._lock:
                now = utc_now()
                if parsed.run_id != self.core.current_run_id:
                    body = {"ok": False, "code": "no_run", "detail": "no matching active run"}
                elif parsed.deadline_ts < now:
                    body = {"ok": False, "code": "expired", "detail": "request deadline passed"}
                else:
                    try:
                        scale = self.timescale.scale
                        if isinstance(parsed, EstimateEta):
                            est = estimate_new(
                                self.core,
                                product=parsed.product,
                                qty=parsed.qty,
                                priority=parsed.priority,
                                due_ts=parsed.due_ts,
                                buffer_k=parsed.buffer_k,
                                now=now,
                                scale=scale,
                            )
                            body = {
                                "ok": True,
                                "eta_lead_s": round(est.lead_wall_s, 3),
                                "station_lead_s": {
                                    k: round(v, 3) for k, v in est.station_lead_wall_s.items()
                                },
                                "degraded": est.degraded,
                            }
                        elif parsed.order_id is not None:
                            # Per-lot live stations (volatile; never a Kafka fact).
                            lot_est = estimate_lot(self.core, parsed.order_id, now=now, scale=scale)
                            if lot_est is None:
                                body = {
                                    "ok": False,
                                    "code": "not_found",
                                    "detail": "lot not in flight",
                                }
                            else:
                                body = {
                                    "ok": True,
                                    "stations": {
                                        st: {
                                            "in": None if a is None else self._iso_z(a),
                                            "out": self._iso_z(b),
                                        }
                                        for st, (a, b) in lot_est.items()
                                    },
                                }
                        else:
                            etas = estimate_wip(self.core, now=now, scale=scale)
                            body = {
                                "ok": True,
                                "eta": {oid: self._iso_z(ts) for oid, ts in etas.items()},
                            }
                    except ValueError as exc:
                        body = {"ok": False, "code": "invalid", "detail": str(exc)}
                    except Exception:  # noqa: BLE001
                        _log.exception("scheduler_estimate_failed")
                        body = {"ok": False, "code": "invalid", "detail": "estimate failed"}
            if getattr(message, "reply_to", None):
                try:
                    await self.channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(body).encode(),
                            content_type="application/json",
                            correlation_id=message.correlation_id,
                        ),
                        routing_key=message.reply_to,
                    )
                except Exception:  # noqa: BLE001 - never nack for a reply problem
                    _log.exception("scheduler_estimate_reply_failed")
        except Exception:  # noqa: BLE001
            _log.exception("scheduler_estimate_failed")
        await message.ack()

    async def _apply_control(self, parsed: Any) -> None:
        """Apply one validated control command (lock held). Raises on produce failure."""
        if isinstance(parsed, RunStarted):
            if parsed.run_id != self.core.current_run_id:
                # Emit RuleChanged fact with the rule from params or current rule
                rule = (parsed.params.rule if parsed.params else None) or self.core.rule
                await self._emit_run_fact(
                    RuleChanged.make(run_id=parsed.run_id, machine_id=MACHINE_ID, rule=rule)
                )
                # Journal the effective recipe table before run state changes (D-09).
                # Per-station dict copy: self._recipes is shared with SchedulerCore.
                await self._emit_run_fact(
                    RecipeSnapshot.make(
                        run_id=parsed.run_id,
                        machine_id=MACHINE_ID,
                        recipes={station: dict(table) for station, table in self._recipes.items()},
                    )
                )
                _log.info(
                    "scheduler_recipe_snapshot",
                    run_id=parsed.run_id,
                    stations=len(self._recipes),
                )
                self.core.start_run(parsed.run_id)
                self._defer_counts.clear()
                self._pending.clear()
                self._seed_last_seen()
                _log.info("scheduler_run_started", run_id=parsed.run_id)
        elif isinstance(parsed, RunPause):
            if not self.core.paused:
                # Fact first, state second: a failed produce leaves no half-state.
                # Host-sleep auto pause back-dates the fact to when the sleep began so
                # the slept time counts as paused in KPI/dashboard (quick-261022-wsj).
                paused_ts = utc_now()
                if parsed.slept_s is not None and parsed.slept_s > 0:
                    paused_ts -= timedelta(seconds=parsed.slept_s)
                await self._emit_run_fact(
                    RunPaused.make(run_id=parsed.run_id, machine_id=MACHINE_ID, ts=paused_ts)
                )
                self.core.paused = True
                _log.info("scheduler_paused", run_id=parsed.run_id)
        elif isinstance(parsed, RunResume):
            if self.core.paused:
                await self._emit_run_fact(
                    RunResumed.make(run_id=parsed.run_id, machine_id=MACHINE_ID)
                )
                self.core.paused = False
                _log.info("scheduler_resumed", run_id=parsed.run_id)
                await self._execute(list(self.core.settle(utc_now())))
        elif isinstance(parsed, RunReset):
            if self._retry_task is not None and not self._retry_task.done():
                self._retry_task.cancel()
            self._retry_task = None
            rule = self.core.rule
            self.core = SchedulerCore(config=self._core_config())
            self.core.rule = rule
            self._defer_counts.clear()
            self._pending.clear()
            self._seed_last_seen()
            _log.info("scheduler_reset")
        elif isinstance(parsed, SetRule):
            # Emit RuleChanged fact BEFORE mutating state (facts first, state second)
            await self._emit_run_fact(
                RuleChanged.make(run_id=parsed.run_id, machine_id=MACHINE_ID, rule=parsed.rule)
            )
            self.core.set_rule(parsed.rule)
            # Settle after rule change so stocker corrections are emitted
            await self._execute(list(self.core.settle(utc_now())))
            _log.info("scheduler_rule_set", rule=parsed.rule)
        elif isinstance(parsed, SetRecipe):
            # Live tuning only: mutate the shared table so SPT ordering sees it.
            table = self._recipes.get(parsed.station)
            if table is None:
                _log.warning("scheduler_recipe_unknown_station", station=parsed.station)
            else:
                table[parsed.product] = parsed.unit_time_s
                _log.info(
                    "scheduler_recipe_set",
                    station=parsed.station,
                    product=parsed.product,
                    unit_time_s=parsed.unit_time_s,
                )
        elif isinstance(parsed, LotSetPriority):
            if parsed.run_id != self.core.current_run_id:
                return
            state = self.core.orders.get(parsed.order_id)
            if state is None or self.core.is_terminal(parsed.order_id):
                return
            previous = state.record.priority
            if previous == parsed.priority:
                return  # D-12: no-op suppression, no fact
            # Fact first, state second (D-13): a failed produce leaves state untouched.
            await self._emit_order_fact(
                PriorityChanged.make(
                    run_id=parsed.run_id,
                    machine_id=MACHINE_ID,
                    order_id=parsed.order_id,
                    previous_priority=previous,
                    new_priority=parsed.priority,
                    source="manual",
                ),
                parsed.order_id,
            )
            self.core.set_priority(parsed.order_id, parsed.priority)
            await self._execute(list(self.core.settle(utc_now())))
            _log.info(
                "scheduler_priority_set",
                order_id=parsed.order_id,
                previous=previous,
                priority=parsed.priority,
            )
        elif isinstance(parsed, LotBank | LotRelease | LotScrap | LotHoldCancel):
            if parsed.run_id != self.core.current_run_id:
                return
            now = utc_now()
            if isinstance(parsed, LotBank):
                actions = self.core.bank(parsed.order_id, parsed.reason, now)
            elif isinstance(parsed, LotScrap):
                actions = self.core.scrap(parsed.order_id, parsed.reason, now)
                _log.info("scheduler_lot_scrap", order_id=parsed.order_id, reason=parsed.reason)
            elif isinstance(parsed, LotHoldCancel):
                actions = self.core.cancel_hold(parsed.order_id, now)
            else:
                actions = self.core.release(parsed.order_id, now)
            for a in actions:
                if isinstance(a, EmitHold) and a.state == "requested":
                    _log.info(
                        "scheduler_lot_hold", order_id=a.order_id, kind=a.kind, station=a.station
                    )
            await self._execute(list(actions))
        # SetArrivalRate: acked and ignored here (the generator consumes it).

    # -- Kafka tail -------------------------------------------------------
    async def on_station_event(self, msg: Any) -> None:
        """Async callback for KafkaTail: op facts, station status and heartbeats."""
        if not isinstance(
            msg, OpStarted | OpCompleted | OpFailed | StationStatus | StationHeartbeat
        ):
            return
        async with self._lock:
            if isinstance(msg, StationStatus | StationHeartbeat):
                station = msg.machine_id
                if station not in self.core.route:
                    return
                actions = list(self.core.on_station_status(msg))
                # The scheduler echoes "offline" on a station's behalf: not liveness.
                if isinstance(msg, StationHeartbeat) or msg.status != "offline":
                    self._last_seen[station] = self._clock()
                    if self.core.health[station].offline:
                        actions.extend(self.core.set_offline(station, False, utc_now()))
            elif isinstance(msg, OpStarted):
                actions = self.core.on_op_started(msg)
            elif isinstance(msg, OpCompleted):
                actions = self.core.on_op_completed(msg)
            else:
                actions = self.core.on_op_failed(msg, utc_now())
            await self._execute(list(actions))

    # -- sweeper ------------------------------------------------------------
    async def sweep_once(self) -> None:
        """Apply heartbeat staleness and time-driven BANK moves."""
        interval = getattr(self.settings, "heartbeat_interval_s", 5.0)
        factor = getattr(self.settings, "offline_factor", 3)
        async with self._lock:
            now = utc_now()
            stale = stale_stations(self._last_seen, self._clock(), interval, factor)
            actions: list[Action] = []
            for station in self.core.route:
                if self.core.health[station].offline != (station in stale):
                    actions.extend(self.core.set_offline(station, station in stale, now))
            actions.extend(self.core.sweep(now))
            await self._execute(actions)

    async def _sweeper_loop(self) -> None:
        interval = getattr(self.settings, "sweep_interval_s", 1.0)
        while True:
            await asyncio.sleep(interval)
            try:
                await self.sweep_once()
            except Exception:  # noqa: BLE001 - keep sweeping
                _log.exception("scheduler_sweep_failed")

    # -- startup / lifecycle ---------------------------------------------
    async def recover(self, facts: list[Any]) -> None:
        """Rebuild from replayed facts, reconcile, dispatch free-credit stations (D-14)."""
        if not self.settings.resume_on_start:
            return
        run_id = newest_run_id(facts)
        if run_id is None:
            return
        async with self._lock:
            self.core, reconcile = SchedulerCore.rebuild(run_id, facts, config=self._core_config())
            _log.info("scheduler_rebuilt", run_id=run_id, orders=len(self.core.orders))
            self._seed_last_seen()
            await self._execute(list(reconcile))
            await self._execute(list(self.core.settle(utc_now())))

    def stop(self) -> None:
        """Request graceful stop; safe from signal handlers and tests."""
        self._stopping.set()

    async def startup(self, connection: Any) -> StartupHandles:
        """Verify, open channels, replay + rebuild, then start consuming."""
        await verify_rabbit(connection)
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap})
        await asyncio.to_thread(verify_kafka, admin)
        self._timescale_feed = TimeScaleFeed(
            self.settings.kafka_bootstrap, "scheduler", self.timescale
        )
        await self._timescale_feed.start()

        intake_channel = await connection.channel()
        await intake_channel.set_qos(prefetch_count=INTAKE_PREFETCH)
        control_channel = await connection.channel()
        self.channel = intake_channel
        # Fetch the exchange now: recovery may dispatch before consumption starts.
        self._exchanges[EX_DISPATCH] = await intake_channel.get_exchange(EX_DISPATCH)
        self._seed_last_seen()

        facts, high = await asyncio.to_thread(
            replay_topics,
            self.settings.kafka_bootstrap,
            [TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS],
            self.settings.replay_timeout_s,
            self.settings.kafka_group_id,
        )
        await self.recover(facts)
        self._sweeper = asyncio.create_task(self._sweeper_loop())

        intake_queue = await intake_channel.get_queue(Q_WORKORDERS_INTAKE, ensure=True)
        control_queue = await control_channel.get_queue(Q_SCHEDULER_CONTROL, ensure=True)
        control_tag = await control_queue.consume(self.handle_control, no_ack=False)
        intake_tag = await intake_queue.consume(self.handle_intake, no_ack=False)
        # Consume returns only after recover() so the rebuilt cooling map exists (D-02).
        return_queue = await control_channel.get_queue(Q_REWORK_RETURN, ensure=True)
        return_tag = await return_queue.consume(self.handle_rework_return, no_ack=False)
        station_high = {tp: off for tp, off in high.items() if tp[0] == TOPIC_STATION_EVENTS}
        self._tail = KafkaTail(
            self.settings.kafka_bootstrap,
            self.settings.kafka_group_id,
            station_high,
            TOPIC_STATION_EVENTS,
            self.on_station_event,
            asyncio.get_running_loop(),
        )
        self._tail.start()
        return StartupHandles(
            intake_queue, intake_tag, control_queue, control_tag, return_queue, return_tag
        )

    async def run(self, connection: Any) -> None:
        """Run until stop(); drain in-flight handlers before closing."""
        h = await self.startup(connection)
        _log.info("scheduler_started")
        await self._stopping.wait()
        _log.info("scheduler_stopping", run_id=self.core.current_run_id)
        await h.intake_queue.cancel(h.intake_tag)
        await h.control_queue.cancel(h.control_tag)
        await h.return_queue.cancel(h.return_tag)
        if self._tail is not None:
            await asyncio.to_thread(self._tail.stop)
        if self._timescale_feed is not None:
            await self._timescale_feed.stop()
        await self._idle.wait()
        if self._sweeper is not None:
            self._sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweeper
            self._sweeper = None
        if self._retry_task is not None and not self._retry_task.done():
            self._retry_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._retry_task
        await self.channel.close()
        _log.info("scheduler_stopped")
