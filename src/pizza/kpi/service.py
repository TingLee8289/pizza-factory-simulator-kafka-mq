"""KPI effect shell around the pure fold/advance (KPI-01, KPI-03, KPI-05, D-11, D-13).

Every control handler path ends in exactly one of ack, nack(requeue=True) after a
delay, or dead_letter_invalid (which acks). State mutations run under one
asyncio.Lock; snapshot publication happens off-loop and outside the lock.
RunStarted params (weights, run_started_ts) are persisted via an immediate
kpi.snapshots publish BEFORE the control message is acked, and restored on
recovery from the replayed snapshots (CR-02).
No Kafka offsets are committed: restart recovery is a full replay.
"""

import asyncio
import contextlib
import time
from collections.abc import Mapping
from typing import Any

import structlog
from confluent_kafka.admin import AdminClient

from pizza.common.bootstrap import verify_kafka, verify_rabbit
from pizza.common.envelope import (
    KPI_CONTROL_TYPES,
    ComparisonResult,
    InvalidEnvelope,
    KpiSnapshot,
    RoundClose,
    RunParams,
    RunReset,
    RunStarted,
    SetRecipe,
    TimeScaleChanged,
    comparison_key,
    parse_message,
    utc_now,
)
from pizza.common.kafka_io import produce_fact
from pizza.common.rabbit_io import dead_letter_invalid
from pizza.common.topology import (
    Q_KPI_CONTROL,
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    TOPIC_TIMESCALE,
)
from pizza.kpi.compare import summarize_round
from pizza.kpi.fold import KpiState, apply_run_params, fold, new_state, newest_run_id
from pizza.kpi.metrics import KpiConfig, advance, effective_as_of
from pizza.scheduler.replay import KafkaTail, replay_topics

_log = structlog.get_logger()

MACHINE_ID = "kpi-service"


class Kpi:
    """Fold station/lifecycle facts into KPI snapshots published every tick.

    Every control handler ends in ack, nack-after-delay or dead_letter_invalid.
    """

    def __init__(
        self,
        producer: Any,
        settings: Any,
        recipes: Mapping[str, Mapping[str, float]],
        channel: Any = None,
    ) -> None:
        self.producer = producer
        self.settings = settings
        # Copy the tables: live recipe commands mutate them in place.
        self.recipes = {station: dict(table) for station, table in recipes.items()}
        self.channel = channel
        self.cfg = KpiConfig(
            warmup_orders=settings.warmup_orders,
            little_tolerance=settings.little_tolerance,
            bottleneck_margin=settings.bottleneck_margin,
            bottleneck_confirm_n=settings.bottleneck_confirm_n,
            bottleneck_min_util=settings.bottleneck_min_util,
        )
        self.state: KpiState = new_state(None)
        self.run_params: RunParams | None = None
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._inflight = 0
        self._tail: KafkaTail | None = None
        self._ticker: asyncio.Task[None] | None = None

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

    async def _handle_round_close(self, parsed: RoundClose, message: Any) -> None:
        """Handle RoundClose command: publish ComparisonResult or ack/nack."""
        try:
            # Validate that this is for the current run with a comparison_id
            if (
                parsed.run_id != self.state.run_id
                or not self.run_params
                or not self.run_params.comparison_id
            ):
                _log.warning(
                    "kpi_round_close_ignored",
                    run_id=parsed.run_id,
                    has_params=self.run_params is not None,
                )
                await message.ack()
                return

            # For partial=False, wait up to round_close_wait_s for orders to complete
            max_orders = self.run_params.max_orders or 0
            deadline = time.monotonic() + self.settings.round_close_wait_s
            while not parsed.partial:
                async with self._lock:
                    completed_or_failed = len([
                        o for o in self.state.orders.values()
                        if o.completed_ts is not None or o.failed_ts is not None
                    ])
                if completed_or_failed >= max_orders:
                    break
                if time.monotonic() > deadline:
                    break
                await asyncio.sleep(0.25)

            # Compute comparison result
            async with self._lock:
                as_of = effective_as_of(self.state, utc_now())
                summary = summarize_round(
                    self.state, as_of, self.recipes, self.cfg, partial=parsed.partial
                )

            # Build ComparisonResult using .make()
            result = ComparisonResult.make(
                run_id=self.state.run_id,
                machine_id=MACHINE_ID,
                ts=utc_now(),
                comparison_id=self.run_params.comparison_id,
                round=self.run_params.round or 0,
                rule=self.run_params.rule or "edd",
                seed=self.run_params.seed,
                n_orders=summary.n_orders,
                n_completed=summary.n_completed,
                n_failed=summary.n_failed,
                late_count=summary.late_count,
                mean_cycle_time_s=summary.mean_cycle_time_s,
                mean_tardiness_s=summary.mean_tardiness_s,
                max_tardiness_s=summary.max_tardiness_s,
                pct_late=summary.pct_late,
                makespan_s=summary.makespan_s,
                partial=parsed.partial,
                k=self.run_params.k,
                arrival_rate_per_min=self.run_params.arrival_rate * 60,
                random_breakdowns=self.run_params.random_breakdowns,
                max_orders=self.run_params.max_orders,
            )

            # Publish with comparison_key, only ack after success
            key = comparison_key(self.run_params.comparison_id, self.run_params.round or 0)
            await asyncio.to_thread(produce_fact, self.producer, TOPIC_KPI_SNAPSHOTS, result, key)
            await message.ack()
            _log.info(
                "kpi_round_close_published",
                comparison_id=self.run_params.comparison_id,
                round=self.run_params.round,
            )
        except Exception:  # noqa: BLE001
            _log.exception("kpi_round_close_failed")
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("kpi_nack_failed")

    # -- recovery / events ------------------------------------------------
    async def recover(self, facts: list[Any], snapshots: list[Any] | None = None) -> None:
        """Rebuild state from replayed facts of the newest run only (D-13).

        ``snapshots`` (replayed kpi.snapshots) restore the RunStarted params and
        run_params of that run from its latest snapshot carrying them (CR-02).
        """
        run_id = newest_run_id(facts)
        if run_id is None:
            async with self._lock:  # no run to rebuild, but keep the speed history
                for fact in facts:
                    if isinstance(fact, TimeScaleChanged):
                        self.state = fold(self.state, fact)
            return
        async with self._lock:
            state = new_state(run_id, None, "observed")
            for fact in facts:
                if getattr(fact, "run_id", None) == run_id or isinstance(fact, TimeScaleChanged):
                    state = fold(state, fact)
            carriers = [
                sn
                for sn in (snapshots or [])
                if isinstance(sn, KpiSnapshot) and sn.run_id == run_id and sn.run_weights
            ]
            if carriers:
                best = max(carriers, key=lambda sn: sn.ts)
                # Invalid weights are ignored (falls back to observed).
                apply_run_params(state, best.run_weights, best.run_started_ts)
            self.state = state
            # Restore run_params from the newest snapshot carrier (Pitfall 3)
            params_carriers = [
                sn
                for sn in (snapshots or [])
                if isinstance(sn, KpiSnapshot) and sn.run_id == run_id and sn.run_params
            ]
            if params_carriers:
                best_params = max(params_carriers, key=lambda sn: sn.ts)
                self.run_params = best_params.run_params
            else:
                self.run_params = None
            _log.info(
                "kpi_rebuilt",
                run_id=run_id,
                orders=len(state.orders),
                weights_source=state.weights_source,
            )

    async def on_event(self, msg: Any) -> None:
        """KafkaTail callback; never raises into the Kafka thread."""
        try:
            async with self._lock:
                # Run adoption/rejection is fold()'s responsibility (CR-01).
                self.state = fold(self.state, msg)
        except Exception:  # noqa: BLE001
            _log.exception("kpi_on_event_failed")

    # -- control ----------------------------------------------------------
    async def handle_control(self, message: Any) -> None:
        """kpi.control callback: run.started adopts a run, run.reset clears state,
        round.close publishes comparison."""
        self._enter()
        try:
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(
                    self.channel, message, f"invalid envelope: {exc}", Q_KPI_CONTROL
                )
                return
            if not isinstance(parsed, KPI_CONTROL_TYPES):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"unexpected message type {parsed.type}",
                    Q_KPI_CONTROL,
                )
                return
            if isinstance(parsed, RunReset):
                async with self._lock:
                    self.state = new_state(None, scale_marks=self.state.scale_marks)
                    self.run_params = None
                _log.info("kpi_run_reset", run_id=parsed.run_id)
                await message.ack()
                return
            if isinstance(parsed, RoundClose):
                await self._handle_round_close(parsed, message)
                return
            if isinstance(parsed, SetRecipe):
                # Theory values use the live table; the next snapshot picks it up.
                async with self._lock:
                    table = self.recipes.get(parsed.station)
                    if table is not None:
                        table[parsed.product] = parsed.unit_time_s
                _log.info(
                    "kpi_recipe_set",
                    station=parsed.station,
                    product=parsed.product,
                    unit_time_s=parsed.unit_time_s,
                )
                await message.ack()
                return
            if not isinstance(parsed, RunStarted):
                # Valid control vocabulary that does not affect KPIs.
                _log.debug("kpi_control_ignored", type=parsed.type)
                await message.ack()
                return
            async with self._lock:
                self.state = fold(self.state, parsed)
                self.run_params = parsed.params
                _log.info("kpi_run_started", run_id=parsed.run_id)
            # Persist params before ack: a publish failure nacks and retries (CR-02).
            pending = await self._snapshot()
            if pending is not None:
                await self._publish(*pending, raise_on_error=True)
            await message.ack()
        except Exception:  # noqa: BLE001
            _log.exception("kpi_handle_control_failed")
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("kpi_nack_failed")
        finally:
            self._leave()

    # -- ticker -----------------------------------------------------------
    async def _snapshot(self) -> tuple[KpiSnapshot, str] | None:
        """Compute one snapshot under the lock (None while no run is known)."""
        async with self._lock:
            run_id = self.state.run_id
            if run_id is None:
                return None
            snap = advance(
                self.state, effective_as_of(self.state, utc_now()), self.recipes, self.cfg
            )
            # Include run_params in every snapshot (Pitfall 3)
            if self.run_params is not None:
                snap = snap.model_copy(update={"run_params": self.run_params})
            return snap, run_id

    async def _publish(self, snap: KpiSnapshot, run_id: str, *, raise_on_error: bool) -> None:
        try:
            await asyncio.to_thread(produce_fact, self.producer, TOPIC_KPI_SNAPSHOTS, snap, run_id)
        except Exception:
            _log.exception("kpi_snapshot_publish_failed", run_id=run_id)
            if raise_on_error:
                raise

    async def tick(self) -> None:
        """Compute one snapshot under the lock, publish it outside the lock."""
        pending = await self._snapshot()
        if pending is None:
            return
        await self._publish(*pending, raise_on_error=False)  # next tick retries

    async def _tick_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 - the ticker must never die
                _log.exception("kpi_tick_failed")
            await asyncio.sleep(self.settings.snapshot_interval_s)

    # -- lifecycle --------------------------------------------------------
    def stop(self) -> None:
        """Request graceful stop; safe from signal handlers and tests."""
        self._stopping.set()

    async def run(self, connection: Any) -> None:
        """Run until stop(); drain in-flight handlers before closing."""
        await verify_rabbit(connection)
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap})
        await asyncio.to_thread(verify_kafka, admin)

        channel = await connection.channel()
        self.channel = channel
        high: dict[Any, int] = {}
        if self.settings.resume_on_start:
            snap_msgs, _ = await asyncio.to_thread(
                replay_topics,
                self.settings.kafka_bootstrap,
                [TOPIC_KPI_SNAPSHOTS],
                self.settings.replay_timeout_s,
                self.settings.kafka_group_id,
            )
            facts, high = await asyncio.to_thread(
                replay_topics,
                self.settings.kafka_bootstrap,
                [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE],
                self.settings.replay_timeout_s,
                self.settings.kafka_group_id,
            )
            await self.recover(facts, snap_msgs)

        control_queue = await channel.get_queue(Q_KPI_CONTROL, ensure=True)
        control_tag = await control_queue.consume(self.handle_control, no_ack=False)
        self._tail = KafkaTail(
            self.settings.kafka_bootstrap,
            self.settings.kafka_group_id,
            high,
            [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE],
            self.on_event,
            asyncio.get_running_loop(),
            thread_name="kpi-tail",
        )
        self._tail.start()
        self._ticker = asyncio.create_task(self._tick_loop())
        _log.info("kpi_started")

        await self._stopping.wait()
        _log.info("kpi_stopping", run_id=self.state.run_id)
        await control_queue.cancel(control_tag)
        await asyncio.to_thread(self._tail.stop)
        await self._idle.wait()
        self._ticker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._ticker
        await channel.close()
        _log.info("kpi_stopped")
