"""Station service: consume jobs, work in real time, emit facts, ack after flush.

Every handler path ends in exactly one of ack, nack(requeue=True) or
dead_letter_invalid (which acks). reject / nack(requeue=False) are never used,
because the jobs queue dead-letters into dispatch.retry and would loop (D-07).

Business failures are reported as op.failed facts (flushed) and then acked; the
scheduler decides about rework (D-11). A breakdown (random or manual) that starts while a
job is in its work window pauses it on the PauseGate reason "down"; the job is NOT resumed
after repair but judged failed: one op.failed(cause=breakdown) is emitted, then acked
(06.1 D-01..D-04). A job held while the station is down before its work starts runs normally.
"""

import asyncio
import contextlib
from collections.abc import Mapping
from typing import Any

import structlog
from confluent_kafka.admin import AdminClient

from pizza.common.bootstrap import verify_kafka, verify_rabbit
from pizza.common.envelope import (
    STATION_CONTROL_TYPES,
    Fact,
    InvalidEnvelope,
    JobCommand,
    MachineBreak,
    MachineRepair,
    OpCompleted,
    OpFailed,
    OpStarted,
    RecipeChanged,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetRecipe,
    StationHeartbeat,
    StationStatus,
    parse_message,
)
from pizza.common.failure_model import FailureSchedule
from pizza.common.kafka_io import produce_fact
from pizza.common.pause import GateAborted, PauseGate
from pizza.common.rabbit_io import dead_letter_invalid
from pizza.common.recipes import load_station_recipes
from pizza.common.timescale import TimeScale, TimeScaleFeed
from pizza.common.topology import TOPIC_STATION_EVENTS, control_queue_name, jobs_queue_name
from pizza.station.logic import (
    attempt_from,
    delivery_limit_exceeded,
    is_for_station,
    should_fail,
    work_seconds,
)

_log = structlog.get_logger()


class Station:
    """One station = one process = one single-server worker (prefetch 1)."""

    def __init__(
        self,
        name: str,
        producer: Any,
        settings: Any,
        channel: Any = None,
        recipes: Mapping[str, float] | None = None,
    ) -> None:
        self.name = name
        self._recipes = recipes
        self.producer = producer
        self.settings = settings
        self.channel = channel
        self.current_run_id: str | None = None
        self._stopping = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._inflight = 0
        self.timescale = TimeScale()
        self.gate = PauseGate(timescale=self.timescale)
        self._timescale_feed: TimeScaleFeed | None = None
        self._guard_active = False
        self._down = False
        self._cause: str | None = None
        self._repair_task: asyncio.Task | None = None
        self._job_active = False
        self._processing = False  # True only inside the work window (06.1 D-01)
        self._broken = asyncio.Event()  # an outage began during the work window
        self._idle_sent = False  # end_outage already emitted idle for a doomed job
        self._heartbeat_task: asyncio.Task | None = None
        self.failure = FailureSchedule(
            seed=42,
            station=name,
            mtbf_s=None,
            mttr_s=getattr(settings, "mttr_s", 20.0),
        )

    # -- emit -------------------------------------------------------------
    async def _emit(self, fact: Fact) -> None:
        """Synchronous flushed produce, off the loop thread (blocks up to 10 s)."""
        await asyncio.to_thread(produce_fact, self.producer, TOPIC_STATION_EVENTS, fact, self.name)

    async def _status(
        self,
        run_id: str,
        status: str,
        cause: str | None = None,
        repair_in_s: float | None = None,
    ) -> None:
        await self._emit(
            StationStatus.make(
                run_id=run_id,
                machine_id=self.name,
                status=status,
                cause=cause,
                repair_in_s=repair_in_s,
            )
        )

    # -- outage controller (D-03, D-05) -------------------------------------
    async def begin_outage(self, run_id: str, cause: str, repair_s: float) -> None:
        """Take the station down; no-op while already down (D-05)."""
        if self._down:
            return
        self._down = True
        self._cause = cause
        if self._processing:
            self._broken.set()
        self.gate.pause("down")
        self._repair_task = asyncio.create_task(self._auto_repair(run_id, repair_s))
        await self._status(run_id, "down", cause=cause, repair_in_s=repair_s)

    async def end_outage(self, run_id: str) -> None:
        """Repair now; no-op when not down."""
        if not self._down:
            return
        self._cancel_repair()
        self._down = False
        self._cause = None
        doomed = self._broken.is_set()
        if doomed:
            self._idle_sent = True
        self.gate.resume("down")
        await self._status(run_id, "busy" if self._job_active and not doomed else "idle")

    async def _auto_repair(self, run_id: str, repair_s: float) -> None:
        await self.timescale.sleep(repair_s)  # repair_s is simulated seconds
        try:
            await self.end_outage(run_id)
        except Exception:  # noqa: BLE001 - never leave the station stuck on a Kafka error
            _log.exception("station_auto_repair_emit_failed", station=self.name)

    def _cancel_repair(self) -> None:
        task, self._repair_task = self._repair_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    def reset_outage(self) -> None:
        """Drop any outage without emitting (run start / reset, Pitfall 3)."""
        self._cancel_repair()
        self._down = False
        self._cause = None
        self.gate.resume("down")

    async def _sleep_or_broken(self, seconds: float) -> bool:
        """Sleep on the gate; False if an outage broke the job meanwhile."""
        sleeper = asyncio.ensure_future(self.gate.sleep(seconds))
        broken = asyncio.ensure_future(self._broken.wait())
        try:
            await asyncio.wait({sleeper, broken}, return_when=asyncio.FIRST_COMPLETED)
            if sleeper.done():
                sleeper.result()  # re-raises GateAborted
                return not self._broken.is_set()
            return False
        finally:
            for task in (sleeper, broken):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeper, broken, return_exceptions=True)

    async def _work(self, job: JobCommand, work_s: float) -> bool:
        """Sliced work loop; False when an outage aborted the job (06.1 D-01).

        On False the outage has already ended (repair) and the job must be judged failed.
        A trip exactly at work end (remaining == 0) is not an outage (D-17).
        """
        remaining = work_s
        while remaining > 0:
            if self._broken.is_set():
                await self.gate.wait_running()
                return False
            budget = self.failure.budget_left()
            trip = budget is not None and budget < remaining
            run = budget if (trip and budget is not None) else remaining
            if not await self._sleep_or_broken(run):
                await self.gate.wait_running()
                return False
            remaining -= run
            self.failure.consume(run)
            if trip and remaining > 0:
                await self.begin_outage(job.run_id, "random", self.failure.trip())
                await self.gate.wait_running()
                return False
        return True

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

    # -- jobs -------------------------------------------------------------
    async def handle_job(self, message: Any) -> None:
        """Jobs-consumer callback; never lets an exception escape (T-02-09)."""
        self._enter()
        try:
            source = jobs_queue_name(self.name)
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(self.channel, message, f"invalid envelope: {exc}", source)
                return
            if not isinstance(parsed, JobCommand):
                await dead_letter_invalid(
                    self.channel, message, f"unexpected message type {parsed.type}", source
                )
                return
            if not is_for_station(parsed, self.name):
                await dead_letter_invalid(
                    self.channel,
                    message,
                    f"job step {parsed.step!r} is not for station {self.name!r}",
                    source,
                )
                return
            try:
                work_s = self.resolve_work_seconds(parsed)
            except ValueError as exc:  # UnknownRecipe, bound violation, RecipeError (D-04)
                await dead_letter_invalid(self.channel, message, f"invalid job: {exc}", source)
                return
            await self.process_job(message, parsed, work_s)
        except Exception:  # noqa: BLE001 - last resort so prefetch window never stalls
            _log.exception("station_handle_job_failed", station=self.name)
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("station_nack_failed", station=self.name)
        finally:
            self._leave()

    def resolve_work_seconds(self, job: JobCommand) -> float:
        """Work time from the override or this station's recipe table (lazy load)."""
        if job.unit_time_s is None:
            return work_seconds(job, self._recipe_table())
        return work_seconds(job, self._recipes)

    def _recipe_table(self) -> dict[str, float]:
        """Lazy-load this station's recipe table so live edits have a target."""
        if self._recipes is None:
            recipes_dir = getattr(self.settings, "recipes_dir", None)
            self._recipes = load_station_recipes(
                self.name, recipes_dir if isinstance(recipes_dir, str) else None
            )
        return self._recipes

    async def process_job(self, message: Any, job: JobCommand, work_s: float | None = None) -> None:
        """busy -> op.started -> work -> op.completed -> idle -> ack (D-05)."""
        if self._guard_active and job.run_id != self.current_run_id:
            _log.info(
                "station_stale_run_job_dropped",
                station=self.name,
                job_run_id=job.run_id,
                current_run_id=self.current_run_id,
            )
            await message.ack()
            return
        if work_s is None:
            work_s = self.resolve_work_seconds(job)
        self.current_run_id = job.run_id
        attempt = attempt_from(bool(message.redelivered), message.headers)
        try:
            if delivery_limit_exceeded(message.headers, jobs_queue_name(self.name)):
                await self._emit_failed(job, attempt, "delivery_limit")
                await message.ack()
                return
            await self.gate.wait_running()
            await self._status(job.run_id, "busy")
            self._job_active = True
            await self._emit(
                OpStarted.make(
                    run_id=job.run_id,
                    machine_id=self.name,
                    order_id=job.order_id,
                    step=job.step,
                    visit=job.visit,
                    attempt=attempt,
                    qty=job.qty,
                    product=job.product,
                    command_event_id=job.event_id,
                    redelivered=bool(message.redelivered),
                )
            )
            start = self.timescale.sim_now()
            if should_fail(job):
                await self._emit_failed(job, attempt, "injected")
                await self._idle_after_failed(job)
                await message.ack()
                return
            self._processing = True
            completed = await self._work(job, work_s)
            self._processing = False
            if not completed:
                if self.current_run_id != job.run_id:
                    # Run reset/restart raced the wake-up: abandon, ack, no events.
                    raise GateAborted
                if self._stopping.is_set():
                    # Shutdown during the outage: no verdict, redeliver (D-16).
                    await message.nack(requeue=True)
                    return
                await self._emit_failed(job, attempt, "machine_breakdown", cause="breakdown")
                if not self._idle_sent:
                    await self._idle_after_failed(job)
                await message.ack()
                return
            await self._emit(
                OpCompleted.make(
                    run_id=job.run_id,
                    machine_id=self.name,
                    order_id=job.order_id,
                    step=job.step,
                    visit=job.visit,
                    attempt=attempt,
                    qty=job.qty,
                    product=job.product,
                    command_event_id=job.event_id,
                    duration_s=self.timescale.sim_now() - start,
                )
            )
            await self._status(job.run_id, "idle")
        except GateAborted:
            # Run reset: abandon the job without op.completed, ack it (D-05).
            try:
                await self._status(job.run_id, "idle")
            except Exception:  # noqa: BLE001 - best effort
                _log.exception("station_abort_idle_failed", station=self.name)
            await message.ack()
            return
        except Exception:  # noqa: BLE001 - produce failure (D-08)
            _log.exception("station_emit_failed", station=self.name, order_id=job.order_id)
            await self._nack_after_delay(message)
            return
        finally:
            self._job_active = False
            self._processing = False
            self._broken.clear()
            self._idle_sent = False
        await message.ack()

    async def _idle_after_failed(self, job: JobCommand) -> None:
        """Best-effort idle once op.failed is out (06.2 WR-03).

        The scheduler returns the credit on op.failed, so the failed command must be
        acked even if this status produce fails: a nack would redeliver the failed
        visit next to the lot the scheduler has already dispatched (credit = 1).
        The next heartbeat / status repairs the missing idle.
        """
        try:
            await self._status(job.run_id, "idle")
        except Exception:  # noqa: BLE001 - best effort
            _log.exception("station_failed_idle_emit_failed", station=self.name)

    async def _emit_failed(
        self, job: JobCommand, attempt: int, reason: str, cause: str | None = None
    ) -> None:
        await self._emit(
            OpFailed.make(
                run_id=job.run_id,
                machine_id=self.name,
                order_id=job.order_id,
                step=job.step,
                visit=job.visit,
                attempt=attempt,
                qty=job.qty,
                product=job.product,
                command_event_id=job.event_id,
                reason=reason,
                cause=cause,
            )
        )

    # -- control ----------------------------------------------------------
    async def handle_control(self, message: Any) -> None:
        """Control-queue callback: run.started sets run_id (D-15)."""
        self._enter()
        try:
            source = control_queue_name(self.name)
            try:
                parsed = parse_message(message.body)
            except InvalidEnvelope as exc:
                await dead_letter_invalid(self.channel, message, f"invalid envelope: {exc}", source)
                return
            if not isinstance(parsed, STATION_CONTROL_TYPES):
                await dead_letter_invalid(
                    self.channel, message, f"unexpected message type {parsed.type}", source
                )
                return
            try:
                if isinstance(parsed, RunStarted):
                    busy = self._inflight > 1
                    self.current_run_id = parsed.run_id
                    self._guard_active = True
                    self.gate.clear()
                    self.reset_outage()
                    params = parsed.params
                    self.failure = FailureSchedule(
                        seed=params.seed if params else 42,
                        station=self.name,
                        mtbf_s=(
                            getattr(self.settings, "mtbf_s", None)
                            if (params and params.random_breakdowns)
                            else None
                        ),
                        mttr_s=getattr(self.settings, "mttr_s", 20.0),
                    )
                    if not busy:
                        await self._status(parsed.run_id, "idle")
                elif isinstance(parsed, RunPause):
                    self.gate.pause("run")
                elif isinstance(parsed, RunResume):
                    self.gate.resume("run")
                elif isinstance(parsed, RunReset):
                    self._guard_active = True
                    self.current_run_id = None
                    self.reset_outage()
                    self.gate.abort()
                    self.gate.clear()
                elif isinstance(parsed, MachineBreak):
                    repair_s = (
                        parsed.duration_s
                        if parsed.duration_s is not None
                        else self.failure.manual_repair_s()
                    )
                    await self.begin_outage(
                        self.current_run_id or parsed.run_id, "manual", repair_s
                    )
                elif isinstance(parsed, MachineRepair):
                    await self.end_outage(self.current_run_id or parsed.run_id)
                elif isinstance(parsed, SetRecipe):
                    if parsed.station == self.name:
                        table = self._recipe_table()
                        old = table.get(parsed.product)
                        if old == parsed.unit_time_s:
                            _log.info(
                                "station_recipe_unchanged",
                                station=self.name,
                                product=parsed.product,
                                unit_time_s=parsed.unit_time_s,
                            )
                        else:
                            run_id = self.current_run_id
                            if run_id is not None:
                                # Facts before state: a produce failure leaves the table untouched.
                                await self._emit(
                                    RecipeChanged.make(
                                        run_id=run_id,
                                        machine_id=self.name,
                                        station=self.name,
                                        product=parsed.product,
                                        old_unit_time_s=old,
                                        new_unit_time_s=parsed.unit_time_s,
                                    )
                                )
                            table[parsed.product] = parsed.unit_time_s
                            _log.info(
                                "station_recipe_set",
                                station=self.name,
                                product=parsed.product,
                                unit_time_s=parsed.unit_time_s,
                                run_id=run_id,
                            )
                    else:
                        _log.warning(
                            "station_recipe_ignored",
                            station=self.name,
                            requested=parsed.station,
                        )
                else:  # SetArrivalRate / SetRule: valid but irrelevant here
                    _log.debug("station_control_ignored", station=self.name, type=parsed.type)
            except Exception:  # noqa: BLE001
                _log.exception("station_emit_failed", station=self.name)
                await self._nack_after_delay(message)
                return
            await message.ack()
        except Exception:  # noqa: BLE001
            _log.exception("station_handle_control_failed", station=self.name)
            try:
                await self._nack_after_delay(message)
            except Exception:  # noqa: BLE001
                _log.exception("station_nack_failed", station=self.name)
        finally:
            self._leave()

    # -- lifecycle --------------------------------------------------------
    def _current_status(self) -> str:
        if self._down:
            return "down"
        return "busy" if self._job_active else "idle"

    async def _heartbeat_loop(self, interval_s: float) -> None:
        """Liveness facts independent of jobs; a Kafka stall never kills the loop."""
        while True:
            await asyncio.sleep(interval_s)
            try:
                await asyncio.wait_for(
                    self._emit(
                        StationHeartbeat.make(
                            run_id=self.current_run_id or "unknown",
                            machine_id=self.name,
                            status=self._current_status(),
                        )
                    ),
                    timeout=interval_s,
                )
            except Exception:  # noqa: BLE001 - includes TimeoutError
                _log.warning("station_heartbeat_failed", station=self.name)

    def stop(self) -> None:
        """Request graceful stop; safe from signal handlers and tests."""
        self._stopping.set()

    async def run(self, connection: Any) -> None:
        """Verify topology, consume jobs and control, stop gracefully (D-14)."""
        await verify_rabbit(connection)
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap})
        await asyncio.to_thread(verify_kafka, admin)
        self._timescale_feed = TimeScaleFeed(
            self.settings.kafka_bootstrap, f"station-{self.name}", self.timescale
        )
        await self._timescale_feed.start()

        jobs_channel = await connection.channel()
        await jobs_channel.set_qos(prefetch_count=1)
        control_channel = await connection.channel()
        self.channel = jobs_channel

        jobs_queue = await jobs_channel.get_queue(jobs_queue_name(self.name), ensure=True)
        control_queue = await control_channel.get_queue(control_queue_name(self.name), ensure=True)
        jobs_tag = await jobs_queue.consume(self.handle_job, no_ack=False)
        control_tag = await control_queue.consume(self.handle_control, no_ack=False)
        _log.info("station_started", station=self.name)

        heartbeat_s = getattr(self.settings, "heartbeat_interval_s", 0)
        if heartbeat_s and heartbeat_s > 0:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(heartbeat_s))

        await self._stopping.wait()

        _log.info("station_stopping", station=self.name, run_id=self.current_run_id)
        await self._shutdown(jobs_queue, jobs_tag, control_queue, control_tag)
        await jobs_channel.close()
        await control_channel.close()
        _log.info("station_stopped", station=self.name, run_id=self.current_run_id)

    async def _shutdown(
        self, jobs_queue: Any, jobs_tag: Any, control_queue: Any, control_tag: Any
    ) -> None:
        await jobs_queue.cancel(jobs_tag)
        await control_queue.cancel(control_tag)
        if self._timescale_feed is not None:
            await self._timescale_feed.stop()
        if self._down:  # a held job must be able to finish (Pitfall 5)
            try:
                await self.end_outage(self.current_run_id or "unknown")
            except Exception:  # noqa: BLE001
                _log.exception("station_shutdown_repair_failed", station=self.name)
                self.reset_outage()
        await self._idle.wait()  # never cancel the in-flight job (Pitfall 5)
        task, self._heartbeat_task = self._heartbeat_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self.current_run_id is not None:
            try:
                await self._status(self.current_run_id, "offline")
            except Exception:  # noqa: BLE001
                _log.exception("station_offline_emit_failed", station=self.name)
