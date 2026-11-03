"""Run lifecycle controller: state machine, command fan-out, queue purge (DASH-03).

State machine: none --start--> running <--pause/resume--> paused;
running|paused --reset--> none. Illegal transitions raise ConflictError (HTTP 409).
Credentials and the AMQP URL are never logged (T-05-27).
"""

import asyncio
import re
import secrets
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Literal

import structlog
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pizza.common.constants import (
    BUFFER_K_DEFAULT,
    MAX_ORDERS_CAP,
    PRIORITY_MAX,
    PRIORITY_MIN,
    PRODUCTS,
    STATIONS,
)
from pizza.common.envelope import (
    LotBank,
    LotHoldCancel,
    LotRelease,
    LotScrap,
    LotSetPriority,
    MachineBreak,
    MachineRepair,
    RoundClose,
    RunParams,
    RunPause,
    RunReset,
    RunResume,
    SetArrivalRate,
    SetRecipe,
    SetRule,
    utc_now,
)
from pizza.common.rabbit_io import publish_command
from pizza.common.topology import (
    Q_DISPATCH_DLQ,
    Q_REWORK_RETURN,
    Q_WORKORDERS_INTAKE,
    ROUTING_SCHEDULER_PRIORITY,
    jobs_queue_name,
    retry_queue_name,
    rework_hold_queue_name,
)
from pizza.tools.send_job import build_run_started, run_targets

_log = structlog.get_logger()

MAX_ARRIVAL_PER_MIN = 12.0
DEFAULT_ARRIVAL_PER_MIN = 60.0 / 35.0  # matches RunParams default (1/35 per second)
MAX_SEED = 2**31 - 1
ORDER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class ConflictError(Exception):
    """Illegal state transition (mapped to HTTP 409)."""


class NotAvailableError(Exception):
    """Requested feature is not available yet (mapped to HTTP 422)."""


class UnknownLotError(Exception):
    """Unknown order id (mapped to HTTP 404)."""


class UnknownStationError(ValueError):
    """Unknown station name (mapped to HTTP 404)."""


def _check_weights(value: dict[str, float] | None) -> dict[str, float] | None:
    if value is None:
        return None
    for key, weight in value.items():
        if key not in PRODUCTS:
            raise ValueError(f"unknown product {key!r}")
        if not weight >= 0:
            raise ValueError("weights must be >= 0")
    if sum(value.values()) <= 0:
        raise ValueError("weights must sum to > 0")
    return value


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seed: int | None = Field(default=None, ge=0, le=MAX_SEED)
    arrival_rate_per_min: float = Field(
        default=DEFAULT_ARRIVAL_PER_MIN, gt=0, le=MAX_ARRIVAL_PER_MIN
    )
    weights: dict[str, float] | None = None
    random_breakdowns: bool = False
    # Phase 7: comparison runner fields (all optional, only used during comparison runs)
    k: float = Field(default=2.0, ge=1.0, le=10.0)
    max_orders: int | None = Field(default=None, ge=1, le=MAX_ORDERS_CAP)
    rule: Literal["fifo", "edd", "spt"] | None = None
    comparison_id: str | None = Field(default=None, pattern=r"^c-[0-9a-z-]{1,36}$")
    round: int | None = Field(default=None, ge=1, le=3)
    # Phase 7.1: multiplier on the ETA lead for auto orders (due = created + lead * buffer_k).
    buffer_k: float | None = Field(default=None, ge=0.5, le=10, allow_inf_nan=False)

    @field_validator("weights")
    @classmethod
    def _weights(cls, value: dict[str, float] | None) -> dict[str, float] | None:
        return _check_weights(value)


class RateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    arrival_rate_per_min: float = Field(gt=0, le=MAX_ARRIVAL_PER_MIN)


class BreakRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duration_s: float | None = Field(default=None, gt=0, le=3600)


class BankRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Literal["capacity", "customer", "schedule"]


class ScrapRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Literal["operator", "quality", "material", "other"]


class PriorityRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority: int = Field(strict=True, ge=PRIORITY_MIN, le=PRIORITY_MAX)


class RuleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule: Literal["fifo", "edd", "spt"]


class TimeScaleRequest(BaseModel):
    """Simulation speed multiplier (quick-et6); valid in every run state."""

    model_config = ConfigDict(extra="forbid")

    scale: Literal[1, 2, 4, 8]


class RecipeRequest(BaseModel):
    """One live recipe edit; ``unit_time_s`` is seconds per pizza."""

    model_config = ConfigDict(extra="forbid")

    product: str = Field(min_length=1)
    unit_time_s: float = Field(gt=0, le=3600)

    @field_validator("product")
    @classmethod
    def _known_product(cls, value: str) -> str:
        if value not in PRODUCTS:
            raise ValueError(f"unknown product {value!r}")
        return value


def command_targets(kind: str) -> list[tuple[str, str]]:
    """(routing_key, machine_id) pairs for a control action, in publish order."""
    if kind == "start":
        return run_targets("all")
    if kind == "pause":
        return [
            ("scheduler.pause", "scheduler"),
            *((f"station.{s}.pause", s) for s in STATIONS),
            ("generator.pause", "generator"),
        ]
    if kind == "resume":
        return [
            *((f"station.{s}.resume", s) for s in STATIONS),
            ("scheduler.resume", "scheduler"),
            ("generator.resume", "generator"),
        ]
    if kind == "reset":
        return [
            ("generator.reset", "generator"),
            ("scheduler.reset", "scheduler"),
            *((f"station.{s}.reset", s) for s in STATIONS),
            ("kpi.reset", "kpi-service"),
        ]
    if kind == "rate":
        return [("generator.rate", "generator")]
    if kind == "rule":
        return [("scheduler.rule", "scheduler")]
    if kind == "close_round":
        return [("kpi.close", "kpi-service")]
    if kind == "stop_generation":
        return [("generator.reset", "generator")]
    raise ValueError(f"unknown command kind {kind!r}")


def line_queue_names() -> list[str]:
    """Fixed purge set: intake, every station jobs+retry queue, rework hold queues,
    the rework return queue, the DLQ (T-05-29)."""
    return [
        Q_WORKORDERS_INTAKE,
        *(jobs_queue_name(s) for s in STATIONS),
        *(retry_queue_name(s) for s in STATIONS),
        *(rework_hold_queue_name(s) for s in STATIONS),
        Q_REWORK_RETURN,
        Q_DISPATCH_DLQ,
    ]


async def purge_line_queues(channel: Any) -> dict[str, int | None]:
    """Purge ready messages from the line queues; a failing queue is logged and skipped."""
    out: dict[str, int | None] = {}
    for name in line_queue_names():
        try:
            queue = await channel.get_queue(name, ensure=False)
            result = await queue.purge()
            count = getattr(result, "message_count", None)
            out[name] = count if isinstance(count, int) else None
        except Exception:  # noqa: BLE001 - keep purging the remaining queues
            _log.warning("purge_queue_failed", queue=name)
            out[name] = None
    return out


def _mint_run_id() -> str:
    return f"r-{datetime.now(UTC):%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"


def _empty_info() -> dict[str, Any]:
    return {
        "state": "none",
        "run_id": None,
        "seed": None,
        "arrival_rate_per_min": None,
        "weights": None,
        "started_ts": None,
        "random_breakdowns": False,
        "k": None,
        "max_orders": None,
        "rule": None,
        "comparison_id": None,
        "round": None,
    }


class RunController:
    def __init__(
        self,
        settings: Any,
        get_control_exchange: Callable[[], Awaitable[Any]],
        purge: Callable[[], Awaitable[Any]],
        on_change: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        place_order_fn: Callable[[Any, Any], Awaitable[dict[str, Any]]] | None = None,
        estimate_order_fn: Callable[[Any, Any], Awaitable[dict[str, Any]]] | None = None,
        lot_state_fn: Callable[[str], str | None] | None = None,
    ) -> None:
        self.settings = settings
        self._get_exchange = get_control_exchange
        self._purge = purge
        self._on_change = on_change
        self._place_order_fn = place_order_fn
        self._estimate_order_fn = estimate_order_fn
        # order_id -> derived lot state ("queued", "done", ...) or None when unknown.
        self._lot_state_fn = lot_state_fn
        self._lock = asyncio.Lock()
        self._info: dict[str, Any] = _empty_info()
        self.comparison_active = False

    # -- state ---------------------------------------------------------------
    @property
    def state(self) -> str:
        return self._info["state"]

    def run_info(self) -> dict[str, Any]:
        info = dict(self._info)
        if info["weights"] is not None:
            info["weights"] = dict(info["weights"])
        return info

    def adopt(self, run_id: str, paused: bool = False) -> None:
        """Restart recovery: seed and params are unknown (Pitfall 9)."""
        self._info = {
            **_empty_info(),
            "state": "paused" if paused else "running",
            "run_id": run_id,
        }

    async def _notify(self, event: str) -> None:
        if self._on_change is not None:
            await self._on_change(event, self.run_info())

    async def _publish_all(self, kind: str, build: Callable[[str], Any]) -> None:
        exchange = await self._get_exchange()
        for routing_key, machine_id in command_targets(kind):
            await publish_command(exchange, routing_key, build(machine_id))

    def _require_active(self, action: str) -> str:
        if self.state == "none":
            raise ConflictError(f"cannot {action}: no active run")
        return self._info["run_id"]

    def _guard(self) -> None:
        """Raise ConflictError if a comparison is active."""
        if self.comparison_active:
            raise ConflictError("Locked while a comparison is running")

    # -- actions -------------------------------------------------------------
    async def start(self, request: StartRequest, *, _internal: bool = False) -> dict[str, Any]:
        async with self._lock:
            if self.state != "none":
                raise ConflictError("a run is already active")
            run_id = _mint_run_id()
            seed = request.seed if request.seed is not None else secrets.randbelow(2**31)
            weights = request.weights or dict.fromkeys(PRODUCTS, 1.0)
            params = RunParams(
                seed=seed,
                arrival_rate=request.arrival_rate_per_min / 60.0,
                weights=weights,
                random_breakdowns=request.random_breakdowns,
                k=request.k,
                # Comparison "Due tightness (k)" now multiplies the ETA lead (D-09(c)); the UI
                # label is unchanged. Explicit buffer_k always wins.
                buffer_k=(
                    request.buffer_k
                    if request.buffer_k is not None
                    else request.k
                    if request.comparison_id is not None
                    else BUFFER_K_DEFAULT
                ),
                max_orders=request.max_orders,
                rule=request.rule,
                comparison_id=request.comparison_id,
                round=request.round,
            )
            ts = utc_now()  # one ts so generator t0 and scheduler agree
            exchange = await self._get_exchange()
            for routing_key, machine_id in command_targets("start"):
                msg = build_run_started(machine_id, run_id, params=params, ts=ts)
                await publish_command(exchange, routing_key, msg)  # generator last
            self._info = {
                "state": "running",
                "run_id": run_id,
                "seed": seed,
                "arrival_rate_per_min": request.arrival_rate_per_min,
                "weights": dict(weights),
                "started_ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "random_breakdowns": request.random_breakdowns,
                "k": request.k,
                "max_orders": request.max_orders,
                "rule": request.rule,
                "comparison_id": request.comparison_id,
                "round": request.round,
            }
            await self._notify("started")
            return self.run_info()

    async def pause(self, slept_s: float | None = None) -> dict[str, Any]:
        self._guard()
        async with self._lock:
            run_id = self._require_active("pause")
            if self.state == "paused":
                raise ConflictError("run is already paused")
            await self._publish_all(
                "pause", lambda m: RunPause.make(run_id=run_id, machine_id=m, slept_s=slept_s)
            )
            self._info["state"] = "paused"
            await self._notify("paused")
            return self.run_info()

    async def resume(self) -> dict[str, Any]:
        self._guard()
        async with self._lock:
            run_id = self._require_active("resume")
            if self.state != "paused":
                raise ConflictError("run is not paused")
            await self._publish_all("resume", lambda m: RunResume.make(run_id=run_id, machine_id=m))
            self._info["state"] = "running"
            await self._notify("resumed")
            return self.run_info()

    async def reset(self, *, _internal: bool = False) -> dict[str, Any]:
        if not _internal:
            self._guard()
        async with self._lock:
            run_id = self._require_active("reset")
            # Commands first so stations abort in-flight jobs, then purge (Pitfall 5).
            await self._publish_all("reset", lambda m: RunReset.make(run_id=run_id, machine_id=m))
            await self._purge()
            self._info = _empty_info()
            await self._notify("reset")
            return self.run_info()

    async def set_rate(self, request: RateRequest) -> dict[str, Any]:
        self._guard()
        async with self._lock:
            run_id = self._require_active("change rate")
            await self._publish_all(
                "rate",
                lambda m: SetArrivalRate.make(
                    run_id=run_id,
                    machine_id=m,
                    arrival_rate=request.arrival_rate_per_min / 60.0,
                ),
            )
            self._info["arrival_rate_per_min"] = request.arrival_rate_per_min
            return self.run_info()

    async def set_rule(self, request: RuleRequest) -> dict[str, Any]:
        self._guard()
        async with self._lock:
            run_id = self._require_active("change rule")
            await self._publish_all(
                "rule",
                lambda m: SetRule.make(run_id=run_id, machine_id=m, rule=request.rule),
            )
            return self.run_info()

    async def set_recipe(self, name: str, request: RecipeRequest) -> dict[str, Any]:
        """Apply one live recipe change to the station, scheduler and KPI service.

        Editing is allowed before a run starts (the services keep the table
        across runs), so the envelope carries ``"config"`` when no run is active.
        Work already in progress keeps its original duration.
        """
        self._check_station(name)
        self._guard()
        async with self._lock:
            run_id = self._info["run_id"] or "config"
            exchange = await self._get_exchange()
            for routing_key, machine_id in (
                (f"station.{name}.recipe", name),
                ("scheduler.recipe", "scheduler"),
                ("kpi.recipe", "kpi-service"),
            ):
                cmd = SetRecipe.make(
                    run_id=run_id,
                    machine_id=machine_id,
                    station=name,
                    product=request.product,
                    unit_time_s=request.unit_time_s,
                )
                await publish_command(exchange, routing_key, cmd)
            return {
                "accepted": True,
                "station": name,
                "product": request.product,
                "unit_time_s": request.unit_time_s,
            }

    # -- failure / BANK controls: publish only, never mutate line state (UI-SPEC 9) ----
    @staticmethod
    def _check_station(name: str) -> None:
        if name not in STATIONS:
            raise UnknownStationError(f"unknown station {name!r}")

    @staticmethod
    def _check_order_id(order_id: str) -> None:
        if not ORDER_ID_RE.fullmatch(order_id):
            raise ValueError("invalid order_id")

    async def break_station(self, name: str, request: BreakRequest | None = None) -> dict[str, Any]:
        self._check_station(name)
        request = request or BreakRequest()
        self._guard()
        async with self._lock:
            run_id = self._require_active("break station")
            cmd = MachineBreak.make(run_id=run_id, machine_id=name, duration_s=request.duration_s)
            await publish_command(await self._get_exchange(), f"station.{name}.break", cmd)
            return {"accepted": True}

    async def repair_station(self, name: str) -> dict[str, Any]:
        self._check_station(name)
        async with self._lock:
            run_id = self._require_active("repair station")
            cmd = MachineRepair.make(run_id=run_id, machine_id=name)
            await publish_command(await self._get_exchange(), f"station.{name}.repair", cmd)
            return {"accepted": True}

    async def bank_lot(self, order_id: str, request: BankRequest) -> dict[str, Any]:
        self._check_order_id(order_id)
        self._guard()
        async with self._lock:
            run_id = self._require_active("bank lot")
            cmd = LotBank.make(
                run_id=run_id, machine_id="scheduler", order_id=order_id, reason=request.reason
            )
            await publish_command(await self._get_exchange(), "scheduler.bank", cmd)
            return {"accepted": True}

    async def scrap_lot(self, order_id: str, request: ScrapRequest) -> dict[str, Any]:
        """Forward a manual scrap; queued / running / banked lots qualify (404 / 409 otherwise).

        Running or dispatched lots get a deferred scrap (applied when the op completes);
        the scheduler validates again.
        """
        self._check_order_id(order_id)
        self._guard()
        async with self._lock:
            run_id = self._require_active("scrap lot")
            if self._lot_state_fn is None:
                raise NotAvailableError("lot state is not wired")
            lot_state = self._lot_state_fn(order_id)
            if lot_state is None:
                raise UnknownLotError("unknown lot")
            if lot_state not in ("queued", "running", "banked"):
                raise ConflictError(f"cannot scrap: lot is {lot_state}")
            cmd = LotScrap.make(
                run_id=run_id, machine_id="scheduler", order_id=order_id, reason=request.reason
            )
            await publish_command(await self._get_exchange(), "scheduler.scrap", cmd)
            return {"accepted": True}

    async def set_lot_priority(self, order_id: str, request: PriorityRequest) -> dict[str, Any]:
        """Forward a priority change; the scheduler validates again and suppresses no-ops."""
        self._check_order_id(order_id)
        self._guard()
        async with self._lock:
            run_id = self._require_active("change priority")
            if self._lot_state_fn is None:
                raise NotAvailableError("lot state is not wired")
            lot_state = self._lot_state_fn(order_id)
            if lot_state is None:
                raise UnknownLotError("unknown lot")
            if lot_state in ("done", "failed", "pre_release"):
                raise ConflictError(f"cannot change priority: lot is {lot_state}")
            cmd = LotSetPriority.make(
                run_id=run_id,
                machine_id="scheduler",
                order_id=order_id,
                priority=request.priority,
            )
            await publish_command(await self._get_exchange(), ROUTING_SCHEDULER_PRIORITY, cmd)
            return {"accepted": True}

    async def release_lot(self, order_id: str) -> dict[str, Any]:
        self._check_order_id(order_id)
        async with self._lock:
            run_id = self._require_active("release lot")
            cmd = LotRelease.make(run_id=run_id, machine_id="scheduler", order_id=order_id)
            await publish_command(await self._get_exchange(), "scheduler.release", cmd)
            return {"accepted": True}

    async def cancel_hold_lot(self, order_id: str) -> dict[str, Any]:
        """Cancel a pending deferred bank / scrap request (scheduler ignores unknown holds)."""
        self._check_order_id(order_id)
        async with self._lock:
            run_id = self._require_active("cancel hold")
            cmd = LotHoldCancel.make(run_id=run_id, machine_id="scheduler", order_id=order_id)
            await publish_command(await self._get_exchange(), "scheduler.hold_cancel", cmd)
            return {"accepted": True}

    async def send_round_close(self, partial: bool) -> dict[str, Any]:
        """Signal the KPI service to close a comparison round."""
        async with self._lock:
            run_id = self._require_active("close round")
            await self._publish_all(
                "close_round",
                lambda m: RoundClose.make(run_id=run_id, machine_id=m, partial=partial),
            )
            return {"accepted": True}

    async def stop_generation(self) -> None:
        """Stop order generation without resetting the run state."""
        async with self._lock:
            run_id = self._require_active("stop generation")
            await self._publish_all(
                "stop_generation",
                lambda m: RunReset.make(run_id=run_id, machine_id=m),
            )

    async def place_order(self, request: Any) -> dict[str, Any]:
        self._guard()
        if self._place_order_fn is None:
            raise NotAvailableError("manual orders are not wired")
        return await self._place_order_fn(self, request)

    async def estimate_order(self, request: Any) -> dict[str, Any]:
        self._guard()
        if self._estimate_order_fn is None:
            raise NotAvailableError("order estimates are not wired")
        return await self._estimate_order_fn(self, request)
