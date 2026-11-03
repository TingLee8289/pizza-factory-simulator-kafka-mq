"""Manual order placement (ORDER-03, D-07), via the generator (quick-wrj).

The dashboard no longer builds order ids or publishes order facts. It sends a
``generator.manual_order`` command over RabbitMQ (RPC: ``reply_to`` is a per-dashboard
exclusive queue, ``correlation_id`` is the command event_id) and waits for the
generator's reply. The generator owns the single L-NNNN sequence, publishes
order.lifecycle "created" and order.submitted, and answers with the final order.

Timeout -> NotAvailableError (HTTP 422); the command carries ``deadline_ts`` so the
generator refuses a request it dequeues too late (no ghost order).
"""

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

import structlog
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from pizza.common.constants import (
    BUFFER_K_DEFAULT,
    MAX_ORDER_QTY,
    PRIORITY_DEFAULT,
    PRIORITY_MAX,
    PRIORITY_MIN,
    PRODUCTS,
)
from pizza.common.envelope import EstimateEta, ManualOrderRequest, utc_now
from pizza.common.rpc import RpcBusy, RpcClient, RpcTimeout
from pizza.dashboard.control import ConflictError, NotAvailableError

_log = structlog.get_logger()

MACHINE_ID = "dashboard"
MANUAL_ORDER_KEY = "generator.manual_order"
ESTIMATE_ETA_KEY = "scheduler.estimate_eta"


class OrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product: str
    qty: int = Field(ge=1, le=MAX_ORDER_QTY)
    priority: int = Field(default=PRIORITY_DEFAULT, strict=True, ge=PRIORITY_MIN, le=PRIORITY_MAX)
    due_ts: AwareDatetime | None = None
    buffer_k: float = Field(default=BUFFER_K_DEFAULT, ge=0.5, le=3, allow_inf_nan=False)

    @field_validator("product")
    @classmethod
    def _product(cls, value: str) -> str:
        if value not in PRODUCTS:
            raise ValueError(f"unknown product {value!r}")
        return value


class EstimateRequest(BaseModel):
    """Dry-run ETA request: no order is created and no sequence number is consumed (D-07)."""

    model_config = ConfigDict(extra="forbid")

    product: str
    qty: int = Field(ge=1, le=MAX_ORDER_QTY)
    # Lax int (query strings arrive as text) but still bounded.
    priority: int = Field(default=PRIORITY_DEFAULT, ge=PRIORITY_MIN, le=PRIORITY_MAX)
    buffer_k: float = Field(default=BUFFER_K_DEFAULT, ge=0.5, le=3, allow_inf_nan=False)

    @field_validator("product")
    @classmethod
    def _product(cls, value: str) -> str:
        if value not in PRODUCTS:
            raise ValueError(f"unknown product {value!r}")
        return value


def _trunc_ms(value: datetime) -> datetime:
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


class ManualOrderClient(RpcClient):
    """RabbitMQ RPC client for ``generator.manual_order`` (shared mechanics in RpcClient)."""

    def __init__(
        self,
        get_channel: Callable[[], Any],
        get_control_exchange: Callable[[], Awaitable[Any]],
    ) -> None:
        super().__init__(get_channel, get_control_exchange)

    async def request(  # type: ignore[override]
        self, command: ManualOrderRequest, timeout_s: float
    ) -> dict[str, Any]:
        try:
            return await super().request(command, MANUAL_ORDER_KEY, timeout_s)
        except RpcTimeout:
            raise NotAvailableError("generator did not answer; order not placed") from None
        except RpcBusy:
            raise NotAvailableError("too many pending order requests") from None


async def place_manual_order(
    controller: Any,
    request: OrderRequest,
    client: ManualOrderClient,
    settings: Any,
) -> dict[str, Any]:
    if controller.state == "none":
        raise ConflictError("cannot place order: no active run")
    run_id = controller.run_info()["run_id"]

    now = utc_now()
    due_ts = None
    if request.due_ts is not None:
        due_ts = _trunc_ms(request.due_ts)
        if due_ts < now:
            raise ValueError("due_ts must not be earlier than now")

    timeout_s = settings.manual_order_timeout_s
    command = ManualOrderRequest.make(
        run_id=run_id,
        machine_id=MACHINE_ID,
        product=request.product,
        qty=request.qty,
        priority=request.priority,
        due_ts=due_ts,
        buffer_k=request.buffer_k,
        deadline_ts=now + timedelta(seconds=timeout_s),
    )
    reply = await client.request(command, timeout_s)
    if reply.get("ok") is True:
        return {
            "order_id": reply["order_id"],
            "run_id": reply["run_id"],
            "due_ts": datetime.fromisoformat(reply["due_ts"]),
            "priority": reply["priority"],
            "eta_ts": (datetime.fromisoformat(reply["eta_ts"]) if reply.get("eta_ts") else None),
        }
    detail = str(reply.get("detail") or "order rejected")
    code = reply.get("code")
    if code == "no_run":
        raise ConflictError(detail)
    if code == "expired":
        raise NotAvailableError(detail)
    raise ValueError(detail)


async def estimate_order(
    controller: Any,
    request: EstimateRequest,
    client: RpcClient,
    settings: Any,
) -> dict[str, Any]:
    """Ask the scheduler for an ETA of a hypothetical order; compose wall times from leads."""
    if controller.state == "none":
        raise ConflictError("cannot place order: no active run")
    run_id = controller.run_info()["run_id"]

    timeout_s = float(getattr(settings, "eta_timeout_s", 1.5))
    now = utc_now()
    command = EstimateEta.make(
        run_id=run_id,
        machine_id=MACHINE_ID,
        product=request.product,
        qty=request.qty,
        priority=request.priority,
        buffer_k=request.buffer_k,
        deadline_ts=now + timedelta(seconds=timeout_s),
    )
    try:
        reply = await client.request(command, ESTIMATE_ETA_KEY, timeout_s)
    except RpcTimeout:
        raise NotAvailableError("scheduler did not answer; estimate unavailable") from None
    except RpcBusy:
        raise NotAvailableError("too many pending estimate requests") from None

    if reply.get("ok") is True:
        lead_s = float(reply["eta_lead_s"])
        return {
            "eta_ts": _trunc_ms(now + timedelta(seconds=lead_s)),
            "station_eta": {
                station: _trunc_ms(now + timedelta(seconds=float(lead)))
                for station, lead in (reply.get("station_lead_s") or {}).items()
            },
            "due_ts": _trunc_ms(now + timedelta(seconds=lead_s * request.buffer_k)),
            "lead_s": lead_s,
            "buffer_k": request.buffer_k,
            "degraded": bool(reply.get("degraded", False)),
        }
    detail = str(reply.get("detail") or "estimate rejected")
    code = reply.get("code")
    if code == "no_run":
        raise ConflictError(detail)
    if code == "expired":
        raise NotAvailableError(detail)
    raise ValueError(detail)
