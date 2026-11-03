"""REST + SSE routes and the security middleware (DASH-01..04, ORDER-03, D-02).

Contract: see 05-08-PLAN.md. Errors are JSON ``{"detail": ...}``: 409 ConflictError,
422 validation / NotAvailableError / bad due_ts, 415 non-JSON POST, 503 too many SSE
clients or runtime not ready, 500 generic. Broker exception text never reaches the
browser (T-05-33). There is deliberately no CORS (same-origin only, T-05-31).
"""

import asyncio
import json
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi import Path as PathParam
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.sse import EventSourceResponse, ServerSentEvent
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from pizza.dashboard.comparison import (
    ComparisonStartRequest,
    comparison_csv,
    comparison_rows,
    csv_filename,
)
from pizza.dashboard.control import (
    BankRequest,
    BreakRequest,
    ConflictError,
    NotAvailableError,
    PriorityRequest,
    RateRequest,
    RecipeRequest,
    RuleRequest,
    ScrapRequest,
    StartRequest,
    TimeScaleRequest,
    UnknownLotError,
    UnknownStationError,
)
from pizza.dashboard.hub import Channel, Subscription, TooManySubscribers
from pizza.dashboard.orders import EstimateRequest, OrderRequest
from pizza.kpi.recompute import RUN_ID_RE, NoStoredSnapshot, run_recompute

_log = structlog.get_logger()

_COMPARISON_ID_RE = re.compile(r"^c-[0-9a-z-]{1,36}$")

STATIC_DIR = Path(__file__).parent / "static"


class NoCacheStaticFiles(StaticFiles):
    # `no-cache` makes the browser revalidate (ETag / Last-Modified) on every load, so edited JS and CSS
    # show up on a normal reload instead of the heuristic cache serving the previous version.
    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


CSP = (
    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
    "connect-src 'self'; img-src 'self' data:"
)
SECURITY_HEADERS = {
    "content-security-policy": CSP,
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
}
RETRY_MS = 2000


# ---- security middleware ------------------------------------------------------


def _json_bytes(status: int, detail: str) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    body = json.dumps({"detail": detail}).encode()
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    return body, headers


class SecurityMiddleware:
    """Adds security headers, enforces JSON-only POST and same-origin, hides 500 details."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = False

        async def send_wrapped(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                headers = list(message.get("headers", []))
                present = {k.lower() for k, _ in headers}
                for name, value in SECURITY_HEADERS.items():
                    if name.encode() not in present:
                        headers.append((name.encode(), value.encode()))
                message = {**message, "headers": headers}
            await send(message)

        async def reject(status: int, detail: str) -> None:
            body, headers = _json_bytes(status, detail)
            await send_wrapped(
                {"type": "http.response.start", "status": status, "headers": headers}
            )
            await send_wrapped({"type": "http.response.body", "body": body})

        headers_in = {k.lower(): v for k, v in scope.get("headers", [])}
        if scope["method"] == "POST":
            origin = headers_in.get(b"origin")
            host = headers_in.get(b"host")
            if origin is not None and host is not None:
                netloc = origin.split(b"://", 1)[-1]
                if netloc != host:
                    await reject(403, "cross-origin request rejected")
                    return
            ctype = headers_in.get(b"content-type")
            length = headers_in.get(b"content-length", b"0")
            has_body = length not in (b"0", b"") or b"transfer-encoding" in headers_in
            if ctype is not None:
                ok = ctype.split(b";", 1)[0].strip().lower() == b"application/json"
            else:
                ok = not has_body
            if not ok:
                await reject(415, "content-type must be application/json")
                return

        try:
            await self.app(scope, receive, send_wrapped)
        except Exception:  # noqa: BLE001 - generic body, details only in the server log
            _log.exception("dashboard_unhandled_error", path=scope.get("path"))
            if not started:
                await reject(500, "internal error")


# ---- SSE generators -----------------------------------------------------------


def _subscribe(runtime: Any, channel: Channel) -> Subscription:
    try:
        return runtime.hub.subscribe(channel)
    except TooManySubscribers as exc:
        raise HTTPException(status_code=503, detail="too many connections") from exc


async def state_stream(
    runtime: Any, sub: Subscription | None = None
) -> AsyncIterator[ServerSentEvent]:
    """Full snapshot on connect, then every hub "state" item (the 1 Hz tick)."""
    sub = sub or _subscribe(runtime, "state")
    try:
        snapshot = runtime.snapshot_payload()
        first_seq = int(snapshot["seq"])
        yield ServerSentEvent(data=snapshot, event="snapshot", id=str(first_seq), retry=RETRY_MS)
        while True:
            item = await sub.get()
            if item.seq <= first_seq:
                continue  # already covered by the snapshot
            yield ServerSentEvent(data=item.payload, event=item.kind, id=str(item.seq))
    finally:
        runtime.hub.unsubscribe(sub)


async def event_stream(
    runtime: Any, sub: Subscription | None = None
) -> AsyncIterator[ServerSentEvent]:
    """One "fact" event per Kafka fact (payload already carries seq)."""
    sub = sub or _subscribe(runtime, "events")
    try:
        first = True
        while True:
            item = await sub.get()
            yield ServerSentEvent(
                data=item.payload,
                event=item.kind,
                id=str(item.seq),
                retry=RETRY_MS if first else None,
            )
            first = False
    finally:
        runtime.hub.unsubscribe(sub)


# ---- dependencies --------------------------------------------------------------


def get_runtime(request: Request) -> Any:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status_code=503, detail="not ready")
    return runtime


RuntimeDep = Annotated[Any, Depends(get_runtime)]


async def _events_sub(runtime: RuntimeDep) -> AsyncIterator[Subscription]:
    sub = _subscribe(runtime, "events")
    try:
        yield sub
    finally:
        runtime.hub.unsubscribe(sub)


async def _state_sub(runtime: RuntimeDep) -> AsyncIterator[Subscription]:
    sub = _subscribe(runtime, "state")
    try:
        yield sub
    finally:
        runtime.hub.unsubscribe(sub)


StateSub = Annotated[Subscription, Depends(_state_sub)]
EventsSub = Annotated[Subscription, Depends(_events_sub)]

# ---- router ----------------------------------------------------------------------

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/state")
async def api_state(runtime: RuntimeDep) -> dict[str, Any]:
    return runtime.snapshot_payload()


@router.get("/api/stream/state", response_class=EventSourceResponse)
async def stream_state(runtime: RuntimeDep, sub: StateSub) -> AsyncIterator[ServerSentEvent]:
    async for item in state_stream(runtime, sub):
        yield item


@router.get("/api/stream/events", response_class=EventSourceResponse)
async def stream_events(runtime: RuntimeDep, sub: EventsSub) -> AsyncIterator[ServerSentEvent]:
    async for item in event_stream(runtime, sub):
        yield item


@router.post("/api/run/start")
async def run_start(runtime: RuntimeDep, body: StartRequest | None = None) -> dict[str, Any]:
    return await runtime.controller.start(body or StartRequest())


@router.post("/api/run/pause")
async def run_pause(runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.pause()


@router.post("/api/run/resume")
async def run_resume(runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.resume()


@router.post("/api/run/reset")
async def run_reset(runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.reset()


@router.post("/api/rate")
async def set_rate(body: RateRequest, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.set_rate(body)


@router.post("/api/rule")
async def set_rule(body: RuleRequest, runtime: RuntimeDep) -> dict[str, Any]:
    await runtime.controller.set_rule(body)
    return {"rule": body.rule}


@router.post("/api/time_scale")
async def set_time_scale(body: TimeScaleRequest, runtime: RuntimeDep) -> dict[str, Any]:
    try:
        return await runtime.set_time_scale(body.scale)
    except Exception as exc:  # noqa: BLE001 - never echo broker detail (T-05-33)
        _log.exception("dashboard_time_scale_publish_failed")
        raise HTTPException(status_code=503, detail="time scale publish failed") from exc


@router.get("/api/recipes")
async def list_recipes(runtime: RuntimeDep) -> dict[str, Any]:
    return {"recipes": runtime.recipe_tables()}


@router.post("/api/station/{name}/recipe")
async def station_recipe(name: str, body: RecipeRequest, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.set_recipe(name, body)


# ---- Phase 7: dispatch-rule comparison -----------------------------------------


@router.post("/api/comparison/start")
async def comparison_start(
    runtime: RuntimeDep, body: ComparisonStartRequest | None = None
) -> dict[str, Any]:
    await runtime.comparison.start(body or ComparisonStartRequest())
    return runtime.comparison_status()


@router.post("/api/comparison/next")
async def comparison_next(runtime: RuntimeDep) -> dict[str, Any]:
    await runtime.comparison.next_round()
    return runtime.comparison_status()


@router.post("/api/comparison/end")
async def comparison_end(runtime: RuntimeDep) -> dict[str, Any]:
    await runtime.comparison.end_round()
    return runtime.comparison_status()


@router.post("/api/comparison/cancel")
async def comparison_cancel(runtime: RuntimeDep) -> dict[str, Any]:
    await runtime.comparison.cancel()
    return runtime.comparison_status()


@router.get("/api/comparison")
async def comparison_status_route(runtime: RuntimeDep) -> dict[str, Any]:
    return runtime.comparison_status()


@router.get("/api/comparison.csv")
async def comparison_csv_route(
    runtime: RuntimeDep, comparison_id: str | None = None
) -> Response:
    if comparison_id is not None and not _COMPARISON_ID_RE.fullmatch(comparison_id):
        raise HTTPException(status_code=422, detail="invalid comparison_id")
    results = runtime.comparison.results
    if comparison_id is not None:
        results = {k: v for k, v in results.items() if k[0] == comparison_id}
    rows = comparison_rows(results)
    body = comparison_csv(rows)
    filename = csv_filename(rows)
    return Response(
        content=body,
        media_type="text/csv",
        headers={"content-disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/runs")
async def list_runs(runtime: RuntimeDep) -> dict[str, Any]:
    return {"runs": runtime.runs_list()}


@router.post("/api/runs/{run_id}/recompute")
async def recompute_run(run_id: str, runtime: RuntimeDep) -> dict[str, Any]:
    if not RUN_ID_RE.match(run_id):
        raise HTTPException(status_code=422, detail="invalid run_id format")
    cfg, recipes = runtime.recompute_cfg()
    settings = runtime.settings
    async with runtime.recompute_sem:
        try:
            report = await asyncio.wait_for(
                asyncio.to_thread(
                    run_recompute,
                    settings.kafka_bootstrap,
                    run_id,
                    recipes,
                    cfg,
                    timeout_s=settings.replay_timeout_s,
                ),
                timeout=settings.replay_timeout_s * 3,
            )
        except NoStoredSnapshot:
            raise
        except Exception as exc:  # noqa: BLE001 - never echo broker/timeout detail (T-05-33)
            raise HTTPException(status_code=502, detail="Replay failed") from exc
    return report


@router.post("/api/station/{name}/break")
async def station_break(
    name: str, runtime: RuntimeDep, body: BreakRequest | None = None
) -> dict[str, Any]:
    return await runtime.controller.break_station(name, body)


@router.post("/api/station/{name}/repair")
async def station_repair(name: str, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.repair_station(name)


@router.get("/api/lot/{order_id}/eta")
async def lot_eta(
    order_id: Annotated[str, PathParam(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")],
    runtime: RuntimeDep,
) -> dict[str, Any]:
    return await runtime.lot_eta(order_id)


@router.get("/api/lot/{order_id}")
async def lot_detail(
    order_id: Annotated[str, PathParam(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")],
    runtime: RuntimeDep,
) -> dict[str, Any]:
    detail = runtime.lot_detail(order_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="unknown lot")
    return detail


@router.post("/api/lot/{order_id}/bank")
async def lot_bank(order_id: str, body: BankRequest, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.bank_lot(order_id, body)


@router.post("/api/lot/{order_id}/scrap")
async def lot_scrap(order_id: str, body: ScrapRequest, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.scrap_lot(order_id, body)


@router.post("/api/lot/{order_id}/priority")
async def lot_priority(
    order_id: str, body: PriorityRequest, runtime: RuntimeDep
) -> dict[str, Any]:
    return await runtime.controller.set_lot_priority(order_id, body)


@router.post("/api/lot/{order_id}/release")
async def lot_release(order_id: str, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.release_lot(order_id)


@router.post("/api/lot/{order_id}/hold/cancel")
async def lot_hold_cancel(order_id: str, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.cancel_hold_lot(order_id)


@router.get("/api/failure-config")
async def failure_config(runtime: RuntimeDep) -> dict[str, Any]:
    s = runtime.settings
    return {
        "mtbf_s": s.mtbf_s,
        "mttr_s": s.mttr_s,
        "rework_limits": dict(s.rework_limits),
        "station_queue_limit": s.station_queue_limit,
        "bank_after_s": s.bank_after_s,
        "bank_wait_threshold": s.bank_wait_threshold,
        "heartbeat_interval_s": s.heartbeat_interval_s,
    }


@router.post("/api/order")
async def place_order(body: OrderRequest, runtime: RuntimeDep) -> dict[str, Any]:
    return await runtime.controller.place_order(body)


@router.get("/api/order/estimate")
async def estimate_order(
    request: Request,
    query: Annotated[EstimateRequest, Query()],
    runtime: RuntimeDep,
) -> dict[str, Any]:
    """Dry-run ETA for a hypothetical order (read-only: no order, no sequence number)."""
    unknown = sorted(set(request.query_params.keys()) - set(EstimateRequest.model_fields))
    if unknown:
        raise HTTPException(status_code=422, detail=f"unknown query parameter: {unknown[0][:40]}")
    return await runtime.controller.estimate_order(query)


@router.get("/", include_in_schema=False)
async def index() -> Any:
    page = STATIC_DIR / "index.html"
    if not page.is_file():
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return FileResponse(page)


# ---- install ---------------------------------------------------------------------


def _sanitize(errors: list[Any]) -> str:
    parts = []
    for err in errors[:5]:
        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "body")
        parts.append(
            f"{loc}: {err.get('msg', 'invalid')}" if loc else str(err.get("msg", "invalid"))
        )
    return "; ".join(parts) or "invalid request"


def install(app: FastAPI) -> None:
    """Attach router, static files, exception mapping and the security middleware."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    @app.exception_handler(UnknownStationError)
    async def _unknown_station(_: Request, exc: UnknownStationError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)[:200]}, status_code=404)

    @app.exception_handler(UnknownLotError)
    async def _unknown_lot(_: Request, exc: UnknownLotError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)[:200]}, status_code=404)

    @app.exception_handler(ConflictError)
    async def _conflict(_: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(NoStoredSnapshot)
    async def _no_stored_snapshot(_: Request, exc: NoStoredSnapshot) -> JSONResponse:
        return JSONResponse({"detail": "No stored snapshot for this run"}, status_code=404)

    @app.exception_handler(NotAvailableError)
    async def _not_available(_: Request, exc: NotAvailableError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=422)

    @app.exception_handler(RequestValidationError)
    async def _request_invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse({"detail": _sanitize(list(exc.errors()))}, status_code=422)

    @app.exception_handler(ValidationError)
    async def _invalid(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse({"detail": _sanitize(exc.errors())}, status_code=422)

    @app.exception_handler(ValueError)
    async def _value(_: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)[:200]}, status_code=422)

    app.include_router(router)
    app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")
    app.add_middleware(SecurityMiddleware)
