"""Dashboard auto-pause on HostSleepDetected (quick-261022-wsj); no brokers."""

import asyncio
from datetime import UTC, datetime, timedelta

from pizza.common.envelope import HostSleepDetected
from pizza.dashboard.app import DashboardRuntime
from pizza.dashboard.control import ConflictError
from pizza.dashboard.settings import DashboardSettings

RUN = "run-1"


class FakeController:
    def __init__(self, state="running", run_id=RUN, conflict=False) -> None:
        self.state = state
        self.run_id = run_id
        self.conflict = conflict
        self.pauses: list[float | None] = []

    def run_info(self):
        return {"state": self.state, "run_id": self.run_id}

    async def pause(self, slept_s=None):
        if self.conflict:
            raise ConflictError("locked")
        self.pauses.append(slept_s)
        self.state = "paused"


def _runtime(**kw) -> tuple[DashboardRuntime, FakeController]:
    rt = DashboardRuntime(DashboardSettings())
    ctl = FakeController(**kw)
    rt.controller = ctl  # type: ignore[assignment]
    rt.replaying = False
    return rt, ctl


def _msg(run_id=RUN, slept_s=1027.0, age_s=0.0) -> HostSleepDetected:
    return HostSleepDetected.make(
        run_id=run_id,
        machine_id="generator",
        slept_s=slept_s,
        ts=datetime.now(UTC) - timedelta(seconds=age_s),
    )


def test_pauses_exactly_once_and_sets_banner():
    async def go():
        rt, ctl = _runtime()
        await rt.on_line_event(_msg())
        await rt.on_line_event(_msg())  # controller already paused now
        assert ctl.pauses == [1027.0]
        assert rt.host_sleep is not None and rt.host_sleep["slept_s"] == 1027.0
        assert rt.tick_payload()["host_sleep"] == rt.host_sleep
        assert rt.snapshot_payload()["host_sleep"] == rt.host_sleep

    asyncio.run(go())


def test_not_paused_when_replaying_stale_wrong_run_or_state():
    async def go():
        rt, ctl = _runtime()
        rt.replaying = True
        await rt.on_line_event(_msg())
        rt.replaying = False
        await rt.on_line_event(_msg(age_s=60))
        await rt.on_line_event(_msg(run_id="other"))
        for state in ("none", "paused"):
            ctl.state = state
            await rt.on_line_event(_msg())
        assert ctl.pauses == []
        assert rt.host_sleep is None

    asyncio.run(go())


def test_conflict_is_swallowed_without_banner():
    async def go():
        rt, ctl = _runtime(conflict=True)
        await rt.on_line_event(_msg())
        assert ctl.pauses == []
        assert rt.host_sleep is None

    asyncio.run(go())


def test_banner_cleared_on_resume_and_reset():
    async def go():
        rt, _ctl = _runtime()
        for event in ("resumed", "reset"):
            rt.host_sleep = {"slept_s": 5.0, "detected_ts": "x", "run_id": RUN}
            await rt.on_change(event, {})
            assert rt.host_sleep is None

    asyncio.run(go())
