"""Unit tests for scripts/record_showcase.py helpers (no browser, no ffmpeg)."""

import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "record_showcase.py"


def _load():
    spec = importlib.util.spec_from_file_location("record_showcase_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def rs():
    return _load()


def test_import_has_no_side_effects(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _load()
    assert not (tmp_path / "recordings").exists()


def test_build_encode_cmd(rs):
    cmd = rs.build_encode_cmd(Path("in.webm"), Path("out.mp4"))
    assert isinstance(cmd, list)
    assert "libx264" in cmd
    assert "yuv420p" in cmd
    assert "+faststart" in cmd
    assert "-an" in cmd
    assert cmd[cmd.index("-crf") + 1] == "18"


def test_build_encode_cmd_trim(rs):
    cmd = rs.build_encode_cmd(Path("in.webm"), Path("out.mp4"), trim_s=3)
    assert "-ss" in cmd
    assert cmd[cmd.index("-ss") + 1] == "3"


def _probe_json(codec="h264", w=1920, h=1080, pix="yuv420p", dur="19.0"):
    return json.dumps(
        {
            "streams": [{"codec_name": codec, "width": w, "height": h, "pix_fmt": pix}],
            "format": {"duration": dur},
        }
    )


def test_parse_probe(rs):
    info = rs.parse_probe(_probe_json())
    assert info["codec"] == "h264"
    assert info["width"] == 1920
    assert info["height"] == 1080
    assert info["pix_fmt"] == "yuv420p"
    assert info["duration"] == pytest.approx(19.0)


def test_validate_video_ok(rs):
    info = rs.parse_probe(_probe_json())
    assert rs.validate_video(info, 18, 20) == []


def test_validate_video_problems(rs):
    bad_size = rs.parse_probe(_probe_json(w=1280, h=720))
    assert rs.validate_video(bad_size, 18, 20)
    bad_dur = rs.parse_probe(_probe_json(dur="40"))
    assert rs.validate_video(bad_dur, 18, 20)
    bad_codec = rs.parse_probe(_probe_json(codec="vp8"))
    assert rs.validate_video(bad_codec, 18, 20)


def test_duration_ranges(rs):
    assert rs.DURATION_RANGES == {"1": (28, 46), "2": (28, 36), "3": (18, 20), "4": (18, 22)}
    assert rs.TOLERANCE_S > 0
    info = rs.parse_probe(_probe_json(dur=str(20 + rs.TOLERANCE_S - 0.01)))
    assert rs.validate_video(info, 18, 20) == []


def test_wait_until(rs):
    state = {"n": 0}

    def pred():
        state["n"] += 1
        return state["n"] >= 3

    assert rs.wait_until(pred, timeout=2, interval=0.01) is True
    with pytest.raises(TimeoutError):
        rs.wait_until(lambda: False, timeout=0.05, interval=0.01)


def test_no_webp():
    text = SCRIPT.read_text(encoding="utf-8").splitlines()
    lines = [ln for ln in text if not ln.lstrip().startswith("#")]
    assert not re.search("webp", "\n".join(lines), re.IGNORECASE)
    assert not list((ROOT / "recordings" / "final").glob("*.webp"))


def test_compose_unhealthy(rs):
    ok = "\n".join(
        json.dumps(r)
        for r in (
            {"Service": "kafka", "State": "running", "Health": "healthy"},
            {"Service": "gen", "State": "running", "Health": ""},
        )
    )
    assert rs.compose_unhealthy(ok) == []
    bad = ok + "\n" + json.dumps({"Service": "rabbit", "State": "running", "Health": "starting"})
    bad += "\n" + json.dumps({"Service": "bake", "State": "restarting", "Health": ""})
    assert rs.compose_unhealthy(bad) == ["rabbit", "bake"]
    assert rs.compose_unhealthy("") == ["(no containers)"]


class _Proc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def test_restart_stack_waits_until_healthy(rs, monkeypatch):
    calls = []
    healthy = json.dumps({"Service": "dashboard", "State": "running", "Health": "healthy"})
    starting = json.dumps({"Service": "dashboard", "State": "running", "Health": "starting"})
    ps_outputs = iter([starting, starting, healthy, healthy])

    def fake_compose(*args):
        calls.append(args)
        if args[0] == "restart":
            return _Proc()
        return _Proc(stdout=next(ps_outputs))

    monkeypatch.setattr(rs, "_compose", fake_compose)
    monkeypatch.setattr(rs.time, "sleep", lambda s: None)
    monkeypatch.setattr(rs, "api", lambda *a, **k: {"run": {"run_id": "r-1", "state": "running"}})
    took = rs.restart_stack(timeout_s=60)
    assert took >= 0
    assert calls[0] == ("restart",)
    assert not any("down" in c or "-v" in c for c in calls)


def test_restart_stack_failure_and_timeout(rs, monkeypatch):
    monkeypatch.setattr(rs, "_compose", lambda *a: _Proc(returncode=1, stderr="boom"))
    with pytest.raises(RuntimeError, match="restart failed"):
        rs.restart_stack(timeout_s=1)

    starting = json.dumps({"Service": "kafka", "State": "running", "Health": "starting"})

    def never_healthy(*args):
        return _Proc() if args[0] == "restart" else _Proc(stdout=starting)

    monkeypatch.setattr(rs, "_compose", never_healthy)
    monkeypatch.setattr(rs.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="not healthy"):
        rs.restart_stack(timeout_s=0)


def test_restart_flag(rs):
    ap = rs.build_parser()
    assert ap.parse_args(["1"]).restart is False  # opt-in: reset already gives a fresh run
    assert ap.parse_args(["1", "--restart"]).restart is True
    assert ap.parse_args(["1", "--no-restart"]).no_restart is True  # compat no-op


def test_reset_flags(rs):
    ap = rs.build_parser()
    assert ap.parse_args(["1"]).no_reset is False  # auto-reset is the default
    assert ap.parse_args(["1", "--no-reset"]).no_reset is True
    assert ap.parse_args(["1", "--reset"]).no_reset is False  # compat alias / no-op
    assert "no-reset" in ap.format_help()


def _fake_api(rs, monkeypatch, fail_scenario=False):
    calls = []

    def fake(method, path, json=None):
        calls.append((method, path, json))
        if path == "/api/state":
            return {"time_scale": 4, "run": {"arrival_rate_per_min": 3.5}}
        return None

    monkeypatch.setattr(rs, "api", fake)
    return calls


def test_run_scenario_resets_then_leaves_rate(rs, monkeypatch):
    calls = _fake_api(rs, monkeypatch)
    seen = []
    rs.run_scenario(lambda p, c, d: seen.append(len(calls)), None, "playwright", False)
    posts = [(c[1], c[2]) for c in calls if c[0] == "POST"]
    assert posts[0] == ("/api/run/reset", None)
    assert seen and calls[seen[0] - 1][1] == "/api/run/reset"  # reset happened before scenario
    assert ("/api/time_scale", {"scale": 4}) in posts
    assert posts[-1] == ("/api/rate", {"arrival_rate_per_min": 3.5})


def test_run_scenario_no_reset_and_failure_still_sets_rate(rs, monkeypatch):
    calls = _fake_api(rs, monkeypatch)

    def boom(p, c, d):
        raise SystemExit("take failed")

    with pytest.raises(SystemExit):
        rs.run_scenario(boom, None, "playwright", False, reset=False)
    posts = [c[1] for c in calls if c[0] == "POST"]
    assert "/api/run/reset" not in posts
    assert calls[-2][:3] == ("POST", "/api/rate", {"arrival_rate_per_min": 3.5})


def test_wip_rate_action(rs):
    assert rs.wip_rate_action(10, 3.0) == rs.V1_WARM_RATE
    assert rs.wip_rate_action(60, 6.0) is None
    assert rs.wip_rate_action(rs.V1_WIP_MAX, 6.0) == rs.V1_HOLD_RATE
    assert rs.wip_rate_action(rs.V1_WIP_MAX + 5, rs.V1_HOLD_RATE) is None
    assert rs.V1_HOLD_RATE <= 3.5
    assert rs.V1_WIP_TAKE_MAX < rs.V1_3D_LOT_CAP


def test_v1_ready_band(rs):
    base = {"lots": 60, "steps": 4, "completed": 5, "warming": False, "tput_pts": 9, "cycle_pts": 9}
    assert rs._v1_ready(base)
    assert not rs._v1_ready({**base, "lots": 30})
    assert not rs._v1_ready({**base, "lots": 95})
    assert not rs._v1_ready({**base, "warming": True})
    assert not rs._v1_ready({**base, "tput_pts": 3})
