"""Automated video recording for the Pizza Factory Simulator portfolio showcase.

Required videos (1080p H.264 MP4, written to recordings/final):
  1  digital_twin  - 3D digital twin with overlay badges
  2  dual_broker   - bank flow: RabbitMQ command (scheduler.control) vs Kafka lot.banked fact
  3  vip_priority  - VIP priority overtake
  4  recipe_edit   - live recipe edit with ETA

Optional extras (never part of `all`): dispatch, chaos.

Usage:
  python scripts/record_showcase.py 1|2|3|4|all|dispatch|chaos [--no-reset] [--restart] [--dry-run]
  python scripts/record_showcase.py --verify [--only N]

Capture backend: Playwright built-in video (headless Edge), re-encoded with ffmpeg.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx

BASE_URL = "http://127.0.0.1:8000"
KAFKA_UI_URL = "http://127.0.0.1:8080"
RABBITMQ_URL = "http://127.0.0.1:15672"
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "pizza")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "pizza")

OUTPUT_DIR = Path("recordings")
RAW_DIR = OUTPUT_DIR / "raw"
FINAL_DIR = OUTPUT_DIR / "final"

# scenario key -> (min, max) seconds of final video
DURATION_RANGES = {"1": (28, 46), "2": (28, 36), "3": (18, 20), "4": (18, 22)}
TOLERANCE_S = 1.0

FINAL_NAMES = {
    "1": "1_digital_twin_overlay.mp4",
    "2": "2_dual_broker_messaging.mp4",
    "3": "3_vip_priority_overtake.mp4",
    "4": "4_live_recipe_eta.mp4",
}

CHROMIUM_ARGS = ["--enable-webgl", "--use-gl=angle", "--use-angle=d3d11"]
VIEWPORT = {"width": 1920, "height": 1080}


# ---------------------------------------------------------------- helpers


# One pooled client: a fresh connection per call costs ~0.7 s on Windows, which would
# stretch the timed browser takes (trust_env off skips proxy lookup for localhost).
_HTTP = httpx.Client(timeout=10.0, trust_env=False)


def api(method: str, path: str, json: dict | None = None):
    """Call the dashboard REST API; raise on HTTP errors."""
    resp = _HTTP.request(method, BASE_URL + path, json=json)
    resp.raise_for_status()
    if resp.content:
        try:
            return resp.json()
        except ValueError:
            return resp.text
    return None


def wait_until(pred, timeout: float, interval: float = 0.5) -> bool:
    """Poll pred() until truthy; raise TimeoutError after timeout seconds."""
    deadline = time.monotonic() + timeout
    while True:
        if pred():
            return True
        if time.monotonic() >= deadline:
            raise TimeoutError(f"condition not met within {timeout}s")
        time.sleep(interval)


REPO_ROOT = Path(__file__).resolve().parents[1]
RESTART_TIMEOUT_S = 300.0


def _compose(*args: str) -> subprocess.CompletedProcess:
    """Run `docker compose <args>` in the repo root (isolated so tests can mock it)."""
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=240,
    )


def compose_unhealthy(ps_stdout: str) -> list[str]:
    """Service names from `compose ps --format json` output that are not up/healthy yet."""
    bad: list[str] = []
    rows = 0
    for line in ps_stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        for rec in data if isinstance(data, list) else [data]:
            rows += 1
            if rec.get("State") != "running" or rec.get("Health") not in ("", None, "healthy"):
                bad.append(str(rec.get("Service") or rec.get("Name")))
    if rows == 0:
        bad.append("(no containers)")
    return bad


def restart_stack(timeout_s: float = RESTART_TIMEOUT_S) -> float:
    """`docker compose restart` (non-destructive), then wait for healthy services + /api/state.

    Returns seconds until everything answered. Raises RuntimeError on failure/timeout.
    """
    t0 = time.monotonic()
    print("restarting compose stack (docker compose restart)...")
    res = _compose("restart")
    if res.returncode != 0:
        raise RuntimeError(f"docker compose restart failed: {res.stderr.strip()[:500]}")
    last = {"bad": ["?"]}

    def _healthy() -> bool:
        ps = _compose("ps", "--format", "json")
        if ps.returncode != 0:
            return False
        last["bad"] = compose_unhealthy(ps.stdout)
        if last["bad"]:
            return False
        try:
            return bool(api("GET", "/api/state"))
        except (httpx.HTTPError, ValueError):
            return False

    deadline = time.monotonic() + timeout_s
    while not _healthy():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"stack not healthy after {timeout_s:.0f}s: {last['bad']}")
        print(f"  waiting ({time.monotonic() - t0:.0f}s) for: {', '.join(last['bad'])}")
        time.sleep(3.0)
    took = time.monotonic() - t0
    run = api("GET", "/api/state")["run"]
    print(f"stack healthy after {took:.0f}s; run_id={run.get('run_id')} state={run.get('state')}")
    return took


def build_encode_cmd(raw: Path, out: Path, trim_s: float = 0) -> list[str]:
    """ffmpeg argv: H.264 high profile, yuv420p, faststart, 30 fps, 1080p, no audio."""
    cmd = ["ffmpeg", "-y"]
    if trim_s:
        cmd += ["-ss", f"{trim_s:g}"]
    cmd += [
        "-i", str(raw),
        "-vf", "fps=30,scale=1920:1080:flags=lanczos",
        "-c:v", "libx264", "-preset", "slow", "-crf", "18",
        "-profile:v", "high", "-pix_fmt", "yuv420p",
        "-movflags", "+faststart", "-an",
        str(out),
    ]  # fmt: skip
    return cmd


def encode_mp4(raw: Path, name: str, trim_s: float = 0) -> Path:
    """Encode a raw capture into FINAL_DIR/name."""
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    out = FINAL_DIR / name
    print(f"Encoding {name} (1080p H.264)...")
    res = subprocess.run(build_encode_cmd(raw, out, trim_s), capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)
        raise RuntimeError(f"ffmpeg failed for {name}")
    print(f"  -> {out} ({out.stat().st_size // 1024} KB)")
    return out


class Capture:
    """Handle yielded by capture(); raw_path is set after the block exits."""

    def __init__(self, page, t0: float):
        self.page = page
        self.raw_path: Path | None = None
        self._t0 = t0
        self.trim_s = 0.0

    def mark_start(self) -> None:
        """Call when the scene is ready; everything before it is trimmed at encode."""
        self.trim_s = round(time.monotonic() - self._t0, 2)


@contextlib.contextmanager
def capture(p, name: str, capture: str = "playwright", storage_state=None):
    """Record a browser session; backend-agnostic for scenarios.

    Usage: with capture(p, "v1") as cap: ...; encode_mp4(cap.raw_path, FINAL_NAMES["1"])
    """
    if capture != "playwright":
        raise ValueError(f"unsupported capture backend: {capture}")
    rec_dir = RAW_DIR / name
    shutil.rmtree(rec_dir, ignore_errors=True)
    rec_dir.mkdir(parents=True, exist_ok=True)

    browser = p.chromium.launch(channel="msedge", headless=True, args=CHROMIUM_ARGS)
    context = browser.new_context(
        record_video_dir=str(rec_dir),
        record_video_size=VIEWPORT,
        viewport=VIEWPORT,
        device_scale_factor=1,
        http_credentials={"username": RABBITMQ_USER, "password": RABBITMQ_PASS},
        storage_state=str(storage_state) if storage_state else None,
    )
    t0 = time.monotonic()
    page = context.new_page()
    cap = Capture(page, t0)
    try:
        yield cap
    finally:
        context.close()
        browser.close()
        webms = sorted(rec_dir.glob("*.webm"), key=lambda f: f.stat().st_mtime)
        cap.raw_path = webms[-1] if webms else None


_OVERLAY_JS = """() => {
    const old = document.getElementById('showcase-pill-badge');
    if (old) old.remove();
    const badge = document.createElement('div');
    badge.id = 'showcase-pill-badge';
    Object.assign(badge.style, {
        position: 'fixed', bottom: '36px', left: '50%',
        transform: 'translateX(-50%)',
        background: 'rgba(15, 23, 42, 0.96)', color: '#f8fafc',
        padding: '14px 32px', borderRadius: '9999px',
        fontSize: '26px', fontWeight: '700',
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        letterSpacing: '0.02em', border: '1px solid rgba(255,255,255,0.2)',
        boxShadow: '0 12px 30px rgba(0,0,0,0.5)', zIndex: '99999',
        transition: 'all 0.4s ease', display: 'flex', alignItems: 'center', gap: '10px',
    });
    document.body.appendChild(badge);
    // Sit ~12px above the dashboard event feed (#feed) so the badge never covers feed rows.
    // Falls back to the fixed 36px offset when the feed is missing or display:none.
    const placeAboveFeed = () => {
        const feed = document.getElementById('feed');
        const rect = feed ? feed.getBoundingClientRect() : null;
        badge.style.bottom = rect && rect.height > 0
            ? (window.innerHeight - rect.top + 12) + 'px'
            : '36px';
    };
    // Re-measure when the feed resizes (its top moves as rows are added) or the window resizes.
    // Disconnect the observer from any previous injection before attaching a new one.
    if (window.__badgeFeedObserver) window.__badgeFeedObserver.disconnect();
    const feedEl = document.getElementById('feed');
    if (feedEl && typeof ResizeObserver === 'function') {
        window.__badgeFeedObserver = new ResizeObserver(placeAboveFeed);
        window.__badgeFeedObserver.observe(feedEl);
    }
    if (window.__badgeResize) window.removeEventListener('resize', window.__badgeResize);
    window.__badgeResize = placeAboveFeed;
    window.addEventListener('resize', placeAboveFeed);
    placeAboveFeed();
    window.setOverlayText = (dotColor, text) => {
        placeAboveFeed();
        badge.style.opacity = '0';
        badge.style.transform = 'translateX(-50%) translateY(8px)';
        setTimeout(() => {
            badge.replaceChildren();
            const dot = document.createElement('span');
            Object.assign(dot.style, {
                display: 'inline-block', width: '14px', height: '14px',
                borderRadius: '50%', background: dotColor,
                boxShadow: '0 0 12px ' + dotColor,
            });
            const label = document.createElement('span');
            label.textContent = text;
            badge.append(dot, label);
            badge.style.opacity = '1';
            badge.style.transform = 'translateX(-50%) translateY(0)';
        }, 250);
    };
}"""


def inject_overlay(page) -> None:
    """Inject the pill badge. Must be re-injected after every cross-origin page.goto."""
    page.evaluate(_OVERLAY_JS)


def set_overlay(page, color: str, text: str) -> None:
    """Change badge dot colour and text (injects the badge if missing)."""
    if not page.evaluate("() => typeof window.setOverlayText === 'function'"):
        inject_overlay(page)
    page.evaluate("([c, t]) => window.setOverlayText(c, t)", [color, text])


# ----------------------------------------------------------- verification


def probe_video(path: Path) -> dict:
    res = subprocess.run(
        [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height,pix_fmt:format=duration",
            "-of", "json", str(path),
        ],
        capture_output=True,
        text=True,
    )  # fmt: skip
    if res.returncode != 0:
        raise RuntimeError(f"ffprobe failed for {path}: {res.stderr.strip()}")
    return parse_probe(res.stdout)


def parse_probe(json_text: str) -> dict:
    data = json.loads(json_text)
    stream = data["streams"][0]
    return {
        "codec": stream["codec_name"],
        "width": int(stream["width"]),
        "height": int(stream["height"]),
        "pix_fmt": stream["pix_fmt"],
        "duration": float(data["format"]["duration"]),
    }


def validate_video(info: dict, lo: float, hi: float) -> list[str]:
    problems = []
    if info["codec"] != "h264":
        problems.append(f"codec {info['codec']} != h264")
    if (info["width"], info["height"]) != (1920, 1080):
        problems.append(f"size {info['width']}x{info['height']} != 1920x1080")
    if info["pix_fmt"] != "yuv420p":
        problems.append(f"pix_fmt {info['pix_fmt']} != yuv420p")
    if not (lo - TOLERANCE_S <= info["duration"] <= hi + TOLERANCE_S):
        problems.append(f"duration {info['duration']:.1f}s outside {lo}-{hi}s (+/-{TOLERANCE_S})")
    return problems


def verify_all(only: str | None = None) -> int:
    keys = [only] if only else list(FINAL_NAMES)
    failed = 0
    for key in keys:
        path = FINAL_DIR / FINAL_NAMES[key]
        if not path.exists():
            print(f"FAIL {path.name}: missing")
            failed += 1
            continue
        try:
            problems = validate_video(probe_video(path), *DURATION_RANGES[key])
        except (RuntimeError, OSError) as exc:
            problems = [str(exc)]
        if problems:
            print(f"FAIL {path.name}: " + "; ".join(problems))
            failed += 1
        else:
            print(f"PASS {path.name}")
    return 1 if failed else 0


# --------------------------------------------------------------- preflight


def preflight() -> None:
    """Fail fast if any service the recordings depend on is down."""
    checks = [
        ("dashboard", BASE_URL + "/api/state", None),
        ("Kafka UI", KAFKA_UI_URL, None),
        ("RabbitMQ management", RABBITMQ_URL + "/api/overview", (RABBITMQ_USER, RABBITMQ_PASS)),
    ]
    for label, url, auth in checks:
        try:
            httpx.get(url, auth=auth, timeout=5.0).raise_for_status()
        except httpx.HTTPError as exc:
            raise SystemExit(f"preflight failed: {label} not reachable at {url} ({exc})") from exc
    print("preflight ok")


# ------------------------------------------------------------- scenarios


def _ensure_running(page) -> None:
    """Start the run from the empty state if the start button is on screen."""
    # count() is used because the button only exists when no run is active.
    if page.locator("#empty-start").count():
        page.click("#empty-start")
        page.wait_for_timeout(500)
        page.fill("#start-rate", "4.0")
        page.click("#start-form button[type='submit']")
        page.wait_for_timeout(2000)


def _speed_4x(page) -> None:
    btn = page.locator("#speed-switch button[data-scale='4']")
    btn.wait_for(timeout=5000)
    btn.click()


def _orbit(page, x0: int, y0: int, dx_max: int, sign: int, slope: float) -> None:
    page.mouse.move(x0, y0)
    page.mouse.down()
    for dx in range(0, dx_max, 16):
        page.mouse.move(x0 + sign * dx, y0 - sign * int(dx * slope))
        page.wait_for_timeout(35)
    page.mouse.up()


def _prepare_run(scale: int) -> int:
    """Make sure the run is live, set time scale, wait for visible flow. Returns prior scale."""
    state = api("GET", "/api/state")
    prior = int(state.get("time_scale") or 1)
    run_state = state["run"]["state"]
    if run_state == "paused":
        api("POST", "/api/run/resume")
    elif run_state != "running":
        api("POST", "/api/run/start", {})
    api("POST", "/api/time_scale", {"scale": scale})

    def _flowing() -> bool:
        stations = api("GET", "/api/state")["line"]["stations"].values()
        active = [s for s in stations if s["queue"] > 0 or s["status"] == "busy"]
        return len(active) >= 3

    wait_until(_flowing, 60)
    return prior


def _wheel(page, x: int, y: int, delta: int, steps: int, pause_ms: int = 150) -> None:
    page.mouse.move(x, y)
    for _ in range(steps):
        page.mouse.wheel(0, delta)
        page.wait_for_timeout(pause_ms)


V1_INTRO_SCALE = 8  # time_scale before the rush order (fast-forward the intro)
V1_FOLLOW_SCALE = 2  # time_scale from order submission to release
V1_ORDER_S = 3.0  # s into the take: open the + Lot popover and place the P1 order
V1_POPOVER_HOLD_MS = 2700  # filled-in popover held before "Place lot" (was 700; +2.0 s)
V1_WAIT_STATIONS = ("toppings", "bake")
V1_FOLLOW_S = 8.5  # follow beat starts (was 6.5; shifted by the 2 s longer popover hold)
V1_WARM_SCALE = 4
V1_WARM_RATE = 6.0  # arrivals/min while building WIP
V1_HOLD_RATE = 3.0  # arrivals/min once WIP is high (line capacity ~4.5/min): WIP holds/drifts down
V1_WIP_MIN = 40  # "relatively high" band for not-done lots
V1_WIP_MAX = 90
V1_WIP_TAKE_MAX = 110  # hard abort before a take: lots3d.js caps 3D models at 120
V1_ATTEMPTS = 3
V1_FOLLOW_MIN_DIFF = 6.0  # mean abs gray diff (0-255) of the scene half after Follow
V1_MIN_LOTS = 6
V1_MIN_STEPS = 3
V1_MIN_SPARK_POINTS = 8


def _at(t0: float, t: float, page) -> None:
    """Sleep until t seconds after the take started."""
    ms = int((t - (time.monotonic() - t0)) * 1000)
    if ms > 0:
        page.wait_for_timeout(ms)


def _lot_rows(page, with_prio: bool = False) -> list[str]:
    """Visible Lots rows as 'id:step' (or 'id:P<prio>') in on-screen order."""
    field = ".lot-prio" if with_prio else ".lot-step"
    prefix = "'P' + " if with_prio else ""
    return page.evaluate(
        "() => [...document.querySelectorAll('#tab-lots .list-row')].map(r => "
        "r.querySelector('.row-name').textContent + ':' + "
        f"{prefix}r.querySelector('{field}').textContent)"
    )


def _prio_of(row: str) -> int:
    return int(row.rsplit(":P", 1)[1])


V1_3D_LOT_CAP = 120  # lots3d.js MAX_LIVE: lots beyond this have no 3D model, so Follow is a no-op


def _follow_state(page) -> str:
    """Evidence that Follow engaged. No screenshots here: they glitch a frame in the recording."""
    page.wait_for_timeout(600)
    title = page.evaluate("() => (document.querySelector('.insp-title')||{}).textContent || null")
    live = len(api("GET", "/api/state")["line"]["lots"])
    cam = (
        "can follow"
        if live < V1_3D_LOT_CAP
        else f"CANNOT follow ({live} lots >= {V1_3D_LOT_CAP} cap)"
    )
    return f"inspector={title} 3D camera {cam}"


def _v1_readiness() -> dict:
    """Snapshot of what Video 1 needs on screen before the take starts."""
    st = api("GET", "/api/state")
    kpi = st.get("kpi") or {}  # null right after a reset / run start
    lots = [lot for lot in (st.get("line") or {}).get("lots") or [] if lot.get("state") != "done"]
    steps = {lot.get("station") for lot in lots if lot.get("station")}
    hist = st.get("history") or []
    return {
        "lots": len(lots),
        "steps": len(steps),
        "completed": int(kpi.get("orders_completed") or 0),
        "tput_pts": sum(1 for h in hist if h.get("pizzas_per_min") is not None),
        "cycle_pts": sum(1 for h in hist if h.get("cycle_mean_s") is not None),
        "warming": bool(kpi.get("warming_up")),
        "rate": (st.get("run") or {}).get("arrival_rate_per_min"),
    }


def _v1_ready(r: dict) -> bool:
    return (
        V1_WIP_MIN <= r["lots"] <= V1_WIP_MAX
        and r["steps"] >= V1_MIN_STEPS
        and r["completed"] >= 3
        and not r["warming"]
        and r["tput_pts"] >= V1_MIN_SPARK_POINTS
        and r["cycle_pts"] >= V1_MIN_SPARK_POINTS
    )


def wip_rate_action(lots: int, rate: float | None) -> float | None:
    """Arrival rate to switch to (None = leave it): build WIP, then hold it under the 3D cap."""
    if lots >= V1_WIP_MAX and (rate is None or rate > V1_HOLD_RATE):
        return V1_HOLD_RATE
    if lots < V1_WIP_MIN and (rate is None or rate < V1_WARM_RATE):
        return V1_WARM_RATE
    return None


class FollowError(RuntimeError):
    """Follow could not engage (lot has no 3D model), so the take would be a bad video."""


def _warm_up_v1(timeout_s: float = 900.0) -> None:
    """Run fast and steer the arrival rate until WIP is moderate-high and the KPIs have data."""
    t_start = time.monotonic()
    state = api("GET", "/api/state")
    if state["run"]["state"] == "paused":
        api("POST", "/api/run/resume")
    elif state["run"]["state"] != "running":
        api("POST", "/api/run/start", {"arrival_rate_per_min": V1_WARM_RATE})
    api("POST", "/api/time_scale", {"scale": V1_WARM_SCALE})
    api("POST", "/api/rate", {"arrival_rate_per_min": V1_WARM_RATE})
    last: dict = {}

    def _ok() -> bool:
        last.update(_v1_readiness())
        new_rate = wip_rate_action(last["lots"], last["rate"])
        if new_rate is not None:
            api("POST", "/api/rate", {"arrival_rate_per_min": new_rate})
            last["rate"] = new_rate
            print(f"  rate -> {new_rate} at wip={last['lots']}")
        print(
            f"  warm-up t={time.monotonic() - t_start:.0f}s wip={last['lots']} "
            f"steps={last['steps']} done={last['completed']} rate={last['rate']}"
        )
        return _v1_ready(last)

    try:
        wait_until(_ok, timeout_s, interval=2.0)
    except TimeoutError as exc:
        raise SystemExit(
            f"warm-up timed out after {timeout_s:.0f}s (need {V1_WIP_MIN}<=WIP<={V1_WIP_MAX}, "
            f">=3 steps, >=3 done, KPI warm, >=8 sparkline points); last: {last}"
        ) from exc
    # Hold arrivals below line capacity so WIP cannot run past the 3D cap during the take.
    api("POST", "/api/rate", {"arrival_rate_per_min": V1_HOLD_RATE})
    wip = _v1_readiness()["lots"]
    if wip >= V1_WIP_TAKE_MAX:
        raise SystemExit(f"abort: {wip} not-done lots >= {V1_WIP_TAKE_MAX}; 3D cap is 120")
    print(
        f"warm-up ready after {time.monotonic() - t_start:.0f}s: {last}; take starts at wip={wip}"
    )


def _mean_abs_diff(raw_a: bytes, raw_b: bytes) -> float:
    n = min(len(raw_a), len(raw_b))
    if n == 0:
        return 0.0
    return sum(abs(x - y) for x, y in zip(raw_a, raw_b, strict=False)) / n


def _gray_frame(video: Path, t: float) -> bytes:
    """Left (3D scene) half of the frame at t seconds as 160x90 gray bytes."""
    cmd = [
        "ffmpeg", "-v", "error", "-ss", f"{t:.2f}", "-i", str(video), "-frames:v", "1",
        "-vf", "crop=iw/2:ih:0:0,scale=160:90,format=gray", "-f", "rawvideo", "-",
    ]  # fmt: skip
    return subprocess.run(cmd, capture_output=True, check=True).stdout


def follow_camera_moved(video: Path) -> tuple[bool, str]:
    """Post-hoc check on the recorded MP4: the scene must change a lot when Follow engages."""
    before = _gray_frame(video, V1_FOLLOW_S - 0.3)
    after = _gray_frame(video, V1_FOLLOW_S + 2.0)
    base = _mean_abs_diff(_gray_frame(video, 1.0), _gray_frame(video, 2.5))
    diff = _mean_abs_diff(before, after)
    ok = diff >= V1_FOLLOW_MIN_DIFF and diff >= 1.5 * base
    return (
        ok,
        f"scene diff across Follow={diff:.1f} (idle baseline {base:.1f}, min {V1_FOLLOW_MIN_DIFF})",
    )


def _take_v1(p, cap_backend: str):
    """One Video 1 take on the warmed-up line. Returns the finished Capture."""
    scale_log: list[tuple[float, int]] = []
    with capture(p, "v1", cap_backend) as cap:
        page = cap.page
        page.goto(BASE_URL)
        page.locator("#scene canvas").wait_for(timeout=15000)
        page.locator("#kpi-throughput").wait_for(timeout=10000)
        page.locator("#tool-home").click()
        page.locator("#tabs-head").click()  # collapse the side panel before the take
        page.wait_for_timeout(1500)
        inject_overlay(page)
        cap.mark_start()
        # Playwright's video clock lags the context clock by ~1 s; keep that much lead-in.
        cap.trim_s = max(0.0, cap.trim_s - 1.0)
        set_overlay(page, "#38bdf8", "3D Digital Twin - 7-Station Line")
        t0 = time.monotonic()
        api("POST", "/api/time_scale", {"scale": V1_INTRO_SCALE})  # fast-forward the intro
        scale_log.append((round(time.monotonic() - t0, 2), V1_INTRO_SCALE))
        _at(t0, V1_ORDER_S, page)
        set_overlay(page, "#f59e0b", "Place a rush lot")
        before = set(_lots_by_id())
        page.locator("#btn-order").click()
        page.locator("#order-form button[type='submit']").wait_for(timeout=5000)
        page.locator("#order-product").select_option("pepperoni")
        page.locator("#order-qty").fill("1")
        page.locator("#order-priority").evaluate(
            """(el, v) => {
                el.value = String(v);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
            }""",
            1,
        )
        shown = page.locator("#order-priority-value").inner_text().strip()
        if shown != "1":
            raise SystemExit(f"priority readout {shown!r} != 1")
        page.wait_for_timeout(V1_POPOVER_HOLD_MS)  # hold the filled-in popover on camera
        page.locator("#order-form button[type='submit']").click()
        api("POST", "/api/time_scale", {"scale": V1_FOLLOW_SCALE})  # slow down: follow from spin
        scale_log.append((round(time.monotonic() - t0, 2), V1_FOLLOW_SCALE))
        page.locator("#toasts > *").first.wait_for(timeout=5000)
        def _mine() -> list[str]:  # background arrivals also appear; ours is the priority-1 lot
            return sorted(
                oid
                for oid, lot in _lots_by_id().items()
                if oid not in before and lot.get("priority") == 1
            )

        wait_until(_mine, 5)
        p1_id = _mine()[0]
        print(f"P1 order placed on camera: {p1_id}")
        _at(t0, V1_FOLLOW_S, page)
        set_overlay(page, "#4ade80", "Follow a lot through the line")
        _open_tab(page, "lots")
        row = page.locator("#tab-lots .list-row", has=page.locator(".row-name", has_text=p1_id))
        if not row.count():  # the click only expanded the panel; select the tab again
            page.locator("button[data-tab='lots']").click()
        row.first.click(timeout=5000)
        print(f"lot selected via the Lots list row {p1_id}")
        page.wait_for_timeout(500)
        # The inspector re-renders every tick; click inside the page so a detach cannot miss.
        page.evaluate(
            "() => [...document.querySelectorAll('.insp-head button')]"
            ".find(b => b.textContent === 'Follow').click()"
        )
        follow_note = _follow_state(page)
        print(f"Follow engaged: {follow_note}")
        if "CANNOT" in follow_note:
            raise FollowError(follow_note)

        # Switch the inspector to Process Detail once the lot waits at toppings/bake (or, as a
        # fallback, when it first reaches bake) and stay there until shipment. No screenshots.
        trigger = None
        deadline = time.monotonic() + 90
        ok = False
        while time.monotonic() < deadline:
            lot = _lots_by_id().get(p1_id) or {}
            if lot.get("state") == "done":
                ok = True
                break
            if trigger is None:
                stn, st = lot.get("station"), lot.get("state")
                if stn in V1_WAIT_STATIONS and st == "queued":
                    trigger = f"waiting (WAIT) at {stn}"
                elif stn == "bake":
                    trigger = f"fallback: reached bake (state={st})"
                if trigger:
                    page.evaluate(
                        "() => [...document.querySelectorAll('.insp-tab')]"
                        ".find(b => b.textContent.includes('Process Detail')).click()"
                    )
                    print(
                        f"Process Detail switch: {trigger}; station={stn} state={st} "
                        f"take t={time.monotonic() - t0:.1f}s"
                    )
            page.wait_for_timeout(250)
        print(f"shipment detected={ok} at take t={time.monotonic() - t0:.1f}s")
        if trigger is None:
            print("WARNING: Process Detail switch never fired")
        page.wait_for_timeout(1000)  # hold on the shipped state
        page.evaluate("() => document.getElementById('showcase-pill-badge').remove()")
        page.locator("#tool-home").click()  # release follow
        page.locator("#tabs-head").click()  # collapse the panel: back to the full view
        page.wait_for_timeout(700)
        print(f"time_scale changes (take t, scale): {scale_log}")
    return cap


def record_digital_twin(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Video 1: on-camera rush (P1) order, follow that lot to shipment (8x intro, 2x follow)."""
    print("\n--- Recording 1: 3D Digital Twin ---")
    # main() already reset the run; it also restores time_scale and leaves the rate at 3.5/min.
    st0 = api("GET", "/api/state")
    prior_scale = st0.get("time_scale") or 1
    print(f"time_scale={prior_scale} run={st0['run']['state']}")
    last_problem = "no attempt ran"
    try:
        for attempt in range(1, V1_ATTEMPTS + 1):
            print(f"\n== take attempt {attempt}/{V1_ATTEMPTS} ==")
            _warm_up_v1()
            try:
                cap = _take_v1(p, cap_backend)
            except FollowError as exc:
                raise SystemExit(f"video 1 failed: Follow did not engage ({exc})") from exc
            if dry_run or cap.raw_path is None:
                return
            out = encode_mp4(cap.raw_path, FINAL_NAMES["1"], cap.trim_s)
            problems = validate_video(probe_video(out), *DURATION_RANGES["1"])
            moved, note = follow_camera_moved(out)
            print(f"follow camera check: moved={moved} ({note})")
            if not moved:
                raise SystemExit(f"video 1 failed: camera did not follow ({note})")
            if not problems:
                return
            last_problem = "; ".join(problems)
            print(f"attempt {attempt} invalid: {last_problem}")
        raise SystemExit(f"video 1 invalid after {V1_ATTEMPTS} attempts: {last_problem}")
    finally:
        api("POST", "/api/time_scale", {"scale": prior_scale})


def _rabbit_storage_state(p, path: Path) -> None:
    """Log in to RabbitMQ management in a NON-recording context; save storage_state."""
    browser = p.chromium.launch(channel="msedge", headless=True)
    try:
        ctx = browser.new_context(viewport=VIEWPORT)
        page = ctx.new_page()
        page.goto(f"{RABBITMQ_URL}/#/queues")
        page.locator("input[name='username']").wait_for(timeout=15000)
        page.fill("input[name='username']", RABBITMQ_USER)
        page.fill("input[name='password']", RABBITMQ_PASS)
        page.locator("input[type='submit']").click()
        page.locator("#queues-name").wait_for(timeout=15000)
        ctx.storage_state(path=str(path))
    finally:
        browser.close()


V2_WARM_RATE = 6.0  # arrivals/min while the line warms up (then POST_RECORDING_RATE)


def _pick_bankable_lot(page) -> str:
    """First lot in the Lots list (DOM order) that is running, else queued. Bank-eligible."""
    visible = page.evaluate(
        "() => [...document.querySelectorAll('#tab-lots .list-row .row-name')]"
        ".map(e => e.textContent)"
    )
    states = {lot["order_id"]: lot["state"] for lot in api("GET", "/api/state")["line"]["lots"]}
    # queued (WAIT) banks at once; a running lot's bank is deferred to the end of its op
    for want in ("queued", "running"):
        for order_id in visible:
            if states.get(order_id) == want:
                return order_id
    raise SystemExit("video 2 failed: no in-progress lot visible in the Lots list to bank")


def record_dual_broker(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Video 2: bank flow (~35 s, 28-36 s accepted).

    0-4   dashboard: click one in-progress lot (inspector opens)
    4-10  inspector Actions -> Bank -> Customer (command published to exchange `control`)
    10-16 RabbitMQ scheduler.control queue: the command is consumed and acked
    16-30 Kafka UI order.lifecycle filtered to this order's lot.banked fact, row expanded
    30-35 dashboard: Lots BANK filter lists the lot; its inspector shows the bank reason
    """
    print("\n--- Recording 2: Bank flow (RabbitMQ command, Kafka fact) ---")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    state_path = RAW_DIR.parent / "test" / "rabbit_storage_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    # A fresh run at the default rate can take minutes to show 3 busy stations; start it at the
    # warm-up rate (rate changes need an active run, so start first), then drop to the default.
    if api("GET", "/api/state")["run"]["state"] not in ("running", "paused"):
        api("POST", "/api/run/start", {"arrival_rate_per_min": V2_WARM_RATE})
    prior_scale = _prepare_run(2)
    api("POST", "/api/rate", {"arrival_rate_per_min": POST_RECORDING_RATE})
    run_id = api("GET", "/api/state")["run"]["run_id"]
    try:
        _rabbit_storage_state(p, state_path)
        with capture(p, "v2", cap_backend, storage_state=state_path) as cap:
            page = cap.page
            page.goto(BASE_URL)
            page.locator("#btn-order").wait_for(timeout=15000)
            page.locator("#tabs [data-tab=lots]").click()
            page.locator("#tab-lots .list-row").first.wait_for(timeout=15000)
            page.wait_for_timeout(1000)
            inject_overlay(page)
            order_id = _pick_bankable_lot(page)
            print(f"bank target: {order_id}")
            cap.mark_start()
            t0 = time.monotonic()
            # 0-4 s: click the in-progress lot on the dashboard
            set_overlay(page, "#f59e0b", "Command: RabbitMQ - lot.bank")
            row = page.locator(
                "#tab-lots .list-row", has=page.locator(".row-name", has_text=order_id)
            ).first
            row.click()  # click() scrolls the row into view and retries across list re-renders
            page.locator("#inspector").wait_for(timeout=5000)
            page.locator(".insp-title", has_text=order_id).wait_for(timeout=8000)
            _at(t0, 4, page)
            # 4-10 s: inspector Actions -> Bank -> Customer
            set_overlay(page, "#f59e0b", "Command: RabbitMQ - issue bank command")
            page.locator(".insp-tab", has_text="Actions").first.click()
            page.locator(".insp-dock button", has_text="Bank").first.click()
            customer = page.locator("[role=menuitem]", has_text="Customer").first
            customer.wait_for(timeout=5000)
            page.wait_for_timeout(1200)
            customer.click()
            # 10-16 s: RabbitMQ scheduler.control queue (ack count). Navigate straight away and
            # wait for the rendered chart: a text-only wait leaves a blank page on screen.
            page.goto(f"{RABBITMQ_URL}/#/queues/%2F/scheduler.control")
            page.get_by_text("Consumer ack").first.wait_for(timeout=15000)
            page.wait_for_timeout(300)
            inject_overlay(page)
            set_overlay(page, "#f59e0b", "Command consumed by scheduler")
            _at(t0, 16, page)
            # 16-30 s: Kafka UI order.lifecycle (lot facts live here, not station.events),
            # filtered to this order's lot.banked fact
            page.goto(f"{KAFKA_UI_URL}/ui/clusters/local/all-topics/order.lifecycle/messages")
            page.locator("table tbody tr td", has_text="event_id").first.wait_for(timeout=20000)
            inject_overlay(page)
            set_overlay(page, "#4ade80", "Fact: Kafka - lot.banked")
            page.get_by_role("button", name="Add Filters").click()
            page.locator("input[placeholder='Enter Name']").wait_for(state="visible", timeout=5000)
            page.locator("textarea").first.click(force=True)  # filter code (CEL editor)
            # Kafbat CEL: the JSON value is bound as record.value (help text in the modal).
            # insert_text, not type(): one paste-like insert, so the editor is not shown typing.
            # run_id too: lot ids restart after each reset, so older runs also have L-0001 banks.
            page.keyboard.insert_text(
                f'record.value.type == "lot.banked" && record.value.order_id == "{order_id}"'
                f' && record.value.run_id == "{run_id}"'
            )
            page.locator("input[placeholder='Enter Name']").fill(f"lot.banked {order_id}")
            page.get_by_role("button", name="Add Filter", exact=True).click()
            # The filtered query is async and the table re-renders while it polls, so find and
            # click the first row's expand toggle inside one page-side loop (retries ~10 s).
            page.evaluate(
                """() => new Promise((resolve, reject) => {
                    const sel = 'table tbody tr td [role=button]';
                    let tries = 0;
                    const timer = setInterval(() => {
                        if (document.querySelector(sel)) {
                            clearInterval(timer);
                            // let the first poll settle: a click during re-render is undone
                            setTimeout(() => {
                                const btn = document.querySelector(sel);
                                if (btn) { btn.click(); resolve(true); }
                                else { reject('expand toggle vanished'); }
                            }, 700);
                        } else if (++tries > 100) {
                            clearInterval(timer); reject('no expand toggle');
                        }
                    }, 100);
                })"""
            )
            page.locator("table tbody").locator("text=lot.banked").first.wait_for(timeout=10000)
            _at(t0, 30, page)
            # 30-35 s: dashboard again; Lots BANK filter lists the lot; its inspector shows the
            # bank reason. (The feed keeps only the 5 newest live lines and resets on reload, so
            # the BANK line is not on screen here.)
            page.goto(BASE_URL)
            page.locator("#tabs [data-tab=lots]").click()
            page.locator("#tab-lots .list-row").first.wait_for(timeout=15000)
            inject_overlay(page)
            set_overlay(page, "#4ade80", "Commands disappear, facts stay")
            page.locator("[data-filter=bank]").first.click()
            banked = page.locator(
                "#tab-lots .list-row", has=page.locator(".row-name", has_text=order_id)
            ).first
            banked.wait_for(timeout=10000)
            banked.click()
            page.locator(".insp-title", has_text=order_id).wait_for(timeout=8000)
            page.locator(".insp-tab", has_text="Process Detail").first.click()
            _at(t0, 35, page)
    finally:
        api("POST", "/api/time_scale", {"scale": prior_scale})
        state_path.unlink(missing_ok=True)
    if dry_run or cap.raw_path is None:
        return
    out = encode_mp4(cap.raw_path, FINAL_NAMES["2"], cap.trim_s)
    problems = validate_video(probe_video(out), *DURATION_RANGES["2"])
    if problems:
        raise SystemExit("video 2 invalid: " + "; ".join(problems))


# ---------------------------------------------------------- VIP scenario helpers

# "direct": inject the VIP straight at P1. "raise": inject at P50, then raise to P1
# through the inspector Priority slider (planning fallback if the ETA drift is too small).
VIP_VARIANT = "raise"
VIP_DRIFT_MIN_S = 5.0
BACKLOG_LOTS = 6


def _iso(ts: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def vip_overtake_ok(vip_eta: dict, backlog_etas: list[dict]) -> bool:
    """True when the VIP's bake.out is earlier than every (non-empty) backlog lot's."""
    if not backlog_etas:
        return False
    try:
        vip_out = _iso(vip_eta["stations"]["bake"]["out"])
        return all(vip_out < _iso(e["stations"]["bake"]["out"]) for e in backlog_etas)
    except (KeyError, TypeError):
        return False


def _lots_by_id() -> dict[str, dict]:
    return {lot["order_id"]: lot for lot in api("GET", "/api/state")["line"]["lots"]}


def queued_at_bake(order_ids: list[str]) -> list[str]:
    """Subset of order_ids waiting (not currently being baked) in the Bake queue."""
    state = api("GET", "/api/state")["line"]
    current = state["stations"]["bake"]["current_lot"]
    lots = {lot["order_id"]: lot for lot in state["lots"]}
    return [
        oid
        for oid in order_ids
        if oid in lots
        and oid != current
        and lots[oid].get("station") == "bake"
        and lots[oid].get("state") == "queued"
    ]


def seed_vip_backlog(
    n: int = BACKLOG_LOTS, timeout: float = 90.0, qty: int = 2, min_queued: int = 3
) -> list[str]:
    """Place n P50 pepperoni lots and wait until >= 3 sit in the Bake queue."""
    ids = [
        api("POST", "/api/order", {"product": "pepperoni", "qty": qty, "priority": 50})["order_id"]
        for _ in range(n)
    ]
    wait_until(lambda: len(queued_at_bake(ids)) >= min_queued, timeout, 0.25)
    return ids


def _vip_drift_s(order_id: str) -> float:
    lot = _lots_by_id()[order_id]
    return abs(_iso(lot["eta_now_ts"]) - _iso(lot["eta_ts"]))


def _check_overtake(vip: str, backlog: list[str]) -> str:
    """Fail the take if the VIP does not beat every backlog lot still queued at Bake."""
    vip_eta = api("GET", f"/api/lot/{vip}/eta")
    remaining = queued_at_bake(backlog)
    print(f"VIP eta stations: {vip_eta.get('stations')}")
    if "bake" not in (vip_eta.get("stations") or {}) or not remaining:
        return "skipped (VIP already past Bake or backlog drained)"
    backlog_eta = [api("GET", f"/api/lot/{oid}/eta") for oid in remaining]
    if not vip_overtake_ok(vip_eta, backlog_eta):
        raise SystemExit("VIP did not overtake the backlog at Bake")
    return f"ok vs {remaining}"


def record_vip_priority(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Video 3: P50 backlog at Bake, P1 VIP injected, inspector shows the ETA change."""
    print("\n--- Recording 3: VIP Priority Injection ---")
    state = api("GET", "/api/state")
    prior_scale = int(state.get("time_scale") or 1)
    if state["run"]["state"] == "paused":
        api("POST", "/api/run/resume")
    elif state["run"]["state"] != "running":
        api("POST", "/api/run/start", {"arrival_rate_per_min": 0.1})
    elif (state["run"].get("arrival_rate_per_min") or 0) > 0.5:
        api("POST", "/api/rate", {"arrival_rate_per_min": 0.1})  # keep random lots off the queue
    api("POST", "/api/time_scale", {"scale": 4})
    vip = None
    try:
        backlog = seed_vip_backlog()
        api("POST", "/api/time_scale", {"scale": 2})  # slower drain while the take runs
        before = set(_lots_by_id())
        print(f"backlog at Bake: {queued_at_bake(backlog)}")
        with capture(p, "v3", cap_backend) as cap:
            page = cap.page
            page.goto(BASE_URL)
            page.locator("#btn-order").wait_for(timeout=15000)
            page.wait_for_timeout(800)
            inject_overlay(page)
            cap.mark_start()
            queued_now = queued_at_bake(backlog)
            if len(queued_now) < 2:
                raise SystemExit(f"backlog drained before injection: {queued_now}")
            set_overlay(page, "#f59e0b", "Backlog P50")
            page.wait_for_timeout(1800)
            page.locator("#btn-order").click()
            page.locator("#order-form button[type='submit']").wait_for(timeout=5000)
            page.locator("#order-product").select_option("pepperoni")
            page.locator("#order-qty").fill("1")
            start_prio = 50 if VIP_VARIANT == "raise" else 1
            page.locator("#order-priority").evaluate(
                """(el, v) => {
                    el.value = String(v);
                    el.dispatchEvent(new Event('input', {bubbles: true}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                }""",
                start_prio,
            )
            shown = page.locator("#order-priority-value").inner_text().strip()
            if shown != str(start_prio):
                raise SystemExit(f"priority readout {shown!r} != {start_prio}")
            page.wait_for_timeout(900)
            page.locator("#order-form button[type='submit']").click()
            page.locator("#toasts > *").first.wait_for(timeout=5000)
            wait_until(lambda: set(_lots_by_id()) - before, 5)
            vip = sorted(set(_lots_by_id()) - before)[0]
            print(f"VIP order: {vip}")
            set_overlay(page, "#ef4444", "VIP P1 injected")
            page.wait_for_timeout(1200)
            page.locator("button[data-tab='lots']").click()
            row = page.locator("#tab-lots .list-row", has_text=vip)
            row.wait_for(timeout=5000)
            page.wait_for_timeout(300)
            row.click()
            page.locator(".insp-tab", has_text="Lot Info").wait_for(timeout=5000)
            if VIP_VARIANT == "raise":
                set_overlay(page, "#ef4444", "Raise to P1 - priority slider")
                page.locator(".insp-tab", has_text="Actions").click()
                page.wait_for_timeout(1000)
                page.locator("#inspector .priority-slider").evaluate(
                    """(el) => {
                        el.value = '1';
                        el.dispatchEvent(new Event('input', {bubbles: true}));
                    }"""
                )
                page.wait_for_timeout(800)
                page.locator("#inspector .priority-edit button", has_text="Apply").click()
                page.locator("#toasts > *").last.wait_for(timeout=5000)
                page.wait_for_timeout(1000)
                page.locator(".insp-tab", has_text="Lot Info").click()
                page.wait_for_timeout(500)
            set_overlay(page, "#38bdf8", "Lot Info: Initial vs Current ETA")
            page.wait_for_timeout(3500)
            drift = _vip_drift_s(vip)  # drift on screen while Lot Info is shown
            insp = page.locator("#inspector").inner_text()
            print(f"on-screen 'vs initial' suffix present: {'vs initial' in insp}")
            overtake = _check_overtake(vip, backlog)
            page.locator(".insp-tab", has_text="Process Detail").click()
            set_overlay(page, "#4ade80", "Process Detail: VIP bypasses the P50 queue")
            page.wait_for_timeout(3500)
            page.locator("button[data-tab='stations']").click()
            page.wait_for_timeout(1600)
        print(f"DRIFT_S={drift:.1f}")
        lot = _lots_by_id().get(vip)
        if lot:
            print(f"VIP initial ETA {lot['eta_ts']} current ETA {lot['eta_now_ts']}")
        print(f"overtake check: {overtake}")
    finally:
        api("POST", "/api/time_scale", {"scale": prior_scale})
    if dry_run or cap.raw_path is None:
        return
    out = encode_mp4(cap.raw_path, FINAL_NAMES["3"], cap.trim_s)
    problems = validate_video(probe_video(out), *DURATION_RANGES["3"])
    if problems:
        raise SystemExit("video 3 invalid: " + "; ".join(problems))


@contextlib.contextmanager
def restored_recipe(station: str, product: str):
    """Yield the original unit_time_s; POST it back on exit, even on exceptions (T-09-09)."""
    original = api("GET", "/api/recipes")["recipes"][station][product]
    try:
        yield original
    finally:
        api(
            "POST",
            f"/api/station/{station}/recipe",
            {"product": product, "unit_time_s": original},
        )
        print(f"restored {station} {product} recipe to {original} s")


def _open_tab(page, name: str) -> None:
    """Select a side-panel tab; clicking the already-active tab would collapse the panel."""
    tab = page.locator(f"button[data-tab='{name}']")
    if tab.get_attribute("aria-selected") != "true":
        tab.click()


RECIPE_NEW_S = 6
RECIPE_BACKLOG_LOTS = 6
RECIPE_BACKLOG_QTY = 3


def _lot_eta_now(order_id: str) -> str | None:
    return _lots_by_id().get(order_id, {}).get("eta_now_ts")


def record_recipe_edit(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Video 4: Station Info -> Recipe edit + Apply -> Actions glimpse; ETA responds."""
    print("\n--- Recording 4: Live Recipe Edit and ETA ---")
    state = api("GET", "/api/state")
    prior_scale = int(state.get("time_scale") or 1)
    if state["run"]["state"] == "paused":
        api("POST", "/api/run/resume")
    elif state["run"]["state"] != "running":
        api("POST", "/api/run/start", {"arrival_rate_per_min": 0.1})
    elif (state["run"].get("arrival_rate_per_min") or 0) > 0.5:
        api("POST", "/api/rate", {"arrival_rate_per_min": 0.1})
    try:
        with restored_recipe("bake", "pepperoni") as original:
            api("POST", "/api/time_scale", {"scale": 4})
            backlog = seed_vip_backlog(RECIPE_BACKLOG_LOTS, qty=RECIPE_BACKLOG_QTY, min_queued=4)
            api("POST", "/api/time_scale", {"scale": 2})
            queued = queued_at_bake(backlog)
            if len(queued) < 2:
                raise SystemExit(f"backlog drained before the take: {queued}")
            target = queued[-1]  # a QUEUED lot, not the one currently baking (Pitfall 5)
            print(f"original bake pepperoni {original} s; target lot {target}")
            with capture(p, "v4", cap_backend) as cap:
                page = cap.page
                page.goto(BASE_URL)
                page.locator("#btn-order").wait_for(timeout=15000)
                page.wait_for_timeout(800)
                inject_overlay(page)
                cap.mark_start()
                # beat 1: Station Info
                set_overlay(page, "#38bdf8", "Station Info: Bake")
                _open_tab(page, "stations")
                bake_row = page.locator("#tab-stations .row-name", has_text="Bake")
                bake_row.wait_for(timeout=5000)
                bake_row.click()
                page.locator(".insp-tab", has_text="Station Info").wait_for(timeout=5000)
                page.wait_for_timeout(2500)
                # beat 2: Recipe edit + Apply
                eta_before = _lot_eta_now(target)
                page.locator(".insp-tab", has_text="Recipe").click()
                set_overlay(
                    page, "#f59e0b", f"Recipe: Bake pepperoni {original:g} s to {RECIPE_NEW_S} s"
                )
                page.wait_for_timeout(1800)
                box = page.locator("input.recipe-input[aria-label='pepperoni seconds at Bake']")
                box.wait_for(timeout=5000)
                box.fill(str(RECIPE_NEW_S))
                page.wait_for_timeout(1000)
                page.locator(".recipe-foot button.btn.primary").click()
                page.locator(
                    "#toasts > *", has_text=f"Bake - pepperoni: {RECIPE_NEW_S} s"
                ).wait_for(timeout=8000)
                set_overlay(page, "#4ade80", "Applied: new jobs use the new time")
                page.wait_for_timeout(2000)
                # beat 3: ETA responds on a queued lot
                _open_tab(page, "lots")
                row = page.locator("#tab-lots .list-row", has_text=target)
                row.wait_for(timeout=5000)
                page.wait_for_timeout(300)
                row.click()
                page.locator(".insp-tab", has_text="Lot Info").wait_for(timeout=5000)
                set_overlay(page, "#38bdf8", "Lot Info: ETA responds")
                page.wait_for_timeout(3000)
                eta_after = _lot_eta_now(target)
                print(f"target {target} Current ETA before {eta_before} after {eta_after}")
                page.locator(".insp-tab", has_text="Process Detail").click()
                set_overlay(page, "#4ade80", "Process Detail: Plan vs recipe")
                page.wait_for_timeout(3000)
                # beat 4: Actions glimpse. Observe only: Break Station is never clicked (T-09-12).
                _open_tab(page, "stations")
                page.locator("#tab-stations .row-name", has_text="Bake").click()
                page.locator(".insp-tab", has_text="Actions").click()
                set_overlay(page, "#ef4444", "Actions: chaos controls (not used)")
                page.wait_for_timeout(2000)
        if dry_run or cap.raw_path is None:
            return
        out = encode_mp4(cap.raw_path, FINAL_NAMES["4"], cap.trim_s)
        problems = validate_video(probe_video(out), *DURATION_RANGES["4"])
        if problems:
            raise SystemExit("video 4 invalid: " + "; ".join(problems))
    finally:
        api("POST", "/api/time_scale", {"scale": prior_scale})


def record_dispatch_compare(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Extra: dispatch rule comparison (FIFO vs SPT vs EDD)."""
    print("\n--- Recording extra: Dispatch Rules ---")
    with capture(p, "dispatch", cap_backend) as cap:
        page = cap.page
        page.goto(BASE_URL)
        page.wait_for_timeout(2000)
        page.click("#rule-switch button[data-rule='edd']")
        page.wait_for_timeout(1200)
        page.click("#rule-switch button[data-rule='spt']")
        page.wait_for_timeout(1200)
        page.click("#btn-compare")
        page.wait_for_timeout(1200)
        page.fill("#compare-n", "8")
        page.fill("#compare-rate", "3.0")
        page.wait_for_timeout(800)
        page.click("#compare-start")
        page.wait_for_timeout(3000)
        _speed_4x(page)
        page.wait_for_timeout(8000)
        page.hover("#little-chip")
        page.wait_for_timeout(2000)
    if dry_run or cap.raw_path is None:
        return
    encode_mp4(cap.raw_path, "extra_dispatch_rules_compare.mp4", cap.trim_s)


def record_resilience_chaos(p, cap_backend: str = "playwright", dry_run: bool = False) -> None:
    """Extra: breakdown, stocker buffering and recovery."""
    print("\n--- Recording extra: Resilience / Chaos ---")
    with capture(p, "chaos", cap_backend) as cap:
        page = cap.page
        page.goto(BASE_URL)
        page.wait_for_timeout(2000)
        _speed_4x(page)
        page.click("#btn-order")
        page.wait_for_timeout(400)
        page.click("#order-form button[type='submit']")
        page.wait_for_timeout(1000)

        bake_row = page.locator("#tab-stations .list-row:has-text('Bake')")
        bake_row.wait_for(timeout=5000)
        bake_row.click()
        page.wait_for_timeout(1200)

        focus_btn = page.locator("#inspector .btn:has-text('Focus')")
        focus_btn.wait_for(timeout=5000)
        focus_btn.click()
        page.wait_for_timeout(1500)

        break_btn = page.locator("#inspector button:has-text('Break Station')")
        break_btn.wait_for(timeout=5000)
        break_btn.click()
        print("Bake station broken")
        page.wait_for_timeout(6000)

        repair_btn = page.locator("#inspector button:has-text('Repair Station')")
        repair_btn.wait_for(timeout=5000)
        repair_btn.click()
        print("Bake station repaired")
        page.wait_for_timeout(6000)
    if dry_run or cap.raw_path is None:
        return
    encode_mp4(cap.raw_path, "extra_resilience_and_recovery.mp4", cap.trim_s)


SCENARIOS = {
    "1": record_digital_twin,
    "2": record_dual_broker,
    "3": record_vip_priority,
    "4": record_recipe_edit,
    "dispatch": record_dispatch_compare,
    "chaos": record_resilience_chaos,
}
ALL_KEYS = ["1", "2", "3", "4"]


# --------------------------------------------------------------------- CLI


POST_RECORDING_RATE = 3.5  # arrivals/min left on the live run after every recording


def run_scenario(fn, p, capture_backend: str, dry_run: bool, reset: bool = True) -> None:
    """Reset the run (default), run one scenario, then leave the arrival rate at 3.5/min.

    time_scale is restored to the value read before the reset/take. The rate is NOT restored.
    """
    prior_scale = int(api("GET", "/api/state").get("time_scale") or 1)
    try:
        if reset:
            api("POST", "/api/run/reset")
            print("NOTICE: live run was reset (auto-reset; --no-reset to skip)")
        fn(p, capture_backend, dry_run)
    finally:
        try:
            api("POST", "/api/time_scale", {"scale": prior_scale})
            api("POST", "/api/rate", {"arrival_rate_per_min": POST_RECORDING_RATE})
            after = api("GET", "/api/state")
            print(
                f"final time_scale={after.get('time_scale')} "
                f"arrival_rate={(after.get('run') or {}).get('arrival_rate_per_min')}"
            )
        except Exception as exc:  # noqa: BLE001 - never mask the scenario's own error
            print(f"WARNING: could not set post-recording state: {exc}")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Record Pizza Factory showcase videos.")
    ap.add_argument(
        "scenario",
        nargs="?",
        choices=[*ALL_KEYS, "all", "dispatch", "chaos"],
        help="video to record; `all` means 1-4 (dispatch/chaos are extras)",
    )
    ap.add_argument("--verify", action="store_true", help="ffprobe-check final MP4s and exit")
    ap.add_argument("--only", choices=ALL_KEYS, help="limit --verify to one video")
    ap.add_argument(
        "--reset",
        action="store_true",
        help="no-op (kept for compatibility): the run is reset before every recording by default",
    )
    ap.add_argument(
        "--no-reset",
        action="store_true",
        help="skip the default POST /api/run/reset before each recording (it discards the run)",
    )
    ap.add_argument(
        "--restart",
        action="store_true",
        help="also `docker compose restart` (non-destructive) before recording; off by default "
        "because the run reset already gives a fresh run",
    )
    ap.add_argument(
        "--no-restart",
        action="store_true",
        help="no-op (restart is opt-in now; kept for compatibility)",
    )
    ap.add_argument("--capture", choices=["playwright"], default="playwright")
    ap.add_argument(
        "--dry-run", action="store_true", help="run the scenario but skip encoding (diagnostics)"
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    if args.verify:
        return verify_all(args.only)
    if not args.scenario:
        ap.error("scenario is required unless --verify is given")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    preflight()
    if args.restart and not args.no_restart:
        restart_stack()

    keys = ALL_KEYS if args.scenario == "all" else [args.scenario]
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        for key in keys:
            run_scenario(SCENARIOS[key], p, args.capture, args.dry_run, reset=not args.no_reset)
    print("\nAll requested recordings finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
