"""5 s video-quality probe for the portfolio showcase (Phase 09, plan 01).

Backends:
  playwright  Edge headless, Playwright built-in video (VP8) re-encoded to H.264.
  gdigrab     Headed Edge window captured with ffmpeg gdigrab (comparison only).

Read-only against the dashboard: navigates and clicks the Lots tab.
"""

import argparse
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:8000"
EDGE_ARGS = ["--enable-webgl", "--use-gl=angle", "--use-angle=d3d11"]
X264_ARGS = [
    "-c:v", "libx264", "-preset", "slow", "-crf", "18",
    "-profile:v", "high", "-pix_fmt", "yuv420p",
    "-movflags", "+faststart", "-an",
]


def dashboard_up() -> bool:
    try:
        with urllib.request.urlopen(BASE_URL, timeout=5) as r:
            return r.status == 200
    except Exception:
        return False


def run_ffmpeg(cmd: list[str]) -> None:
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("ffmpeg failed:")
        print(res.stderr[-3000:])
        sys.exit(1)


def open_lots(page) -> None:
    page.goto(BASE_URL)
    page.wait_for_timeout(2000)
    tab = page.locator("button[data-tab='lots']")
    if tab.count() and tab.first.is_visible():
        tab.first.click()


def sample_playwright(out: Path) -> None:
    raw = out / "raw_sample"
    raw.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True, args=EDGE_ARGS)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
            record_video_dir=str(raw),
            record_video_size={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        open_lots(page)
        page.wait_for_timeout(5000)
        page.screenshot(path=str(out / "sample_frame.png"))
        context.close()
        browser.close()
    webms = sorted(raw.glob("*.webm"), key=lambda f: f.stat().st_mtime)
    if not webms:
        print("No webm produced")
        sys.exit(1)
    mp4 = out / "sample_playwright.mp4"
    run_ffmpeg([
        "ffmpeg", "-y", "-i", str(webms[-1]),
        "-vf", "fps=30,scale=1920:1080:flags=lanczos",
        *X264_ARGS, str(mp4),
    ])
    print(f"Wrote {mp4}")


def sample_gdigrab(out: Path) -> None:
    mp4 = out / "sample_gdigrab.mp4"
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="msedge",
            headless=False,
            args=EDGE_ARGS + ["--window-position=0,0", "--window-size=1920,1080"],
        )
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        open_lots(page)
        page.wait_for_timeout(1500)
        proc = subprocess.Popen(
            ["ffmpeg", "-y", "-f", "gdigrab", "-framerate", "30", "-t", "5",
             "-i", "desktop", *X264_ARGS, str(mp4)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        _, err = proc.communicate()
        time.sleep(0.5)
        context.close()
        browser.close()
    if proc.returncode != 0:
        print("ffmpeg gdigrab failed:")
        print(err[-3000:])
        sys.exit(1)
    print(f"Wrote {mp4}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["playwright", "gdigrab"], default="playwright")
    ap.add_argument("--out", default="recordings/test")
    args = ap.parse_args()
    if not dashboard_up():
        print(f"Dashboard not reachable at {BASE_URL}; start the stack first.")
        sys.exit(2)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.backend == "playwright":
        sample_playwright(out)
    else:
        sample_gdigrab(out)


if __name__ == "__main__":
    main()
