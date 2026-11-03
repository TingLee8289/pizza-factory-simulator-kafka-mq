"""Record Video 1 (3D Digital Twin) with sleek dynamic overlay pill badges.
Generates:
  - recordings/final/1_digital_twin_clean.mp4  (Clean version)
  - recordings/final/1_digital_twin_overlay.mp4 (Badge overlay version)
"""

import os
import shutil
import subprocess
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:8000"
RAW_DIR = Path("recordings/raw/v1_compare")
FINAL_DIR = Path("recordings/final")

RAW_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)


def convert_to_mp4(raw_webm: Path, output_mp4: Path):
    print(f"Encoding to {output_mp4.name} (1080p)...")
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(raw_webm),
            "-c:v",
            "libx264",
            "-preset",
            "slow",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(output_mp4),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"  -> Generated: {output_mp4} ({output_mp4.stat().st_size // 1024} KB)")


def record_digital_twin_overlay(p):
    rec_dir = RAW_DIR / "overlay"
    rec_dir.mkdir(parents=True, exist_ok=True)

    browser = p.chromium.launch(
        channel="msedge",
        headless=True,
        args=["--enable-webgl", "--use-gl=angle", "--use-angle=d3d11"],
    )
    context = browser.new_context(
        record_video_dir=str(rec_dir),
        record_video_size={"width": 1920, "height": 1080},
        viewport={"width": 1920, "height": 1080},
    )
    page = context.new_page()
    page.goto(BASE_URL)
    page.wait_for_timeout(2000)

    # Inject sleek floating pill badge into DOM
    page.evaluate("""() => {
        const badge = document.createElement('div');
        badge.id = 'showcase-pill-badge';
        badge.style.position = 'fixed';
        badge.style.bottom = '36px';
        badge.style.left = '50%';
        badge.style.transform = 'translateX(-50%)';
        badge.style.background = 'rgba(15, 23, 42, 0.88)';
        badge.style.color = '#f8fafc';
        badge.style.padding = '10px 24px';
        badge.style.borderRadius = '9999px';
        badge.style.fontSize = '17px';
        badge.style.fontWeight = '600';
        badge.style.fontFamily = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
        badge.style.letterSpacing = '0.02em';
        badge.style.border = '1px solid rgba(255, 255, 255, 0.2)';
        badge.style.boxShadow = '0 12px 30px rgba(0, 0, 0, 0.5), 0 0 20px rgba(56, 189, 248, 0.2)';
        badge.style.backdropFilter = 'blur(12px)';
        badge.style.zIndex = '99999';
        badge.style.transition = 'all 0.4s ease';
        badge.style.display = 'flex';
        badge.style.alignItems = 'center';
        badge.style.gap = '10px';
        badge.innerHTML = '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#38bdf8;box-shadow:0 0 8px #38bdf8;"></span><span>Three.js 3D Digital Twin • Interactive Orbit</span>';
        document.body.appendChild(badge);
        
        window.setOverlayText = (dotColor, text) => {
            badge.style.opacity = '0';
            badge.style.transform = 'translateX(-50%) translateY(8px)';
            setTimeout(() => {
                badge.innerHTML = `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${dotColor};box-shadow:0 0 8px ${dotColor};"></span><span>${text}</span>`;
                badge.style.opacity = '1';
                badge.style.transform = 'translateX(-50%) translateY(0)';
            }, 250);
        };
    }""")

    # 1. Start simulation if not running
    start_btn = page.locator("#empty-start")
    if start_btn.is_visible():
        start_btn.click()
        page.wait_for_timeout(500)
        page.fill("#start-rate", "4.0")
        page.click("#start-form button[type='submit']")
        page.wait_for_timeout(2000)

    speed4 = page.locator("#speed-switch button[data-scale='4']")
    if speed4.is_visible():
        speed4.click()

    # Place initial orders
    for _ in range(2):
        page.click("#btn-order")
        page.wait_for_timeout(300)
        page.click("#order-form button[type='submit']")
        page.wait_for_timeout(500)

    page.wait_for_timeout(1000)

    # Orbit camera in 3D
    page.mouse.move(960, 500)
    page.mouse.down()
    for dx in range(0, 320, 16):
        page.mouse.move(960 + dx, 500 - int(dx * 0.18))
        page.wait_for_timeout(35)
    page.mouse.up()
    page.wait_for_timeout(800)

    # Switch Badge 2: 7-Station Continuous Flow
    page.evaluate(
        "window.setOverlayText('#4ade80', '7-Station Continuous Line • Real-time Lot Flow')"
    )

    page.click("#tool-zoom-in")
    page.wait_for_timeout(600)
    page.click("#tool-zoom-in")
    page.wait_for_timeout(1000)

    # Rotate slightly back
    page.mouse.move(1120, 480)
    page.mouse.down()
    for dx in range(0, 220, 16):
        page.mouse.move(1120 - dx, 480 + int(dx * 0.15))
        page.wait_for_timeout(35)
    page.mouse.up()
    page.wait_for_timeout(1000)

    # Switch Badge 3: Real-time IE Performance Metrics
    page.evaluate(
        "window.setOverlayText('#f59e0b', 'Real-time IE Performance Metrics • Little\\'s Law')"
    )
    page.hover("#kpi-throughput")
    page.wait_for_timeout(1500)
    page.hover("#kpi-cycle")
    page.wait_for_timeout(1500)

    page.click("#tool-home")
    page.wait_for_timeout(2000)

    context.close()
    browser.close()

    webms = list(rec_dir.glob("*.webm"))
    if webms:
        convert_to_mp4(webms[-1], FINAL_DIR / "1_digital_twin_overlay.mp4")


if __name__ == "__main__":
    # Ensure clean version exists
    src_clean = FINAL_DIR / "1_digital_twin_3d.mp4"
    dst_clean = FINAL_DIR / "1_digital_twin_clean.mp4"
    if src_clean.exists() and not dst_clean.exists():
        shutil.copy(src_clean, dst_clean)
        print(f"Prepared clean version: {dst_clean}")

    with sync_playwright() as p:
        record_digital_twin_overlay(p)
