"""Static sanity checks for the dashboard HUD shell (plan 05-09)."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "src" / "pizza" / "dashboard" / "static"

REQUIRED_IDS = [
    "scene", "hud", "topbar", "brand", "search", "rule-switch", "rate-slider", "rate-value",
    "btn-start", "btn-reset", "btn-order", "live-badge", "run-id", "conn-state",
    "hud-left", "kpi-row", "kpi-throughput", "kpi-wip", "kpi-cycle",
    "little-chip", "inspector", "inspector-close", "inspector-body",
    "tool-strip", "tool-zoom-in", "tool-zoom-out", "tool-rotate-left",
    "tool-rotate-right", "tool-home", "feed", "tabs", "tabs-toggle", "tab-stations",
    "tab-lots",
    "kpi-trend-throughput", "kpi-trend-cycle", "kpi-trend-wip",
    "chart-throughput", "chart-wip", "chart-cycle", "empty-run", "empty-start",
    "modal-backdrop", "start-modal", "start-form", "start-seed", "start-rate", "start-close",
    "reset-modal", "reset-confirm", "reset-cancel", "order-popover", "order-form",
    "order-product", "order-qty", "order-due", "order-priority", "order-priority-value", "toasts", "error-banner",
]

FORBIDDEN_JS = ["innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("]


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    return re.sub(r"(^|\s)//[^\n]*", r"\1", text)


def _js_files() -> list[Path]:
    return sorted((STATIC / "js").glob("*.js"))


def test_index_has_all_required_ids() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    missing = [i for i in REQUIRED_IDS if f'id="{i}"' not in html]
    assert not missing, missing
    assert "importmap" in html
    for rule in ("fifo", "edd", "spt"):
        assert f'data-rule="{rule}"' in html
    for tab in ("stations", "lots"):
        assert f'data-tab="{tab}"' in html
    assert 'data-tab="trends"' not in html
    # 3 in the Start run modal (plan 05-09) + 3 in the Compare setup form
    # (plan 07-11, "same three inputs as Start run").
    assert len(re.findall(r'class="weight-input"', html)) == 6
    assert "No active run" in html
    assert "Press Start to begin" in html


def test_start_modal_random_breakdowns_checkbox() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert '<input id="start-random-breakdowns" type="checkbox">' in html
    assert "Random breakdowns (MTBF/MTTR)" in html
    assert "Off keeps a clean baseline for theory checks. Manual Break always works." in html


def test_no_remote_urls() -> None:
    files = [STATIC / "index.html", STATIC / "theme.css", *_js_files()]
    for f in files:
        body = _strip_comments(f.read_text(encoding="utf-8"))
        assert not re.search(r"https?://", body), f.name


def test_no_unsafe_dom_apis_in_js() -> None:
    for f in _js_files():
        text = f.read_text(encoding="utf-8")
        for token in FORBIDDEN_JS:
            assert token not in text, f"{f.name} contains {token}"


def test_vendor_readme_matches_committed_hash() -> None:
    readme = (STATIC / "vendor" / "README.md").read_text(encoding="utf-8")
    assert "chart.js" in readme and "4.5.1" in readme
    actual = hashlib.sha256((STATIC / "vendor" / "chart.umd.min.js").read_bytes()).hexdigest()
    hashes = re.findall(r"\b[0-9a-f]{64}\b", readme)
    assert actual in hashes


def test_three_vendor_pair_matches_readme() -> None:
    vendor = STATIC / "vendor"
    module = vendor / "three.module.js"
    core = vendor / "three.core.js"
    assert module.is_file() and core.is_file()
    assert "./three.core.js" in module.read_text(encoding="utf-8")
    readme = (vendor / "README.md").read_text(encoding="utf-8")
    assert "0.186.1" in readme
    hashes = re.findall(r"\b[0-9a-f]{64}\b", readme)
    for f in (module, core):
        assert hashlib.sha256(f.read_bytes()).hexdigest() in hashes, f.name
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert "/static/vendor/three.module.js" in html


def test_camera_module_is_pure() -> None:
    text = (STATIC / "js" / "camera.js").read_text(encoding="utf-8")
    assert not re.search(r"^\s*import\b", text, flags=re.M)


def test_speed_switch_has_six_scale_buttons_and_is_wired() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert 'id="speed-switch"' in html
    scales = re.findall(r'<button[^>]*data-scale="(\d+)"', html)
    assert scales == ["1", "2", "4", "8"]
    # No button may ship disabled: the scale is valid in every run state.
    block = html[html.index('id="speed-switch"') : html.index("</div>", html.index('id="speed-switch"'))]
    assert "disabled" not in block
    assert '"speed"' in (STATIC / "js" / "main.js").read_text(encoding="utf-8")
    assert "#speed-switch button.active" in (STATIC / "controls.css").read_text(encoding="utf-8")


def test_no_backdrop_filter_with_position_fixed() -> None:
    css = re.sub(r"/\*.*?\*/", "", (STATIC / "theme.css").read_text(encoding="utf-8"), flags=re.S)
    for block in re.findall(r"\{([^{}]*)\}", css):
        assert not ("backdrop-filter" in block and re.search(r"position:\s*fixed", block))


def test_priority_slider_replaces_rush() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert 'id="order-rush"' not in html
    assert re.search(r'<input id="order-priority" type="range" min="1" max="99" step="1" value="50">', html)
    assert "Rush orders always go first" not in html
    assert "smaller number first" in html
    compare = (STATIC / "js" / "compare.js").read_text(encoding="utf-8")
    assert "Rush orders always go first" not in compare
    assert "smaller number first" in compare
    prio = (STATIC / "js" / "priority.js").read_text(encoding="utf-8")
    assert "HIGH_PRIORITY_MAX = 10" in prio
