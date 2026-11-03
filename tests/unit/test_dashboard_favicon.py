"""Favicon wiring for the dashboard shell."""

from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "src" / "pizza" / "dashboard" / "static"


def test_index_links_favicon_assets() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert 'rel="icon"' in html
    assert "/static/favicon.svg" in html
    assert "/static/favicon.ico" in html
    assert "/static/apple-touch-icon.png" in html


def test_favicon_assets_exist_and_are_non_empty() -> None:
    for name in ("favicon.svg", "favicon.ico", "apple-touch-icon.png"):
        path = STATIC / name
        assert path.is_file(), name
        assert path.stat().st_size > 0, name


def test_svg_favicon_is_self_contained() -> None:
    svg = (STATIC / "favicon.svg").read_text(encoding="utf-8")
    assert svg.lstrip().startswith("<svg")
    assert 'viewBox="0 0 256 256"' in svg
    # CSP for the dashboard only allows images from 'self' and data: URIs.
    assert "https://" not in svg
    assert "<image" not in svg


def test_ico_favicon_has_ico_header() -> None:
    head = (STATIC / "favicon.ico").read_bytes()[:6]
    reserved, kind, count = (
        int.from_bytes(head[0:2], "little"),
        int.from_bytes(head[2:4], "little"),
        int.from_bytes(head[4:6], "little"),
    )
    assert (reserved, kind) == (0, 1)
    assert count >= 1
