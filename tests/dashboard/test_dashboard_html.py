"""Static checks on the generated page: no external requests, accessibility, contrast tokens."""

from __future__ import annotations

import re
from pathlib import Path

DASH = Path(__file__).resolve().parents[2] / "dashboard"


# ---------- no external requests ----------
def test_html_makes_no_external_requests(built):
    html = built["html"]
    # strip the embedded JSON (data may legitimately contain anything) before scanning markup/JS
    body = re.sub(r'<script type="application/json".*?</script>', "", html, flags=re.S)
    assert not re.search(
        r"<(script|img|iframe|link|source|video|audio)\b[^>]*\b(src|href)=[\"'](?!data:)",
        body,
        re.I,
    ), (  # noqa: E501
        "external resource reference"
    )
    assert not re.search(r"@import|url\(\s*[\"']?https?:", body, re.I)
    assert not re.search(
        r"\b(fetch|XMLHttpRequest|WebSocket|EventSource|sendBeacon|importScripts)\b", body
    )
    assert not re.search(r"\bimport\s*\(", body)
    # the only absolute URLs allowed are plain navigation anchors
    for m in re.finditer(r"https?://[^\s\"'<>)]+", body):
        before = body[max(0, m.start() - 12) : m.start()]
        if m.group(0) == "http://www.w3.org/2000/svg":
            continue  # XML namespace identifier, never fetched
        assert re.search(r"href=[\"']$", before), f"unexpected URL {m.group(0)}"


def test_no_remote_urls_in_sources():
    for name in ("template.html", "style.css", "app.js"):
        text = (DASH / name).read_text()
        for m in re.finditer(r"https?://[^\s\"';]+", text):
            before = text[max(0, m.start() - 12) : m.start()]
            if m.group(0) == "http://www.w3.org/2000/svg":
                continue  # an XML namespace identifier, never fetched
            assert name == "template.html" and re.search(r"href=[\"']$", before), (name, m.group(0))


# ---------- accessibility ----------
def test_document_landmarks_and_metadata(built):
    html = built["html"]
    assert re.search(r'<html lang="en"', html)
    assert re.search(r"<title>[^<]{5,}</title>", html)
    assert 'name="viewport"' in html
    assert html.count("<h1") == 1
    assert "<main" in html and "<header" in html and "<footer" in html and "<nav" in html
    assert 'class="skip"' in html
    assert 'role="note"' in html  # synthetic-data banner
    assert "Synthetic data" in html
    assert "<noscript>" in html


def test_charts_are_labelled_and_have_table_fallbacks():
    js = (DASH / "app.js").read_text()
    assert 'role: "img"' in js and '"aria-label": label' in js  # every svgRoot is labelled
    assert js.count("svgRoot(") >= 6
    assert js.count("dataTable(") + js.count("dataTableOpen(") >= 10  # table view per chart
    assert "tabindex" in js and "keydown" in js  # keyboard reachable tooltips
    assert "textContent" in js and ".innerHTML" not in js  # data never injected as HTML
    assert "aria-live" in (DASH / "template.html").read_text()


def test_status_never_colour_only():
    js = (DASH / "app.js").read_text()
    # every status() carries an icon and a text label
    assert "function statusEl(kind, text)" in js and 'aria-hidden": "true"' in js


def test_reduced_motion_and_theme_support():
    css = (DASH / "style.css").read_text()
    assert "prefers-reduced-motion" in css
    assert "prefers-color-scheme: dark" in css and '[data-theme="dark"]' in css
    assert "forced-colors" in css


# ---------- contrast tokens ----------
def _tokens(block: str) -> dict[str, str]:
    return dict(re.findall(r"--([\w-]+):\s*(#[0-9a-fA-F]{6})", block))


def _blocks():
    css = (DASH / "style.css").read_text()
    light = css.split("@media (prefers-color-scheme: dark)")[0]
    dark = css[css.index(':root[data-theme="dark"]') :].split("* { box-sizing")[0]
    media_dark = css.split("@media (prefers-color-scheme: dark)")[1].split(
        ':root[data-theme="dark"]'
    )[0]
    return _tokens(light), _tokens(dark), _tokens(media_dark)


def _lum(hex_: str) -> float:
    rgb = [int(hex_[i : i + 2], 16) / 255 for i in (1, 3, 5)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def contrast(a: str, b: str) -> float:
    la, lb = sorted((_lum(a), _lum(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def test_dark_scopes_agree():
    _, dark, media_dark = _blocks()
    for k, v in media_dark.items():
        assert dark[k] == v, f"--{k} differs between the media query and [data-theme=dark]"


def test_text_contrast_meets_wcag_aa():
    light, dark, _ = _blocks()
    for name, t in (("light", light), ("dark", {**light, **dark})):
        for fg in ("text", "text-2", "muted", "accent", "good-text", "crit-text"):
            for bg in ("surface", "bg"):
                assert contrast(t[fg], t[bg]) >= 4.5, (name, fg, bg, contrast(t[fg], t[bg]))
        assert contrast(t["banner-text"], t["banner-bg"]) >= 4.5, name


def test_chart_marks_meet_non_text_contrast():
    light, dark, _ = _blocks()
    for name, t in (("light", light), ("dark", {**light, **dark})):
        for series in ("series-1", "series-2"):  # headline series; both are also direct-labelled
            assert contrast(t[series], t["surface"]) >= 3.0, (name, series)
        # series-1 vs series-2 are separated by hue (CVD-validated, see docs/dashboard.md),
        # direct labels, legend and the table view, not by lightness.
