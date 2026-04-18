from pathlib import Path

from jinja2 import Environment

from web.asset_versions import ASSET_VERSIONS, static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def test_altcoin_radar_assets_are_wired_into_index_template():
    template_source = _read("web/templates/index.html")
    template = _render_template(template_source)
    app_js = _read("web/static/js/app.js")
    radar_js = _read("web/static/js/altcoin_radar.js")
    style_css = _read("web/static/css/style.css")

    assert "data-tab=\"altcoin-radar\"" in template_source
    assert "id=\"altcoin-radar\"" in template_source
    assert "山寨雷达" in template_source
    assert "btn-altcoin-radar-refresh" in template_source
    assert "altcoin-radar-ranking-body" in template_source
    assert "altcoin-radar-inspector-shell" in template_source

    assert "{{ static_asset_url('js/altcoin_radar.js') }}" in template_source
    assert static_asset_url("js/altcoin_radar.js") == f"/static/js/altcoin_radar.js?v={ASSET_VERSIONS['js/altcoin_radar.js']}"
    assert static_asset_url("js/altcoin_radar.js") in template

    assert "async function loadAltcoinRadarTabBridge" in app_js
    assert "'altcoin-radar':()=>loadAltcoinRadarTabBridge(false)" in app_js
    assert "window.bindAltcoinRadarPage==='function'" in app_js

    assert "window.__loadAltcoinRadarTabData = loadAltcoinRadarTabData" in radar_js
    assert "function bindAltcoinRadarPage()" in radar_js
    assert "async function loadAltcoinRadarTabData(force = false)" in radar_js
    assert "async function openResearchWorkbench(symbol)" in radar_js
    assert "async function createPresetAlert(kind, symbol)" in radar_js
    assert "const hasAlertRule = !!row?.has_alert_rule;" in radar_js
    assert "button.textContent = hasAlertRule ? '已建预警' : defaultLabel;" in radar_js
    assert "btn-altcoin-radar-alert-anomaly" in radar_js
    assert "altcoin-radar-related-list" in radar_js
    assert "altcoin-radar-universe" in radar_js

    assert ".altcoin-radar-workspace" in style_css
    assert ".altcoin-radar-table" in style_css
    assert ".altcoin-radar-inspector-card" in style_css
    assert ".altcoin-radar-score-strip" in style_css
