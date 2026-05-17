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

    assert "{{ static_asset_url('css/style.css') }}" in template_source
    assert static_asset_url("css/style.css") == f"/static/css/style.css?v={ASSET_VERSIONS['css/style.css']}"
    assert static_asset_url("css/style.css") in template
    assert "{{ static_asset_url('js/altcoin_radar.js') }}" in template_source
    assert static_asset_url("js/altcoin_radar.js") == f"/static/js/altcoin_radar.js?v={ASSET_VERSIONS['js/altcoin_radar.js']}"
    assert static_asset_url("js/altcoin_radar.js") in template

    assert "async function loadAltcoinRadarTabBridge" in app_js
    assert "'altcoin-radar':()=>loadAltcoinRadarTabBridge(false)" in app_js
    assert "window.bindAltcoinRadarPage==='function'" in app_js
    assert "扫描范围" in template_source
    assert "研究清单（使用下方多选）" in template_source
    assert "Watchlist 收藏列表" in template_source

    assert "window.__loadAltcoinRadarTabData = loadAltcoinRadarTabData" in radar_js
    assert "function bindAltcoinRadarPage()" in radar_js
    assert "async function loadAltcoinRadarTabData(force = false)" in radar_js
    assert "const universePromise = loadUniverseOptions(force).catch((error) => {" in radar_js
    assert "await universePromise;" in radar_js
    assert "await watchlistPromise;" in radar_js
    assert "await scanRadar(force);" in radar_js
    assert "async function openResearchWorkbench(symbol)" in radar_js
    assert 'data-row-action="research-proposal"' in radar_js
    assert "生成研究提案" in radar_js
    assert "/altcoin/radar/${encodeURIComponent(symbol)}/research-proposal" in radar_js
    assert "researchProposalInFlight" in radar_js
    assert "new URLSearchParams" in radar_js
    assert "universe_scope: controls.universeScope" in radar_js
    assert "timeoutMs: 60000" in radar_js
    assert "async function openAiResearchProposal(response, symbol)" in radar_js
    assert "activateTab('ai-research')" in radar_js
    assert "window.AI?.refreshWorkbench" in radar_js
    assert "altcoin-radar-operating-mode-banner" in radar_js
    assert "/ai/operating-mode" in radar_js
    assert "Operating Mode" in radar_js
    assert "async function createPresetAlert(kind, symbol)" in radar_js
    assert "async function recyclePresetAlert(kind, symbol)" in radar_js
    assert "async function recycleAllAlerts(symbol)" in radar_js
    assert "async function loadWatchlist()" in radar_js
    assert "async function mutateWatchlist(action, symbol)" in radar_js
    assert "const activeKinds = alertKindsForRow(row);" in radar_js
    assert "button.textContent = activeKinds.has(kind) ? `回收${label}` : `建${label}`;" in radar_js
    assert "btn-altcoin-radar-alert-anomaly" in radar_js
    assert "btn-altcoin-radar-alert-narrative" in template_source
    assert "btn-altcoin-radar-alert-recycle" in template_source
    assert "btn-altcoin-radar-watchlist-add" in template_source
    assert "btn-altcoin-radar-watchlist-remove" in template_source
    assert "btn-altcoin-radar-watchlist-add-manual" in template_source
    assert "altcoin-radar-watchlist-input" in template_source
    assert "altcoin-radar-watchlist-list" in template_source
    assert "altcoin-radar-universe-summary" in template_source
    assert "altcoin-radar-custom-universe-group" in template_source
    assert "altcoin-radar-watchlist-summary" in template_source
    assert "altcoin-radar-ignition-summary" in template_source
    assert "altcoin-radar-action-list" in template_source
    assert "altcoin-radar-action-tags" in template_source
    assert "altcoin-radar-table-scroll" in template_source
    assert "altcoin-radar-theme-summary" in template_source
    assert "altcoin-radar-related-list" in radar_js
    assert "altcoin-radar-universe" in radar_js
    assert "function normalizeWatchlistSymbolInput(symbol)" in radar_js
    assert "function buildSelectionPlaceholder(symbol)" in radar_js
    assert "function shouldKeepSelectedSymbol(rows, symbol)" in radar_js
    assert "function focusWatchlistSymbol(symbol)" in radar_js
    assert "function syncSelectedRankingRow()" in radar_js
    assert "function refreshCurrentScan()" in radar_js
    assert "function cacheTimelineEvents(symbol, events)" in radar_js
    assert "function getCachedTimelineEvents(symbol)" in radar_js
    assert "renderUniverseManagerLegacy" not in radar_js
    assert "loadUniverseOptionsLegacy" not in radar_js
    assert radar_js.count("function renderUniverseManager()") == 1
    assert radar_js.count("async function loadUniverseOptions(force = false)") == 1

    assert "function formatDerivativesStatus(detailPayload, selected)" in radar_js
    assert "const actionPlan = detailPayload?.action_plan || {}" in radar_js
    assert "'altcoin-radar-action-list'" in radar_js
    assert "['Derivatives 来源', String(derivativesContext.source_name || '--')]" in radar_js
    assert "['Long/Short', shortNumber(metrics.long_short_ratio)]" in radar_js
    assert "['Derivatives 错误', derivativesError || '--']" in radar_js

    assert ".altcoin-radar-workspace" in style_css
    assert ".altcoin-radar-table" in style_css
    assert ".altcoin-radar-inspector-card" in style_css
    assert ".altcoin-radar-score-strip" in style_css
    assert ".altcoin-radar-watchlist-list" in style_css
    assert ".altcoin-radar-watchlist-chip.is-active" in style_css
    assert ".altcoin-radar-table-scroll" in style_css
