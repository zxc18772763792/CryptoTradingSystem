import re
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
    assert "altcoin-radar-universe-scope" in template_source
    assert "Watchlist 收藏列表" in template_source

    assert "window.__loadAltcoinRadarTabData = loadAltcoinRadarTabData" in radar_js
    assert "function bindAltcoinRadarPage()" in radar_js
    assert "async function loadAltcoinRadarTabData(force = false)" in radar_js
    assert "const universePromise = loadUniverseOptions(force).catch((error) => {" in radar_js
    assert "await universePromise;" in radar_js
    assert "await scanRadar(force);" in radar_js
    # The scan must not wait for the watchlist endpoint (it took 2-8s on a cold start).
    assert "await watchlistPromise" not in radar_js
    assert "loadWatchlist().catch((error) => {" in radar_js
    assert "async function openResearchWorkbench(symbol)" in radar_js
    assert 'data-row-action="research-proposal"' in radar_js
    assert "生成研究提案" in radar_js
    assert "/altcoin/radar/${encodeURIComponent(symbol)}/research-proposal" in radar_js
    assert "researchProposalInFlight" in radar_js
    assert "new URLSearchParams" in radar_js
    assert "universe_scope: controls.universeScope" in radar_js
    assert "include_major=false" in radar_js
    assert "value=\"priority\" selected" in template_source
    assert "'BTC/USDT'," not in radar_js.split("const PRESET_BY_KIND", 1)[0]
    # Cold scans take 16-60s and a forced one ~57s: the old flat 60s limit tripped on both.
    assert "const SCAN_TIMEOUT_MS = 120000;" in radar_js
    assert "const FORCE_SCAN_TIMEOUT_MS = 180000;" in radar_js
    assert "timeoutMs: refresh ? FORCE_SCAN_TIMEOUT_MS : SCAN_TIMEOUT_MS," in radar_js
    assert "async function openAiResearchProposal(response, symbol)" in radar_js
    assert "activateTab('ai-research')" in radar_js
    assert "window.AI?.refreshWorkbench" in radar_js
    assert "async function createPresetAlert(kind, symbol)" in radar_js
    assert "async function recyclePresetAlert(kind, symbol)" in radar_js
    assert "async function recycleAllAlerts(symbol)" in radar_js
    assert "async function loadWatchlist()" in radar_js
    assert "const DEFAULT_WATCHLIST = [" in radar_js
    assert "state.watchlist = symbols.length ? symbols : DEFAULT_WATCHLIST.slice();" in radar_js
    assert "if (!state.watchlist.length) state.watchlist = DEFAULT_WATCHLIST.slice();" in radar_js
    assert "return 'RENDER/USDT';" in radar_js
    assert "async function mutateWatchlist(action, symbol)" in radar_js
    assert "const updated = normalizeSymbols(resp?.symbols || []);" in radar_js
    assert "const activeKinds = alertKindsForRow(row);" in radar_js
    # One alert-type dropdown + one toggle button replace the four per-preset buttons.
    assert 'id="altcoin-radar-alert-kind"' in template_source
    assert 'value="narrative"' in template_source
    assert "btn-altcoin-radar-alert-toggle" in template_source
    assert "btn-altcoin-radar-alert-toggle" in radar_js
    assert "toggleBtn.textContent = activeKinds.has(kind) ? `回收${label}` : `建${label}`;" in radar_js
    assert "btn-altcoin-radar-alert-recycle" in template_source
    assert "btn-altcoin-radar-watchlist-add" in template_source
    assert "btn-altcoin-radar-watchlist-remove" in template_source
    assert "btn-altcoin-radar-watchlist-add-manual" in template_source
    assert "altcoin-radar-watchlist-input" in template_source
    assert "altcoin-radar-watchlist-list" in template_source
    assert "altcoin-radar-universe-summary" in template_source
    assert "altcoin-radar-watchlist-summary" in template_source
    assert "altcoin-radar-action-list" in template_source
    assert "altcoin-radar-action-tags" in template_source
    assert "altcoin-radar-invalidate-list" in template_source
    assert "altcoin-radar-event-timeline" in template_source
    assert "altcoin-radar-table-scroll" in template_source
    assert "altcoin-radar-health-chips" in template_source
    assert "altcoin-radar-warning-list" in template_source
    assert "function normalizeWatchlistSymbolInput(symbol)" in radar_js
    assert "function dedupeRowsBySymbol(rows)" in radar_js
    assert "applyClientFilters(dedupeRowsBySymbol(rows))" in radar_js
    assert "text = `${text}/USDT`;" in radar_js
    assert "function buildSelectionPlaceholder(symbol)" in radar_js
    assert "function shouldKeepSelectedSymbol(rows, symbol)" in radar_js
    assert "function focusWatchlistSymbol(symbol)" in radar_js
    assert "function syncSelectedRankingRow()" in radar_js
    assert "function refreshCurrentScan()" in radar_js
    assert "const ALTCOIN_RADAR_PREFS_KEY" in radar_js
    assert "function applySavedControls()" in radar_js
    assert "function saveControls(extra = {})" in radar_js
    assert "state.detailSeq += 1;" in radar_js
    assert 'data-row-action="clear-filters"' in radar_js
    assert "function clearClientFilters()" in radar_js
    assert "function marketSourceLabel(row)" in radar_js
    assert "function cacheTimelineEvents(symbol, events)" in radar_js
    assert "function getCachedTimelineEvents(symbol)" in radar_js
    assert "renderUniverseManagerLegacy" not in radar_js
    assert "loadUniverseOptionsLegacy" not in radar_js
    assert radar_js.count("function renderUniverseManager()") == 1
    assert radar_js.count("async function loadUniverseOptions(force = false)") == 1

    assert "function formatDerivativesStatus(detailPayload, selected)" in radar_js
    assert "const actionPlan = detailPayload?.action_plan || {}" in radar_js
    assert "'altcoin-radar-action-list'" in radar_js
    assert "['行情源', marketSourceLabel(selected)]" in radar_js
    assert "['行情时间', selectedFreshness.as_of ? fmtDateTime(selectedFreshness.as_of) : '--']" in radar_js
    assert "['衍生品', formatDerivativesStatus(detailPayload, selected)]" in radar_js
    # "no data" must not render as a real-looking zero.
    assert "if (value === null || value === undefined || value === '') return fallback;" in radar_js

    assert ".altcoin-radar-workspace" in style_css
    assert ".altcoin-radar-table" in style_css
    assert ".altcoin-radar-inspector-card" in style_css
    assert ".altcoin-radar-score-strip" in style_css
    assert ".altcoin-radar-watchlist-list" in style_css
    assert ".altcoin-radar-watchlist-chip.is-active" in style_css
    assert ".altcoin-radar-table-scroll" in style_css
    assert ".altcoin-radar-health" in style_css
    assert ".altcoin-radar-table--rank" in style_css
    assert ".altcoin-radar-table--pump" in style_css


def test_altcoin_radar_page_drops_the_removed_controls_and_cards():
    template_source = _read("web/templates/index.html")
    radar_block = template_source.split('id="altcoin-radar"', 1)[1].split('id="arbitrage"', 1)[0]
    radar_js = _read("web/static/js/altcoin_radar.js")
    style_css = _read("web/static/css/style.css")

    # Controls that were broken or pointless: perp mode returned 0 rows without derivatives
    # data, OKX/Gate have no local K-lines, 15m/1h cold scans outlive any request timeout.
    for removed in (
        "altcoin-radar-mode-btns",
        "altcoin-mode-btn",
        "altcoin-radar-exchange",
        "altcoin-radar-timeframe",
        "altcoin-radar-only-alerted",
        "altcoin-radar-exclude-retired",
        "altcoin-radar-universe\"",
        "kol-consensus",
        "lsr-crowding",
        "altcoin-radar-summary-strip",
        "altcoin-radar-meta-list",
        "altcoin-radar-output",
        "altcoin-perp-",
        "altcoin-narrative-",
        "altcoin-radar-proxy-components",
        "altcoin-radar-chain-components",
        "altcoin-radar-key-metrics",
        "altcoin-radar-related-list",
        "altcoin-radar-theme-peers",
        "altcoin-radar-ignition-summary",
        "altcoin-radar-page-head",
    ):
        assert removed not in radar_block, removed
    assert 'value="okx"' not in radar_block and 'value="gate"' not in radar_block
    assert 'value="15m"' not in radar_block and 'value="1h"' not in radar_block

    for removed in (
        "loadKolConsensus",
        "loadLsrCrowding",
        "refreshOperatingModeBanner",
        "renderOperatingModeBanner",
        "RADAR_MODES",
        "altcoin-radar-mode-btns",
        "setOutput(",
        "altcoin-radar-output",
        "'Long/Short'",
    ):
        assert removed not in radar_js, removed
    # The scan is pinned to the only configuration that has data behind it.
    assert "exchange: 'binance'," in radar_js
    assert "timeframe: '4h'," in radar_js

    # The weekly table lost its constant columns (惯犯 was 15/15 "是", 30 日解锁 0/15 filled).
    pump_block = radar_block.split('id="pump-watchlist-card"', 1)[1].split("</thead>", 1)[0]
    assert "惯犯" not in pump_block
    assert "30日解锁" not in pump_block
    assert "30日波动" not in pump_block
    assert len(re.findall(r"<th[\s>]", pump_block)) == 9

    rank_block = radar_block.split("altcoin-radar-table--rank", 1)[1].split("</thead>", 1)[0]
    assert len(re.findall(r"<th[\s>]", rank_block)) == 6

    # CSS: the three-column control matrix overflowed the 340px rail above 1500px.
    assert "minmax(260px, 0.72fr) minmax(520px, 1.45fr)" not in style_css
    slim_css = style_css.split("ALTCOIN RADAR - SLIM PAGE", 1)[1]
    matrix_rule = slim_css.split(".altcoin-radar-control-matrix {", 1)[1].split("}", 1)[0]
    assert "grid-template-columns: minmax(0, 1fr);" in matrix_rule
    for dead_class in (".altcoin-mode-btn", ".altcoin-radar-mode-btns", ".altcoin-signal-src", ".altcoin-radar-summary-metric"):
        assert dead_class not in style_css, dead_class


def test_news_template_uses_versioned_css_asset():
    template_source = _read("web/templates/news.html")
    template = _render_template(template_source)

    assert "{{ static_asset_url('css/style.css') }}" in template_source
    assert "/static/css/style.css?v=88" not in template_source
    assert static_asset_url("css/style.css") in template
