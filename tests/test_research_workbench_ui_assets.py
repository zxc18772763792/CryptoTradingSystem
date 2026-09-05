from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_research_workbench_module_actions_share_primary_style():
    template = _read("web/templates/index.html")
    style_css = _read("web/static/css/style.css")
    workbench_js = _read("web/static/js/research_workbench.js")
    app_js = _read("web/static/js/app.js")

    assert 'class="btn btn-primary btn-sm" id="btn-workbench-market-state"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-factors"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-cross-asset"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-onchain"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-discipline"' in template
    assert ".research-module-btns .btn" in style_css
    assert (
        "linear-gradient(135deg, #1a9a5e, #26dc85)" in style_css
        or "linear-gradient(135deg, var(--positive-deep), var(--positive))" in style_css
    )
    assert "bindAsyncButton('btn-workbench-market-state'" in workbench_js
    assert "bindAsyncButton('btn-workbench-discipline'" in workbench_js
    assert 'id="btn-refresh-market-sentiment-panel"' in template
    assert "async function syncWorkbenchMarketSentiment(payload, options = {})" in workbench_js
    assert "window.syncWorkbenchMarketSentiment = syncWorkbenchMarketSentiment;" in workbench_js
    assert "window.syncWorkbenchMarketSentiment(payload).catch(()=>{});" in app_js


def test_research_workbench_guides_the_workflow_and_keeps_profile_chrome_current():
    template = _read("web/templates/index.html")
    style_css = _read("web/static/css/style.css")
    workbench_js = _read("web/static/js/research_workbench.js")

    assert 'id="research-section-nav"' in template
    assert 'class="research-config-section research-universe-picker"' in template
    assert 'id="research-universe-summary"' in template
    assert 'id="research-factor-section"' in template
    assert ".research-section-nav-links" in style_css
    assert ".research-module-btns .btn" in style_css
    assert "function renderWorkflowChrome(profile = getProfile())" in workbench_js
    assert "function watchProgrammaticConfigChanges()" in workbench_js
    assert "state.configObserver.observe(select" in workbench_js

    marker = "function renderStatusCards()"
    section = workbench_js.split(marker, 1)[1].split("function setOverviewCardValue", 1)[0]
    assert "const profile = getProfile();" in section
    assert "renderWorkflowChrome(profile);" in section
    assert "运行总览后启用 30 分钟自动刷新" in section


def test_research_workbench_recommendations_render_structured_actions():
    template = _read("web/templates/index.html")
    research_api = _read("web/api/research.py")
    style_css = _read("web/static/css/style.css")
    workbench_js = _read("web/static/js/research_workbench.js")

    assert "结论 / 下一步" in template
    assert "_profile_symbol_window(profile, 30)" in research_api
    assert "apiResearch('/recommendations'" in workbench_js
    assert "function executeRecommendationAction(action)" in workbench_js
    assert "function applyRecommendationToAi(action)" in workbench_js
    assert "function getFactorFocusItems(rec = state.recommendations)" in workbench_js
    assert (
        "function describeRecommendationSource(meta = getRecommendationSourceMeta())"
        in workbench_js
    )
    assert (
        "data-action-id=\"${escSafe(String(action.id || ''))}\"" in workbench_js
        or "data-action-id=\"${escSafe(String(a.id || ''))}\"" in workbench_js
    )
    assert (
        "research-conclusion-action-btn" in workbench_js
        or 'class="rec-action-btn"' in workbench_js
    )
    assert "research-brief-grid" in workbench_js
    assert ".research-conclusion-action-btn" in style_css
    assert ".research-brief-grid" in style_css
    assert ".research-conclusion-tag" in style_css

    # Regression guard: the verdict hero block must use ASCII-quoted class
    # attributes. A prior commit corrupted these with smart/curly quotes
    # (U+201C/U+201D), which silently broke ALL verdict-card styling because
    # the rendered class names no longer matched the stylesheet. The earlier
    # assertions above only covered rec-action-btn (which stayed ASCII), so
    # the bug went undetected. Pin the verdict classes explicitly and forbid
    # smart double-quotes anywhere in the workbench script.
    assert 'class="rec-verdict-top"' in workbench_js
    assert 'class="rec-bias-badge ${bc.cls}"' in workbench_js
    assert 'class="rec-headline"' in workbench_js
    assert 'class="rec-symbol-tag"' in workbench_js
    assert 'class="research-conclusion-empty"' in workbench_js
    assert "“" not in workbench_js
    assert "”" not in workbench_js


def test_research_workbench_overview_button_uses_parallel_overview_endpoint():
    workbench_js = _read("web/static/js/research_workbench.js")

    marker = "async function runWorkbenchOverviewDirect(quiet = false)"
    assert marker in workbench_js
    section = workbench_js.split(marker, 1)[1].split("function maybeAutoRefreshWorkbench", 1)[0]

    assert "apiResearch(`/overview?${profileQuery(state.profile)}`" in section
    assert "Object.entries(overview.modules || {}).forEach" in section
    assert "research.workbench.overview.fallback" in section


def test_research_workbench_auto_refresh_stops_timers_when_tab_hidden():
    workbench_js = _read("web/static/js/research_workbench.js")

    stop_marker = "function stopWorkbenchAutoRefresh()"
    assert stop_marker in workbench_js
    stop_section = workbench_js.split(stop_marker, 1)[1].split(
        "function startWorkbenchAutoRefresh", 1
    )[0]
    assert "clearInterval(state.autoRefreshTimer)" in stop_section
    assert "clearInterval(state._countdownTimer)" in stop_section
    assert "state.autoRefreshTimer = null;" in stop_section
    assert "state._countdownTimer = null;" in stop_section

    start_section = workbench_js.split("function startWorkbenchAutoRefresh", 1)[1].split(
        "if (typeof document !== 'undefined')", 1
    )[0]
    assert "stopWorkbenchAutoRefresh();" in start_section
    assert "document.hidden" in start_section

    visibility_section = workbench_js.split("document.addEventListener('visibilitychange'", 1)[
        1
    ].split("function bindAsyncButton", 1)[0]
    assert "if (document.hidden)" in visibility_section
    assert "stopWorkbenchAutoRefresh();" in visibility_section


def test_onchain_panels_use_auto_chain_resolution():
    app_js = _read("web/static/js/app.js")
    workbench_js = _read("web/static/js/research_workbench.js")

    assert "chain=auto" in app_js
    assert "chain=auto" in workbench_js
    assert "renderData?.chain_context?.display_name||'Auto'" in app_js
    assert "onchainRes?.chain_context?.display_name || 'Auto'" in workbench_js


def test_research_symbol_options_keep_defaults_and_retry_after_timeout():
    app_js = _read("web/static/js/app.js")

    marker = "async function loadResearchSymbolOptions(exchange,options={})"
    assert marker in app_js
    section = app_js.split(marker, 1)[1].split("function mapDownloadTaskStatus", 1)[0]

    assert "renderResearchSymbolSelects(RESEARCH_DEFAULT_SYMBOLS);" in section
    assert "loadResearchSymbolOptions.retryTimer=setTimeout" in section
    assert "loadResearchSymbolOptions timeout; using defaults for now" in section


def test_research_workbench_microstructure_summary_wires_long_short_and_order_walls():
    workbench_js = _read("web/static/js/research_workbench.js")
    app_js = _read("web/static/js/app.js")
    trading_api = _read("web/api/trading.py")
    research_api = _read("web/api/research.py")

    assert "function summarizeMicrostructureSignal" in workbench_js
    assert "long_short_ratio" in workbench_js
    assert "microstructure_summary" in workbench_js
    assert "Long/short ratio unavailable" in workbench_js
    assert "function fmtAgeSeconds(value)" in workbench_js
    assert "payload.derivatives_summary || {}" in workbench_js
    assert "listItem('Derivatives', derivativesParts || '-')" in workbench_js
    assert "Derivatives Source / Quota" in workbench_js
    assert "Derivatives History" in workbench_js
    assert "Funding Z-Score / Mean" in workbench_js
    assert "Long/Short 24h / Liq Burst" in workbench_js
    assert "Derivatives Labels" in workbench_js
    assert "history_ready: !!derivativesPayload?.history_ready" in workbench_js
    assert "funding_zscore: Number(derivativesPayload?.funding_zscore)" in workbench_js
    assert (
        "long_short_ratio_change_24h: Number(derivativesPayload?.long_short_ratio_change_24h)"
        in workbench_js
    )
    assert (
        "derivatives_labels: Array.isArray(derivativesPayload?.derivatives_labels)"
        in workbench_js
    )
    assert (
        "/trading/analytics/history/status?exchange=${exchange}&symbol=${primarySymbol}"
        in workbench_js
    )
    assert "iceberg_candidates" in app_js
    assert "large_order_count" in app_js
    assert "coinglass_cache:'CoinGlass缓存'" in app_js
    assert "fred:'FRED'" in app_js
    assert "'stats.gov.cn':'国家统计局'" in app_js
    assert "function formatAnalyticsRecentPoint(rowKey,item)" in app_js
    assert "if(row.key==='derivatives')" in app_js
    assert "资金费率 ${funding} | 拥挤 ${crowding} | 挤压 ${squeeze}" in app_js
    assert "拥挤 ${crowding} | 资金 ${funding}" in app_js
    assert "key!=='derivatives'" in app_js
    assert "macro_source_summary" in app_js
    assert "derivatives_source_summary" in app_js
    assert "title:'宏观快照'" in app_js
    assert "title:'衍生品上下文'" in app_js
    assert "_fetch_long_short_ratio_snapshot" in trading_api
    assert "_build_microstructure_summary" in research_api
    assert "_build_derivatives_shadow_summary" in research_api
    assert "_build_derivatives_source_summary" in research_api
    assert "_build_macro_source_summary" in research_api
    assert '"derivatives_summary": derivatives_summary' in research_api
    assert '"macro_source_summary": macro_source_summary' in research_api
    assert '"derivatives_source_summary": derivatives_source_summary' in research_api
    assert "get_analytics_history_status" in research_api
    assert "slice(0, 4)" in workbench_js


def test_ai_research_diagnostics_warm_action_covers_macro_and_funding():
    template = _read("web/templates/index.html")
    diagnostics_js = _read("web/static/js/ai_research_diagnostics.js")
    ai_api = _read("web/api/ai_research.py")

    assert 'id="ai-funding-warm-btn"' in template
    assert "预热研究缓存" in template
    assert "/diagnostics/funding-cache/warm" in diagnostics_js
    assert "/diagnostics/macro-cache/warm" in diagnostics_js
    assert "研究缓存已预热:" in diagnostics_js
    assert (
        '@router.post("/diagnostics/macro-cache/warm", '
        'dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])'
    ) in ai_api
