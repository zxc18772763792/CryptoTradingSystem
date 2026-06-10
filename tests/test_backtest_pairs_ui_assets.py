from pathlib import Path

from jinja2 import Environment

from web.asset_versions import ASSET_VERSIONS, static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def _section_between(source: str, start: str, end: str) -> str:
    return source.split(start, 1)[1].split(end, 1)[0]


def test_backtest_pairs_dual_leg_ui_hooks_exist():
    template_source = _read("web/templates/index.html")
    template = _render_template(template_source)
    app_js = _read("web/static/js/app.js")

    assert "{{ static_asset_url('js/app.js') }}" in template_source
    assert static_asset_url("js/app.js") == f"/static/js/app.js?v={ASSET_VERSIONS['js/app.js']}"
    assert static_asset_url("js/app.js") in template
    assert 'id="backtest-pair-symbol-group"' in template_source
    assert 'id="backtest-pair-symbol"' in template_source
    assert 'id="backtest-symbol-label"' in template_source
    assert "function isBacktestDualLegStrategy" in app_js
    assert "function renderBacktestSymbolMode" in app_js
    assert "function loadBacktestSymbolOptions" in app_js
    assert "function buildBacktestRequestContext" in app_js
    assert "pairs_spread_dual_leg" in app_js
    assert "pair_symbol" in app_js
    assert "pair_metrics" in app_js
    assert "tradeDirectionText" in app_js
    assert "tradeDirectionColor" in app_js
    assert "direction==='long'?'Long':direction==='short'?'Short':'--'" in app_js
    assert "direction==='long'?'#3fb950':direction==='short'?'#f85149':'#9fb1c9'" in app_js
    assert "pushDirectionalTradeTrace(openRows,'open')" in app_js
    assert "pushDirectionalTradeTrace(closeRows,'close')" in app_js
    assert "Z-Score" in app_js
    assert "async function registerOptimizeTrialByRank" in app_js
    assert "registerOptimizeTrialByRank(${i}, this)" in app_js
    assert "window.registerOptimizeTrialByRank=registerOptimizeTrialByRank" in app_js


def test_backtest_chart_uses_shanghai_axis_for_trade_points():
    app_js = _read("web/static/js/app.js")
    section = app_js.split("function renderBacktest(r){", 1)[1].split(
        "function getBacktestStrategyCatalogFromSelect()", 1
    )[0]

    assert "const timestampMs=toMs(i.timestamp)" in section
    assert "const timestampMs=toMs(p?.timestamp)" in section
    assert "timestamp:klineShanghaiAxisIso(timestampMs)" in section
    assert "timestamp:toDate(i.timestamp)" not in section
    assert "timestamp:toDate(p?.timestamp)" not in section
    assert "const priceByTimestamp=new Map(rows.map(i=>[i.timestamp,i.close]));" in section
    assert "price:Number.isFinite(linePrice)?linePrice:executionPrice" in section
    assert "执行价: %{customdata:.6f}" in section


def test_dashboard_mode_ui_uses_runtime_mode_snapshot():
    app_js = _read("web/static/js/app.js")

    assert "function normalizeRuntimeMode" in app_js
    assert "function resolveRuntimeModeSnapshot" in app_js
    assert "function hasRenderableBalanceSnapshot" in app_js
    assert "function resolveActiveAccountUsdEstimate" in app_js
    assert "renderExchanges(displayBalances,activeType);" in app_js
    assert "const activeUsd=resolveActiveAccountUsdEstimate(displayBalances,mergedRisk,activeType);" in app_js
    assert "const balanceWarning=String(displayBalances?.warning||'').trim();" in app_js
    assert "else if(balanceWarning){" in app_js
    assert "资产分布暂不可用：" in app_js
    assert "await loadSystemStatus().catch" in app_js


def test_backtest_compare_ui_avoids_single_strategy_hard_dependency():
    template_source = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")

    assert "未填写开始/结束时间时，多策略对比会自动锁定最近窗口来控制耗时" in template_source
    assert "function recommendBacktestCompareWindowDays" in app_js
    assert "function resolveBacktestCompareExecutionScope" in app_js
    assert "function estimateBacktestCompareTimeoutMs" in app_js
    assert "Math.min(64,parseInt(strategyCount,10)||1)" in app_js
    assert "Math.min(20*60*1000,timeoutMs)" in app_js
    assert "await ensureBacktestStrategySelect().catch(err=>{" in app_js
    assert "backtest compare preflight skipped:" in app_js
    assert "const compareScope=resolveBacktestCompareExecutionScope" in app_js
    assert "const compareTimeoutMs=estimateBacktestCompareTimeoutMs(chosenStrategies.length,maxTrials,tf,compareScope.windowDays);" in app_js
    assert "本次可能持续数分钟，请勿重复点击" in app_js
    assert "autoWindowApplied:!!compareScope.autoWindowApplied" in app_js
    assert "实际回测区间 / 样本" in app_js
    assert "退出模板 / 预优化" in app_js
    assert "预算提示" in app_js
    assert "预算跳过" in app_js
    compare_section = app_js.split("const b1=document.getElementById('btn-backtest-compare');", 1)[1]
    compare_section = compare_section.split("const b2=document.getElementById('btn-backtest-optimize');", 1)[0]
    assert "await ensureSelectedBacktestStrategy();" not in compare_section
    assert "b1.disabled=true;" in compare_section
    assert "b1.disabled=false;" in compare_section


def test_backtest_optimize_uses_dynamic_timeout_and_custom_params():
    app_js = _read("web/static/js/app.js")

    assert "function estimateBacktestOptimizeTimeoutMs" in app_js
    optimize_section = _section_between(
        app_js,
        "const b2=document.getElementById('btn-backtest-optimize');",
        "const b3=document.getElementById('btn-backtest-export');",
    )

    assert "buildBacktestRequestContext(st,{includeCustomParams:false})" not in optimize_section
    assert "params_json=${encodeURIComponent(JSON.stringify(ctx.params))}" in optimize_section
    assert "const optimizeTimeoutMs=estimateBacktestOptimizeTimeoutMs(st,maxTrials,tf," in optimize_section
    assert "timeoutMs:optimizeTimeoutMs" in optimize_section
    assert "timeoutMs:90000" not in optimize_section


def test_backtest_custom_params_hint_matches_optimize_behavior():
    template_source = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")
    stale_hint = "多策略对比 / 参数优化暂不读取这里的 JSON"

    assert stale_hint not in template_source
    assert stale_hint not in app_js
    assert "运行回测与参数优化会读取这里的 JSON" in template_source
    assert "运行回测与参数优化会读取这里的 JSON" in app_js
    assert "多策略对比不会读取这里的自由 JSON" in template_source
    assert "多策略对比不会读取这里的自由 JSON" in app_js


def test_backtest_fixed_take_profit_ui_matches_backend_range():
    template_source = _read("web/templates/index.html")

    assert "固定止盈比例（0.01~0.99，如 0.10 = 10%）" in template_source
    assert 'id="backtest-take-profit-pct" value="0.10" min="0.01" max="0.99" step="0.01" disabled' in template_source
    assert 'id="backtest-take-profit-pct" value="0.10" min="0.01" max="1.0"' not in template_source


def test_backtest_registration_preserves_exit_management_hooks():
    app_js = _read("web/static/js/app.js")

    assert "function resolveBacktestRuntimeProtection" in app_js
    assert "function buildBacktestRuntimeStrategyParams" in app_js
    assert "function ensureStrategyEditorRuntimeExitFields" in app_js
    assert "params.exit_template=protection.exitTemplate" in app_js
    assert "ensureStrategyEditorRuntimeExitFields(panel,info.params||{})" in app_js
    assert "ensureStrategyEditorRuntimeExitFields(panel,bestParams);" in app_js
    assert "exit_template: best?.exit_template||opt?.exit_template||opt?.default_exit_template||DEFAULT_BACKTEST_EXIT_TEMPLATE" in app_js
    assert "exit_template: row?.exit_template||backtestUIState?.lastCompare?.exit_template||backtestUIState?.lastCompare?.default_exit_template||DEFAULT_BACKTEST_EXIT_TEMPLATE" in app_js
