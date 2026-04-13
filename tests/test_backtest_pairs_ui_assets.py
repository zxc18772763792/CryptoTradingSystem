from pathlib import Path

from jinja2 import Environment

from web.asset_versions import ASSET_VERSIONS, static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def test_backtest_pairs_dual_leg_ui_hooks_exist():
    template_source = _read("web/templates/index.html")
    template = _render_template(template_source)
    app_js = _read("web/static/js/app.js")

    assert "{{ static_asset_url('js/app.js') }}" in template_source
    assert static_asset_url("js/app.js") == f"/static/js/app.js?v={ASSET_VERSIONS['js/app.js']}"
    assert static_asset_url("js/app.js") in template
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


def test_dashboard_mode_ui_uses_runtime_mode_snapshot():
    app_js = _read("web/static/js/app.js")

    assert "function normalizeRuntimeMode" in app_js
    assert "function resolveRuntimeModeSnapshot" in app_js
    assert "renderExchanges(displayBalances,activeType);" in app_js
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
