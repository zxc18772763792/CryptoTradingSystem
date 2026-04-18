from pathlib import Path

from jinja2 import Environment

from web.asset_versions import ASSET_VERSIONS, static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def test_arbitrage_page_assets_and_hooks_exist():
    template_source = _read("web/templates/index.html")
    template = _render_template(template_source)
    app_js = _read("web/static/js/app.js")
    style_css = _read("web/static/css/style.css")

    assert "{{ static_asset_url('js/app.js') }}" in template_source
    assert static_asset_url("js/app.js") == f"/static/js/app.js?v={ASSET_VERSIONS['js/app.js']}"
    assert static_asset_url("js/app.js") in template
    assert 'data-tab="arbitrage"' in template
    assert template.index('data-tab="research"') < template.index('data-tab="arbitrage"') < template.index('data-tab="backtest"')
    assert 'id="arbitrage"' in template
    assert 'id="arbitrage-strategy"' in template
    assert 'id="arbitrage-primary-symbol"' in template
    assert 'id="arbitrage-universe"' in template
    assert 'id="btn-arbitrage-register"' in template
    assert 'id="btn-arbitrage-backtest"' in template
    assert 'id="btn-arbitrage-scan-pairs"' in template
    assert 'id="btn-arbitrage-apply-top-pair"' in template
    assert 'id="btn-arbitrage-apply-live-top-pair"' in template
    assert 'id="arbitrage-pair-scan-summary"' in template
    assert 'id="arbitrage-pair-ranking-body"' in template
    assert 'id="arbitrage-executable-pair-body"' in template
    assert 'id="arbitrage-data-card"' in template
    assert 'id="arbitrage-backtest-card"' in template
    assert 'id="arbitrage-cost-card"' in template
    assert 'id="arbitrage-entry-card"' in template
    assert 'id="arbitrage-action-chip"' in template
    assert 'id="arbitrage-risk-diagnostics"' in template
    assert 'id="arbitrage-payload-preview"' in template
    assert 'id="backtest-custom-params"' in template
    assert 'id="backtest-custom-params-panel"' in template

    assert "const ARBITRAGE_STRATEGY_ORDER" in app_js
    assert "async function loadArbitrageTabData" in app_js
    assert "arbitrage:()=>loadArbitrageTabData(false)" in app_js
    assert "async function openBacktestWithSpec" in app_js
    assert "function setBacktestCustomParams" in app_js
    assert "function getBacktestCustomParams" in app_js
    assert "function buildArbitrageStrategySpec" in app_js
    assert "async function registerArbitrageStrategy" in app_js
    assert "async function jumpToBacktestFromArbitrage" in app_js
    assert "async function scanArbitragePairsRanking" in app_js
    assert "async function applyArbitragePairCandidate" in app_js
    assert "/data/research/pairs-ranking" in app_js
    assert "/data/research/arbitrage-readiness" in app_js
    assert "function renderArbitrageRiskConsole" in app_js
    assert "function getArbitrageCtaState" in app_js
    assert "function resetArbitrageOutputForTemplate" in app_js
    assert "function isArbitrageOutputContextCurrent" in app_js
    assert "function setArbitrageOutputIfCurrent" in app_js
    assert "scheduleArbitrageReadinessRefresh" in app_js
    assert "window.openBacktestWithSpec=openBacktestWithSpec" in app_js
    assert "window.registerArbitrageStrategy=registerArbitrageStrategy" in app_js
    assert "window.jumpToBacktestFromArbitrage=jumpToBacktestFromArbitrage" in app_js
    assert "window.scanArbitragePairsRanking=scanArbitragePairsRanking" in app_js
    assert "window.applyArbitragePairCandidate=applyArbitragePairCandidate" in app_js
    assert "'run_custom':'run'" in app_js
    assert "params_json=" in app_js

    assert ".arbitrage-workspace" in style_css
    assert ".arbitrage-plan-steps" in style_css
    assert ".arbitrage-strategy-grid" in style_css
    assert '.arbitrage-card[data-selected="true"]' in style_css
    assert ".arbitrage-risk-grid" in style_css
    assert ".arbitrage-pair-split" in style_css
    assert ".arbitrage-group-advanced" in style_css
    assert ".arbitrage-pair-scanner-card" in style_css
    assert ".arbitrage-pair-table" in style_css
    assert ".arbitrage-pair-empty" in style_css
    assert ".backtest-custom-params" in style_css


def test_arbitrage_risk_console_copy_and_gate_texts_are_present():
    template_source = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")
    data_api = _read("web/api/data.py")

    assert "套利风控闸门" in template_source
    assert "数据就绪" in template_source
    assert "回测可信度" in template_source
    assert "成本压缩" in template_source
    assert "当前入场状态" in template_source
    assert "研究候选" in template_source
    assert "当前可开仓候选" in template_source
    assert "回填研究榜首" in template_source
    assert "回填可执行榜首" in template_source
    assert "诊断与接入说明" in template_source

    assert "高级 / 仅实时验证" in app_js
    assert "当前策略" in app_js
    assert "当前仅观察" in app_js
    assert "加入观察清单" in app_js
    assert "先回测后再运行" in app_js
    assert "先回测验证" in app_js
    assert "当前暂无进入开仓区的 pair" in app_js
    assert "研究候选用于挑选配对组合，真正可执行仍要同时通过上方风控卡与入场闸门。" in app_js
    assert "已切换到 ${strategyTypeShortName(selected)} 模板" in app_js

    assert "结果不可信" in data_api
    assert "结构边际不足" in data_api
    assert "成本吞噬边际" in data_api
    assert "gross > 0 and net <= 0" in data_api
