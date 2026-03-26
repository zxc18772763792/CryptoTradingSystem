from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_arbitrage_page_assets_and_hooks_exist():
    template = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")
    style_css = _read("web/static/css/style.css")

    assert 'data-tab="arbitrage"' in template
    assert template.index('data-tab="research"') < template.index('data-tab="arbitrage"') < template.index('data-tab="backtest"')
    assert 'id="arbitrage"' in template
    assert 'id="arbitrage-strategy"' in template
    assert 'id="arbitrage-primary-symbol"' in template
    assert 'id="arbitrage-universe"' in template
    assert 'id="btn-arbitrage-register"' in template
    assert 'id="btn-arbitrage-backtest"' in template
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
    assert "window.openBacktestWithSpec=openBacktestWithSpec" in app_js
    assert "window.registerArbitrageStrategy=registerArbitrageStrategy" in app_js
    assert "window.jumpToBacktestFromArbitrage=jumpToBacktestFromArbitrage" in app_js
    assert "'run_custom':'run'" in app_js
    assert "params_json=" in app_js

    assert ".arbitrage-workspace" in style_css
    assert ".arbitrage-plan-steps" in style_css
    assert ".arbitrage-strategy-grid" in style_css
    assert '.arbitrage-card[data-selected="true"]' in style_css
    assert ".backtest-custom-params" in style_css
