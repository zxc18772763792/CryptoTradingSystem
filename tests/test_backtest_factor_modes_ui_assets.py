from pathlib import Path

from jinja2 import Environment

from web.asset_versions import static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def test_backtest_factor_mode_assets_are_wired():
    template_source = _read("web/templates/index.html")
    template = _render_template(template_source)
    app_js = _read("web/static/js/app.js")
    style_css = _read("web/static/css/style.css")

    assert 'id="backtest-mode-tabs"' in template
    assert 'data-backtest-mode="classic"' in template
    assert 'data-backtest-mode="factor_template"' in template
    assert 'data-backtest-mode="factor_batch"' in template
    assert 'id="backtest-mode-classic"' in template
    assert 'id="backtest-mode-factor-template"' in template
    assert 'id="backtest-mode-factor-batch"' in template
    assert 'id="factor-template-strategy"' in template
    assert 'id="factor-template-universe"' in template
    assert 'id="btn-factor-template-run"' in template
    assert 'id="btn-factor-template-add-batch"' in template
    assert 'id="btn-factor-template-register"' in template
    assert 'id="factor-batch-list"' in template
    assert 'id="btn-factor-batch-run"' in template
    assert 'id="factor-batch-output"' in template

    assert app_js.count("function setBacktestMode(") == 1
    assert "function runFactorTemplateBacktest" in app_js
    assert "function runFactorBatchBacktest" in app_js
    assert "function renderFactorBatchOutput" in app_js
    assert "function renderBacktestExtraStatus" in app_js
    assert "renderBacktestExtraStatus('因子模板回测'" in app_js
    assert "FACTOR_BACKTEST_DEFAULT_UNIVERSE_LIMIT=20" in app_js
    assert "FACTOR_BACKTEST_DEFAULT_BATCH_LIMIT=6" in app_js
    assert "strategy_kind" in app_js
    assert "template_spec" in app_js
    assert "params_json=${encodeURIComponent(JSON.stringify(params))}" in app_js
    assert "compare_mode:'factor_batch'" in app_js
    assert "requested_start_date:dates.start" in app_js
    assert "use_stop_take:itemUseStopTake" in app_js
    assert "register_spec:{strategy_type:item.strategy,symbol,timeframe:tf,params:previewParams" in app_js
    assert "compare.requested_start_date" in app_js
    assert "_compare_request_meta?.effectiveStartDate" in app_js
    assert "const previewProtection=resolveBacktestRuntimeProtection({" in app_js
    assert "u=appendBacktestProtectionParams(u,previewProtection)" in app_js

    assert ".backtest-mode-tabs" in style_css
    assert ".backtest-mode-panel" in style_css
    assert ".factor-template-spec" in style_css
    assert ".factor-batch-output" in style_css
