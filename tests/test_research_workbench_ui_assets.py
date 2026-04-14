from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_research_workbench_module_actions_share_primary_style():
    template = _read("web/templates/index.html")
    style_css = _read("web/static/css/style.css")
    workbench_js = _read("web/static/js/research_workbench.js")

    assert 'class="btn btn-primary btn-sm" id="btn-workbench-market-state"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-factors"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-cross-asset"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-onchain"' in template
    assert 'class="btn btn-primary btn-sm" id="btn-workbench-discipline"' in template
    assert ".research-module-btns .btn" in style_css
    assert "linear-gradient(135deg, #1a9a5e, #26dc85)" in style_css
    assert "bindAsyncButton('btn-workbench-market-state'" in workbench_js
    assert "bindAsyncButton('btn-workbench-discipline'" in workbench_js


def test_research_workbench_recommendations_render_structured_actions():
    template = _read("web/templates/index.html")
    research_api = _read("web/api/research.py")
    style_css = _read("web/static/css/style.css")
    workbench_js = _read("web/static/js/research_workbench.js")

    assert '结论 / 下一步' in template
    assert '_profile_symbol_window(profile, 30)' in research_api
    assert "apiResearch('/recommendations'" in workbench_js
    assert "function executeRecommendationAction(action)" in workbench_js
    assert "function applyRecommendationToAi(action)" in workbench_js
    assert "function getFactorFocusItems(rec = state.recommendations)" in workbench_js
    assert "function describeRecommendationSource(meta = getRecommendationSourceMeta())" in workbench_js
    assert 'data-action-id="${escSafe(String(action.id || \'\'))}"' in workbench_js
    assert "research-conclusion-action-btn" in workbench_js
    assert "research-brief-grid" in workbench_js
    assert "因子来源" in workbench_js
    assert ".research-conclusion-action-btn" in style_css
    assert ".research-brief-grid" in style_css
    assert ".research-conclusion-tag" in style_css


def test_research_workbench_microstructure_summary_wires_long_short_and_order_walls():
    workbench_js = _read("web/static/js/research_workbench.js")
    app_js = _read("web/static/js/app.js")
    trading_api = _read("web/api/trading.py")
    research_api = _read("web/api/research.py")

    assert "function summarizeMicrostructureSignal" in workbench_js
    assert "long_short_ratio" in workbench_js
    assert "microstructure_summary" in workbench_js
    assert "Long/short ratio unavailable" in workbench_js
    assert "iceberg_candidates" in app_js
    assert "large_order_count" in app_js
    assert "_fetch_long_short_ratio_snapshot" in trading_api
    assert "_build_microstructure_summary" in research_api


def test_ai_research_diagnostics_warm_action_covers_macro_and_funding():
    template = _read("web/templates/index.html")
    diagnostics_js = _read("web/static/js/ai_research_diagnostics.js")
    ai_api = _read("web/api/ai_research.py")

    assert 'id="ai-funding-warm-btn"' in template
    assert "预热研究缓存" in template
    assert "/diagnostics/funding-cache/warm" in diagnostics_js
    assert "/diagnostics/macro-cache/warm" in diagnostics_js
    assert "研究缓存已预热:" in diagnostics_js
    assert '@router.post("/diagnostics/macro-cache/warm")' in ai_api
