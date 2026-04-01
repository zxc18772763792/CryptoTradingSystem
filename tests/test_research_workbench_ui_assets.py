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
