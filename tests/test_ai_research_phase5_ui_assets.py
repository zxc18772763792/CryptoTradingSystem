from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_ai_research_template_loads_phase5_modules():
    template = _read("web/templates/index.html")

    assert 'id="ai-flow-console"' in template
    assert 'id="ai-flow-stage-grid"' in template
    assert "/static/js/ai_research.js" in template
    assert "/static/js/ai_research_diagnostics.js" in template
    assert "/static/js/ai_research_runtime.js" in template
    assert "/static/js/ai_research_agent.js" in template
    assert "/static/js/ai_research_patch.js" not in template


def test_ai_research_phase5_assets_exist_and_define_flow_styles():
    diagnostics_js = _read("web/static/js/ai_research_diagnostics.js")
    runtime_js = _read("web/static/js/ai_research_runtime.js")
    agent_js = _read("web/static/js/ai_research_agent.js")
    style_css = _read("web/static/css/style.css")

    assert "modules.diagnostics" in diagnostics_js
    assert "ai-flow-stage-grid" in runtime_js
    assert "window.agentStart = agentStart" in agent_js
    assert ".ai-flow-console" in style_css
    assert ".ai-flow-stage-grid" in style_css
