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
