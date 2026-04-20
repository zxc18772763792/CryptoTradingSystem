from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_pnl_heatmap_request_includes_runtime_mode():
    app_js = _read("web/static/js/app.js")

    assert "const mode=resolveRuntimeModeSnapshot({" in app_js
    assert "&mode=${encodeURIComponent(mode)}" in app_js
    assert "/trading/pnl/heatmap?days=${Math.max(1,d)}&bucket=${encodeURIComponent(b)}&mode=${encodeURIComponent(mode)}" in app_js
