from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_kline_chart_clears_loading_placeholder_before_plotly_render():
    app_js = _read("web/static/js/app.js")
    cleanup = "Array.from(c.querySelectorAll(':scope > .kline-chart-placeholder')).forEach(el=>el.remove());"
    plotly_render = "Plotly.react(c,"

    assert "kline-chart-placeholder" in app_js
    assert cleanup in app_js
    assert plotly_render in app_js
    assert app_js.index(cleanup) < app_js.index(plotly_render)
