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


def test_dashboard_scripts_defer_execution_until_html_is_parsed():
    template = _read("web/templates/index.html")
    assert '<script defer charset="utf-8" src="{{ static_asset_url(\'js/app.js\') }}' in template
    assert '<script defer charset="utf-8" src="{{ static_asset_url(\'js/ai_research.js\') }}' in template


def test_versioned_assets_are_cacheable_across_dashboard_visits():
    main_py = _read("web/main.py")
    assert 'request.url.path.startswith("/static/") and request.query_params.get("v")' in main_py
    assert '"public, max-age=31536000, immutable"' in main_py
