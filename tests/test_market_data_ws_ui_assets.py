from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_market_data_ws_status_has_separate_header_badge():
    template = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")
    css = _read("web/static/css/style.css")

    assert 'id="market-data-status"' in template
    assert "market-data-status" in css
    assert "function renderMarketDataStatus" in app_js
    assert "document.getElementById('market-data-status')" in app_js
    assert "document.getElementById('system-status')" in app_js


def test_market_data_ws_status_uses_exchange_ws_metrics_not_browser_ws_badge():
    app_js = _read("web/static/js/app.js")

    required_fields = [
        "market_ws",
        "feed_healthy",
        "hub_healthy",
        "ws_hub_healthy",
        "rest_fallback_count",
        "ws_stale_symbol_count",
        "stale_symbol_count",
        "mark_price_enabled",
        "auxiliary_symbol_count",
        "channel_counts",
        "shadow_compare_violation_count",
    ]
    for field in required_fields:
        assert field in app_js

    assert "行情: WS shadow" in app_js
    assert "行情: WS primary" in app_js
    assert "行情: REST fallback" in app_js
    assert "行情: stale" in app_js
    assert "ws_stale_symbols" in app_js
    assert "stale_symbols_total" in app_js
    assert "mark_price_enabled" in app_js
    assert "auxiliary_symbols" in app_js
    assert "channels=" in app_js
