from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_backtest_pairs_dual_leg_ui_hooks_exist():
    template = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")

    assert '/static/js/app.js?v=123' in template
    assert "pairs_spread_dual_leg" in app_js
    assert "pair_symbol" in app_js
    assert "开仓点" in app_js
    assert "平仓点" in app_js
    assert "副腿价格" in app_js
    assert "async function registerOptimizeTrialByRank" in app_js
    assert "registerOptimizeTrialByRank(${i}, this)" in app_js
    assert "window.registerOptimizeTrialByRank=registerOptimizeTrialByRank" in app_js
