from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_paper_shadow_launcher_detaches_with_controlled_env_and_logs():
    script = _read("scripts/market_ws_paper_shadow.ps1")

    assert "New-ScheduledTaskAction" in script
    assert "Register-ScheduledTask" in script
    assert "Start-ScheduledTask" in script
    assert "CryptoMarketWsPaperShadow_" in script
    assert "paper_shadow_6h_{0}_{1}.cmd" in script
    assert "paper_shadow_6h_service_{0}.out.log" in script
    assert "paper_shadow_6h_service_{0}.err.log" in script
    assert "paper_shadow_6h_selfcheck_{0}.out.json" in script
    assert "paper_shadow_6h_selfcheck_{0}.err.log" in script
    assert "MARKET_WS_SHADOW_${Kind}_START" in script
    assert "MARKET_WS_SHADOW_${Kind}_EXIT" in script
    assert "MARKET_WS_SHADOW_EXIT_CODE=%ERRORLEVEL%" in script
    assert '-Kind "SERVICE"' in script
    assert '-Kind "SELFCHECK"' in script

    for expected in (
        'set "TRADING_MODE=paper"',
        'set "ALLOW_PERSISTED_LIVE_MODE_START=false"',
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=shadow"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_EXCHANGES=binance"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"',
        'set "EXCHANGE_WATCHDOG_ENABLED=false"',
        'set "COINGLASS_WORKER_ENABLED=false"',
        'set "NEWS_BACKGROUND_ENABLED=false"',
        'set "NEWS_LLM_BACKGROUND_ENABLED=false"',
        'set "DATA_MAINTENANCE_ENABLED=false"',
        'set "PUBLIC_MACRO_WORKERS_ENABLED=false"',
        'set "PREMIUM_EXTERNAL_WORKERS_ENABLED=false"',
        'set "ANALYTICS_HISTORY_ENABLED=false"',
    ):
        assert expected in script


def test_paper_shadow_launcher_uses_ops_token_header_and_level1_gates():
    script = _read("scripts/market_ws_paper_shadow.ps1")

    assert '"X-OPS-TOKEN" = $Token' in script
    assert "Authorization" not in script
    assert "--duration-sec $DurationSec" in script
    assert "--min-samples 361" in script
    assert "--min-ws-tick-delta 1" in script
    assert "--min-shadow-compare-delta 1" in script
    assert "--max-shadow-violation-delta 0" in script
    assert "--max-invalid-payload-delta 0" in script
    assert "--max-timestamp-regression-delta 0" in script
    assert "--max-shadow-stale-skip-delta 0" in script
    assert "--max-feed-watch-empty-delta 0" in script
    assert "--max-stale-symbol-count 0" in script
    assert "--max-price-diff-bps 20" in script
    assert "--max-ws-age-p95-ms 10000" in script


def test_live_shadow_launcher_pins_mark_price_stream_off_for_level2_shadow():
    script = _read("scripts/market_ws_live_shadow.ps1")

    for expected in (
        'set "TRADING_MODE=live"',
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=shadow"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_FAIL_CLOSED_FOR_LIVE=true"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"',
    ):
        assert expected in script
    assert "precheck_market_ws_live_shadow.py" in script
    assert "--expect-runtime live" in script
    assert "--timeout 20" in script
