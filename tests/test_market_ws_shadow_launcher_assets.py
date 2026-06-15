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


def test_ui_primary_launcher_pins_level3_env_and_gates():
    script = _read("scripts/market_ws_ui_primary.ps1")

    # Level-3 runtime env: live + ui_primary + quality guard forced on.
    for expected in (
        'set "TRADING_MODE=live"',
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=ui_primary"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_FAIL_CLOSED_FOR_LIVE=true"',
        'set "MARKET_WS_QUALITY_GUARD_ENABLED=true"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"',
    ):
        assert expected in script

    # Real-money confirm guard plus the hard Level-2 evaluator gate; the gate
    # must run inside start-service (not only as a standalone action).
    assert "Require-LiveConfirm" in script
    assert "evaluate_market_ws_shadow_report.py" in script
    assert "Invoke-Level2Gate" in script
    assert "Level-2 evaluator gate FAILED" in script
    assert '"--expect-runtime", "live"' in script
    assert '"--min-samples", "1430"' in script
    assert '"--require-final-runtime-fields"' in script

    # Observation selfcheck pins ui_primary/live and must NOT require shadow
    # compare growth (REST reconcile is suppressed while WS is healthy).
    assert "--expect-mode ui_primary" in script
    assert "--expect-runtime live" in script
    assert "--min-shadow-compare-delta 0" in script
    assert "--tolerate-transient" in script

    # Instrumented detach markers + ops auth header, same as Level 1/2.
    assert "MARKET_WS_UI_PRIMARY_${Kind}_START" in script
    assert "MARKET_WS_UI_PRIMARY_${Kind}_EXIT" in script
    assert '"X-OPS-TOKEN" = $Token' in script
    assert "Authorization" not in script

    # Fallback drill (s28.5): -DrillForceRest flips ONLY the kill switch while
    # every other pinned env line and gate stays identical, and the READY
    # probe verifies the service actually came up with the requested value.
    assert "[switch]$DrillForceRest" in script
    assert 'set "MARKET_WS_FORCE_REST=true"' in script
    assert "force_rest=$($market.force_rest)" in script


def test_strategy_primary_launcher_pins_level4_env_and_gates():
    script = _read("scripts/market_ws_strategy_primary.ps1")

    # Level-4 runtime env: live + strategy_primary + quality guard forced on +
    # fail-closed for live. Mark-price stream stays pinned off (no scope creep).
    for expected in (
        'set "TRADING_MODE=live"',
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=strategy_primary"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_FAIL_CLOSED_FOR_LIVE=true"',
        'set "MARKET_WS_QUALITY_GUARD_ENABLED=true"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"',
    ):
        assert expected in script

    # Defense-in-depth: BOTH live-confirm switches are required, and the L4 gate
    # re-evaluates the COMPLETED Level-3 ui_primary report inside start-service.
    assert "[switch]$ConfirmLive" in script
    assert "[switch]$ConfirmStrategyPrimary" in script
    assert "Require-LiveConfirm" in script
    assert "evaluate_market_ws_shadow_report.py" in script
    assert "Invoke-Level3Gate" in script
    assert "Level-3 evaluator gate FAILED" in script
    assert '"--expect-mode", "ui_primary"' in script
    assert '"--expect-runtime", "live"' in script
    assert '"--require-final-runtime-fields"' in script

    # Hard s28.5 fallback-drill-record gate (no skip switch).
    assert "Require-DrillReport" in script
    assert "[string]$DrillReport" in script

    # Observation selfcheck pins strategy_primary/live; REST reconcile suppressed
    # while WS healthy, so do not require shadow-compare growth.
    assert "--expect-mode strategy_primary" in script
    assert "--expect-runtime live" in script
    assert "--min-shadow-compare-delta 0" in script
    assert "--tolerate-transient" in script

    # READY probe must verify the service truly came up as strategy_primary with
    # the guard on and fail-closed armed -- never trust the requested mode blindly.
    assert "expected 'strategy_primary'" in script
    assert "without the WS quality guard enabled" in script
    assert "fail_closed_for_live=false" in script

    # Instrumented detach markers + ops auth header, same as Level 1/2/3.
    assert "MARKET_WS_STRATEGY_PRIMARY_${Kind}_START" in script
    assert "MARKET_WS_STRATEGY_PRIMARY_${Kind}_EXIT" in script
    assert '"X-OPS-TOKEN" = $Token' in script
    assert "Authorization" not in script

    # Fallback drill for L4 rollback proof.
    assert "[switch]$DrillForceRest" in script
    assert 'set "MARKET_WS_FORCE_REST=true"' in script
    assert "force_rest=$($market.force_rest)" in script
