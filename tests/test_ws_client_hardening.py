"""Static asset tests for the WebSocket client hardening.

The trading tab was flooding the server with refresh bursts because the
client treated the every-2s ``runtime_snapshot`` heartbeat as a data-change
event. These tests are tripwires for that regression.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _app_js() -> str:
    return (REPO_ROOT / "web" / "static" / "js" / "app.js").read_text(encoding="utf-8-sig")


def test_runtime_snapshot_does_not_trigger_soft_refresh():
    """``runtime_snapshot`` fires every 2s as a heartbeat. The trading-tab
    softRefresh handler loads 9 endpoints; including this event in the
    refresh trigger list produced ~330 req/min and caused worker overload
    + WS disconnect storms."""
    js = _app_js()
    # The refresh-trigger array should explicitly enumerate the events that
    # actually mutate data — and ``runtime_snapshot`` must NOT be among them.
    assert "['order_event','position_event','execution_event','mode_changed']" in js, (
        "Refresh trigger list must be order/position/execution/mode_changed only"
    )
    # Belt-and-braces: there should be no surviving occurrence that adds
    # runtime_snapshot back to the refresh trigger array.
    assert "runtime_snapshot'].includes(ev)" not in js
    assert "'runtime_snapshot'" not in js or "do not trigger refresh" in js


def test_ws_reconnect_uses_exponential_backoff():
    """Fixed 2s reconnect would hammer an overloaded server. Exponential
    backoff with a cap is the contract."""
    js = _app_js()
    assert "WS_RECONNECT_BASE_MS" in js
    assert "WS_RECONNECT_MAX_MS" in js
    assert "Math.pow(2,wsReconnectAttempt)" in js
    # Sanity: the cap should be at least 10s so steady-state isn't a fast cycle.
    assert "WS_RECONNECT_MAX_MS=30000" in js


def test_ws_heartbeat_armed_and_pong_received():
    """Half-open TCP connections need an application-level heartbeat. The
    client must send `ping` periodically and force-close on timeout."""
    js = _app_js()
    assert "WS_HEARTBEAT_INTERVAL_MS" in js
    assert "WS_HEARTBEAT_TIMEOUT_MS" in js
    assert "socket.send('ping')" in js
    assert "heartbeat timeout, closing socket" in js
    # Pong receipt must reset the heartbeat deadline.
    assert "if(ev==='pong')" in js


def test_trading_tab_soft_refresh_is_throttled():
    """Trading tab fires 9 concurrent API calls per refresh. Burst events
    (cancel-all, mode switch, etc.) must coalesce into a single refresh."""
    js = _app_js()
    assert "softRefresh(getActiveTabName()==='trading'?500:120)" in js
