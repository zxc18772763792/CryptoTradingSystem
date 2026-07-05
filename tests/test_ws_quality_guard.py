"""Tests for the WS market-data quality auto-degrade guard."""
from __future__ import annotations

from core.marketdata.ws_quality_guard import (
    WsQualityGuard,
    WsQualitySample,
    sample_from_market_ws_status,
)


def _healthy(ts: float, **kw) -> WsQualitySample:
    base = dict(ts=ts, feed_healthy=True, hub_healthy=True, ws_stale_symbol_count=0, last_tick_age_ms=500.0)
    base.update(kw)
    return WsQualitySample(**base)


def _bad(ts: float, **kw) -> WsQualitySample:
    base = dict(ts=ts, feed_healthy=False, hub_healthy=True, ws_stale_symbol_count=2, last_tick_age_ms=99999.0)
    base.update(kw)
    return WsQualitySample(**base)


def test_disabled_guard_never_forces_rest():
    g = WsQualityGuard(enabled=False, window_sec=60, min_samples=2)
    d = None
    for t in range(0, 200, 10):
        d = g.observe(_bad(float(t)))
    assert g.force_rest is False
    assert g.state == "ws"
    assert d.action == "none"


def test_sustained_unhealthy_degrades_and_forces_rest():
    g = WsQualityGuard(enabled=True, window_sec=60, min_samples=3, degrade_unhealthy_fraction=0.5)
    for t in range(0, 60, 10):
        g.observe(_bad(float(t)))
    assert g.state == "degraded"
    assert g.force_rest is True
    assert g.status()["degrade_count"] == 1


def test_brief_blip_does_not_degrade():
    g = WsQualityGuard(enabled=True, window_sec=120, min_samples=4, degrade_unhealthy_fraction=0.5)
    g.observe(_healthy(0))
    g.observe(_healthy(10))
    g.observe(_bad(20))           # single blip among healthy
    g.observe(_healthy(30))
    g.observe(_healthy(40))
    assert g.state == "ws"
    assert g.force_rest is False


def test_quality_breach_delta_degrades_even_when_healthy():
    # A price-divergence violation is a correctness signal -> degrade regardless
    # of sample count / health flags.
    g = WsQualityGuard(enabled=True, window_sec=120, min_samples=10, degrade_breach_delta=1)
    g.observe(_healthy(0, shadow_violation_count=0))
    g.observe(_healthy(10, shadow_violation_count=1))
    assert g.state == "degraded"


def test_invalid_payload_delta_degrades():
    g = WsQualityGuard(enabled=True, window_sec=120, min_samples=10, degrade_breach_delta=2)
    g.observe(_healthy(0, invalid_payload_count=5))
    g.observe(_healthy(10, invalid_payload_count=6))  # delta=1 < 2 -> no
    assert g.state == "ws"
    g.observe(_healthy(20, invalid_payload_count=7))  # delta=2 >= 2 -> degrade
    assert g.state == "degraded"


def test_recovers_only_after_sustained_healthy_window():
    g = WsQualityGuard(
        enabled=True, window_sec=60, min_samples=2, degrade_unhealthy_fraction=0.5, recover_healthy_sec=30
    )
    for t in range(0, 40, 10):
        g.observe(_bad(float(t)))
    assert g.state == "degraded"
    g.observe(_healthy(50))          # healthy window starts
    assert g.state == "degraded"
    g.observe(_healthy(70))          # 20s < 30s
    assert g.state == "degraded"
    d = g.observe(_healthy(85))      # 35s >= 30s -> recover
    assert g.state == "ws"
    assert g.force_rest is False
    assert d.action == "recover"
    assert g.status()["recover_count"] == 1


def test_relapse_resets_recovery_timer():
    g = WsQualityGuard(
        enabled=True, window_sec=120, min_samples=2, degrade_unhealthy_fraction=0.5, recover_healthy_sec=30
    )
    for t in range(0, 40, 10):
        g.observe(_bad(float(t)))
    assert g.state == "degraded"
    g.observe(_healthy(50))
    g.observe(_bad(60))              # relapse -> resets healthy timer
    g.observe(_healthy(70))
    g.observe(_healthy(95))          # only 25s of continuous health
    assert g.state == "degraded"


def test_sample_from_market_ws_status_maps_fields():
    payload = {
        "feed_healthy": True,
        "ws_hub_healthy": False,
        "ws_stale_symbol_count": 3,
        "last_tick_age_ms": 1234.0,
        "rest_fallback_count": 7,
        "shadow_compare_violation_count": 2,
        "invalid_payload_count": 1,
        "feed_last_error": "boom",
    }
    s = sample_from_market_ws_status(payload, now=100.0)
    assert s.ts == 100.0
    assert s.feed_healthy is True and s.hub_healthy is False
    assert s.ws_stale_symbol_count == 3
    assert s.last_tick_age_ms == 1234.0
    assert s.shadow_violation_count == 2
    assert s.invalid_payload_count == 1
    assert s.feed_last_error == "boom"
    # A guard fed this clearly-unhealthy sample treats it as not healthy.
    g = WsQualityGuard(enabled=True, window_sec=60, min_samples=1, degrade_unhealthy_fraction=0.5)
    g.observe(s)
    assert g.state == "degraded"


def test_status_clears_error_sentinels():
    s = sample_from_market_ws_status({"feed_last_error": "none"}, now=1.0)
    assert s.feed_last_error is None


def test_partial_stale_symbols_do_not_flap_the_guard():
    """Regression (2026-07-04): 4 illiquid watchlist alts going >10s without a
    trade flapped the guard 130x/day while BTC/ETH streamed fine. Per-symbol
    silence is not venue unhealth; only ALL WS symbols stale is."""
    from core.marketdata.ws_quality_guard import WsQualityGuard, WsQualitySample

    guard = WsQualityGuard(enabled=True, min_samples=4)
    for i in range(20):
        decision = guard.observe(WsQualitySample(
            ts=float(i * 5), feed_healthy=True, hub_healthy=True,
            ws_stale_symbol_count=4, ws_symbol_count=6, last_tick_age_ms=500.0,
        ))
    assert decision.state == "ws"
    assert decision.force_rest is False


def test_all_stale_symbols_still_degrade_the_guard():
    from core.marketdata.ws_quality_guard import WsQualityGuard, WsQualitySample

    guard = WsQualityGuard(enabled=True, min_samples=4)
    decision = None
    for i in range(8):
        decision = guard.observe(WsQualitySample(
            ts=float(i * 5), feed_healthy=True, hub_healthy=True,
            ws_stale_symbol_count=6, ws_symbol_count=6, last_tick_age_ms=500.0,
        ))
    assert decision.state == "degraded"
    assert decision.force_rest is True


def test_stale_with_unknown_symbol_count_stays_conservative():
    from core.marketdata.ws_quality_guard import WsQualityGuard, WsQualitySample

    guard = WsQualityGuard(enabled=True, min_samples=4)
    decision = None
    for i in range(8):
        decision = guard.observe(WsQualitySample(
            ts=float(i * 5), feed_healthy=True, hub_healthy=True,
            ws_stale_symbol_count=1, ws_symbol_count=0, last_tick_age_ms=500.0,
        ))
    assert decision.state == "degraded"
