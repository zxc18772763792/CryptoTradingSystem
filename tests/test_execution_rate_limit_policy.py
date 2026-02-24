import time

import pytest

from core.execution.rate_limit_and_reconnect import RateLimitAndReconnectPolicy, RateLimitExceeded


def test_token_bucket_acquire_and_refill():
    p = RateLimitAndReconnectPolicy()
    p.configure_bucket("rest_weight", capacity=2, refill_per_sec=2.0, initial_tokens=2)

    assert p.acquire("rest_weight", cost=1) is True
    assert p.acquire("rest_weight", cost=1) is True
    assert p.acquire("rest_weight", cost=1) is False

    time.sleep(0.55)
    assert p.acquire("rest_weight", cost=1) is True


def test_penalty_blocks_until_retry():
    p = RateLimitAndReconnectPolicy()
    p.configure_bucket("order_1m", capacity=10, refill_per_sec=10.0)
    p.penalize("order_1m", retry_after_ms=300)

    assert p.acquire("order_1m", cost=1) is False
    retry_ms = p.retry_after_ms("order_1m", cost=1)
    assert retry_ms > 0

    time.sleep(0.35)
    assert p.acquire("order_1m", cost=1) is True


def test_acquire_timeout_raises():
    p = RateLimitAndReconnectPolicy()
    p.configure_bucket("ws_ctrl", capacity=1, refill_per_sec=0.1, initial_tokens=0)

    with pytest.raises(RateLimitExceeded):
        p.acquire("ws_ctrl", cost=1, wait=True, timeout_ms=50)


def test_reduce_only_mode_lifecycle():
    p = RateLimitAndReconnectPolicy()
    assert p.can_open_positions() is True
    p.set_reduce_only_cooldown(0.2, reason="quant_rules_-4400")
    assert p.can_open_positions() is False
    st = p.stats()
    assert st["mode"] == "reduce_only"
    assert "quant_rules" in st["mode_reason"]
    time.sleep(0.25)
    assert p.can_open_positions() is True
    assert p.stats()["mode"] == "normal"


def test_binance_defaults_exist():
    p = RateLimitAndReconnectPolicy()
    p.configure_binance_futures_defaults()
    st = p.stats()
    for k in ("rest_weight", "order_1m", "order_10s", "ws_ctrl"):
        assert k in st["buckets"]

