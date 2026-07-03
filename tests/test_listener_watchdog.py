"""Tests for the "alive but headless" listener watchdog (policy + probe)."""
from __future__ import annotations

import socket

from core.utils.listener_watchdog import (
    LISTENER_DEAD_EXIT_CODE,
    ListenerWatchdogPolicy,
    probe_listener,
)


def test_policy_never_trips_before_first_success():
    # A slow cold start (listener not bound yet) must not trip the watchdog.
    policy = ListenerWatchdogPolicy(max_consecutive_failures=3)
    for _ in range(50):
        assert policy.observe(False) is False
    assert policy.armed is False


def test_policy_trips_after_consecutive_failures_once_armed():
    policy = ListenerWatchdogPolicy(max_consecutive_failures=3)
    assert policy.observe(True) is False  # armed
    assert policy.observe(False) is False  # 1/3
    assert policy.observe(False) is False  # 2/3
    assert policy.observe(False) is True   # 3/3 -> exit


def test_policy_resets_failure_run_on_success():
    policy = ListenerWatchdogPolicy(max_consecutive_failures=3)
    policy.observe(True)
    policy.observe(False)
    policy.observe(False)
    assert policy.observe(True) is False  # recovered
    assert policy.consecutive_failures == 0
    # A fresh run of failures is required to trip.
    assert policy.observe(False) is False
    assert policy.observe(False) is False
    assert policy.observe(False) is True


def test_probe_listener_true_when_listening_false_when_closed():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        assert probe_listener("127.0.0.1", port, timeout_sec=2.0) is True
    finally:
        srv.close()
    # Same port, listener gone -> dead. (Nothing else can grab it that fast.)
    assert probe_listener("127.0.0.1", port, timeout_sec=1.0) is False


def test_exit_code_matches_launcher_restart_contract():
    # The launchers' bounded restart loops key on exactly this exit code; if it
    # changes, scripts/market_ws_*.ps1 must change with it.
    assert LISTENER_DEAD_EXIT_CODE == 64
