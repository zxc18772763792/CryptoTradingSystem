"""Atomic paper<->live runtime-mode switch for registered strategy instances."""
from __future__ import annotations

import pytest


class _FakeStrategy:
    def __init__(self, mode="paper"):
        self._runtime_mode = mode
        self.params = {"runtime_mode": mode}
        self.metadata = {"runtime_mode": mode}
        self._running = False

    @property
    def is_running(self):
        return self._running

    @property
    def runtime_mode(self):
        return self._runtime_mode


class _FakeConfig:
    def __init__(self, mode="paper"):
        self.params = {"runtime_mode": mode, "account_id": "strategy_x"}
        self.metadata = {"runtime_mode": mode}


def _make_manager(monkeypatch, mode="paper"):
    from core.strategies import strategy_manager as mgr
    from core.trading.account_manager import account_manager as am

    strat = _FakeStrategy(mode)
    cfg = _FakeConfig(mode)
    monkeypatch.setitem(mgr._strategies, "X", strat)
    monkeypatch.setitem(mgr._configs, "X", cfg)
    monkeypatch.setattr(mgr, "_strategy_account_id", lambda name: "strategy_x")
    monkeypatch.setattr(mgr, "get_strategy_runtime_mode", lambda name: strat._runtime_mode)
    modes = {"strategy_x": mode}
    monkeypatch.setattr(am, "set_mode", lambda aid, m: modes.__setitem__(aid, m) or True)
    return mgr, strat, cfg, modes


def test_switch_paper_to_live_rewires_all_sites(monkeypatch):
    mgr, strat, cfg, modes = _make_manager(monkeypatch, "paper")
    res = mgr.set_strategy_runtime_mode("X", "live")
    assert res["ok"] and res["changed"] and res["runtime_mode"] == "live"
    assert strat._runtime_mode == "live"
    assert strat.params["runtime_mode"] == "live"
    assert strat.metadata["runtime_mode"] == "live"
    assert cfg.params["runtime_mode"] == "live"
    assert cfg.metadata["runtime_mode"] == "live"
    assert modes["strategy_x"] == "live"
    # stale alias keys purged so they cannot override later
    assert "trading_mode" not in cfg.params and "mode" not in cfg.params


def test_switch_refused_while_running(monkeypatch):
    mgr, strat, cfg, modes = _make_manager(monkeypatch, "paper")
    strat._running = True
    res = mgr.set_strategy_runtime_mode("X", "live")
    assert res["ok"] is False and res["reason"] == "running"
    assert strat._runtime_mode == "paper"  # unchanged
    assert modes["strategy_x"] == "paper"


def test_switch_same_mode_is_noop(monkeypatch):
    mgr, strat, cfg, modes = _make_manager(monkeypatch, "live")
    res = mgr.set_strategy_runtime_mode("X", "live")
    assert res["ok"] is True and res["changed"] is False


def test_switch_missing_instance(monkeypatch):
    from core.strategies import strategy_manager as mgr
    res = mgr.set_strategy_runtime_mode("NOPE_DOES_NOT_EXIST", "live")
    assert res["ok"] is False and res["reason"] == "not_found"
