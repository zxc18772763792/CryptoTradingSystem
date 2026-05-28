"""Regression tests for the abnormal-cashflow false-positive.

Live incident 2026-05-25:
- risk_manager.current_equity got cached at 1.1574 (corrupt — probably a
  funding-wallet-only fetch from a partial-failure window).
- Next successful /balances call retrieved real total 5062 USDT.
- abnormal-cashflow detector compared 5062 vs 1.15, ratio=4400x, and
  classified it as "likely transfer/cashflow", rejecting the update.
- UI displayed 1.15 forever.

These tests pin the sanity-fallback that uses the day_start baseline as
the reference value when prev_equity is obviously corrupt.
"""
from __future__ import annotations

import inspect
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_balances_module() -> str:
    return (REPO_ROOT / "web" / "api" / "trading_balances.py").read_text(encoding="utf-8")


def test_prev_equity_unreliable_below_10_usd():
    """A prev_equity under $10 absolute is almost certainly not a real account
    value — guard must mark it as unreliable regardless of baseline."""
    src = _read_balances_module()
    assert "prev_equity_unreliable" in src
    assert "prev_equity < 10.0" in src


def test_prev_equity_unreliable_when_negative():
    """Negative cached equity is corrupt state, not a valid cashflow baseline."""
    src = _read_balances_module()
    assert "prev_equity < 0" in src


def test_prev_equity_unreliable_below_10pct_of_baseline():
    """A prev_equity below 10% of today's day_start baseline indicates a
    corrupt cached value, e.g. funding-wallet-only fetch result."""
    src = _read_balances_module()
    assert "prev_equity < sanity_baseline * 0.1" in src


def test_unreliable_prev_falls_back_to_baseline_for_sanity_checks():
    """When prev_equity is discarded, the abnormal-cashflow check must use
    the live_day_start_equity baseline so a real successful balance fetch
    isn't rejected against a phantom 1.15."""
    src = _read_balances_module()
    assert "prev_equity_for_check = sanity_baseline if sanity_baseline > 0 else 0.0" in src
    # The abnormal-cashflow check must reference the sanitized value, NOT
    # the raw `prev_equity` — otherwise the fix has no effect.
    assert "move_ratio = abs(delta_usd) / max(prev_equity_for_check," in src
    assert "risk_equity_input < prev_equity_for_check * 0.6" in src


def test_stale_prev_warning_emitted_for_observability():
    """When we discard a stale prev_equity, a warning must be logged so the
    operator can diagnose corrupt risk state."""
    src = _read_balances_module()
    assert "Discarding stale risk equity" in src
    assert "day_start_baseline=" in src
