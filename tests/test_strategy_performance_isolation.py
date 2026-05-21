"""Per-strategy performance display isolation.

Two strategies on isolated accounts (the codebase default — each gets
``strategy_<name>``) must show return % against THEIR OWN account
equity, not the global ``risk_manager.get_risk_report().equity.current``.
Otherwise every strategy on its own account shares one baseline and the
UI misattributes capital between them — an isolation gap between
execution (correctly scoped via account_id+strategy_key) and the
performance display.

Locks the ``_strategy_account_equity`` helper + the per-caller hookup.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import patch

import pytest

from web.api.strategies import (
    _build_strategy_performance_view,
    _strategy_account_equity,
)


def test_helper_returns_none_for_shared_main_account():
    """When a strategy runs on the shared ``main`` account, the helper
    returns None so callers fall back to the global equity."""
    with patch(
        "web.api.strategies.strategy_manager._strategy_account_id",
        return_value="main",
    ):
        assert _strategy_account_equity("ma_shared") is None


def test_helper_returns_none_when_account_cache_empty():
    """A strategy on its own account but with no cached equity yet must
    return None so the caller falls back to the global. Avoids dividing
    by 0 for paper-mode strategies that haven't received a wallet
    snapshot yet."""
    with patch(
        "web.api.strategies.strategy_manager._strategy_account_id",
        return_value="strategy_ma_isolated",
    ), patch(
        "web.api.strategies.execution_engine._get_cached_equity_value",
        return_value=0.0,
    ):
        assert _strategy_account_equity("ma_isolated") is None


def test_helper_returns_account_equity_when_isolated_and_cached():
    with patch(
        "web.api.strategies.strategy_manager._strategy_account_id",
        return_value="strategy_alpha",
    ), patch(
        "web.api.strategies.execution_engine._get_cached_equity_value",
        return_value=1234.5,
    ):
        assert _strategy_account_equity("alpha") == 1234.5


def test_two_isolated_strategies_get_different_denominators():
    """Two strategies on different isolated accounts must each see their
    own account equity as the return-% denominator. This is the heart
    of the isolation fix — if the helper or the call site regresses,
    both strategies fall back to the global value and the UI silently
    misattributes capital."""
    by_account = {
        "strategy_A": 1000.0,
        "strategy_B": 500.0,
    }
    by_strategy = {
        "A": "strategy_A",
        "B": "strategy_B",
    }

    def _account_for(name: str) -> str:
        return by_strategy[name]

    def _equity_for(account_id: Optional[str] = None) -> float:
        return float(by_account.get(account_id or "", 0.0))

    with patch(
        "web.api.strategies.strategy_manager._strategy_account_id",
        side_effect=_account_for,
    ), patch(
        "web.api.strategies.execution_engine._get_cached_equity_value",
        side_effect=_equity_for,
    ):
        eq_a = _strategy_account_equity("A")
        eq_b = _strategy_account_equity("B")
    assert eq_a == 1000.0
    assert eq_b == 500.0
    assert eq_a != eq_b, "strategies on different accounts must get different equities"


def test_performance_view_uses_passed_equity_as_denominator():
    """``_build_strategy_performance_view`` honors the ``current_equity``
    arg as the return-% denominator — the call site can therefore swap
    in per-strategy equity without touching the view function itself."""
    # Bypass any global lookups inside the view.
    fake_config_alloc = type("Cfg", (), {"params": {}, "allocation": 1.0})()
    with patch(
        "web.api.strategies.strategy_manager._configs",
        {"alpha": fake_config_alloc},
        create=True,
    ):
        # No trades; a strategy with 100% allocation on a 2000-equity
        # account should show capital_base == 2000.
        view = _build_strategy_performance_view(
            name="alpha",
            info={"timeframe": "1h", "allocation": 1.0},
            runtime_mode="paper",
            trades=[],
            seed_performance={},
            current_equity=2000.0,
            include_equity=False,
            timeframe="1h",
        )
    assert view["capital_base"] == pytest.approx(2000.0, rel=1e-9), (
        f"expected capital_base=2000, got {view['capital_base']}"
    )
    # And the sources must record that the denominator came from the
    # current_equity * allocation path, not the fallback.
    assert view["sources"]["capital_base"] == "current_equity_allocation"
