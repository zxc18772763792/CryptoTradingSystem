from types import SimpleNamespace
import asyncio
from unittest.mock import AsyncMock

import pytest

from core.trading.equity_attribution import build_paper_equity_attribution


def test_strategy_contributions_reconcile_with_explicit_carry_forward():
    result = build_paper_equity_attribution(
        equity=10117, initial_equity=10000,
        closed_positions=[SimpleNamespace(strategy="agent", realized_pnl=20)],
        open_positions=[SimpleNamespace(strategy="trend", unrealized_pnl=-2)],
        fee_rows=[{"strategy": "agent", "paper_fee_usd": 1}], total_fees=1,
    )
    assert result["attributed_pnl"] == pytest.approx(17)
    assert result["carry_forward_difference"] == pytest.approx(100)
    assert result["equity"] == pytest.approx(result["initial_equity"] + result["attributed_pnl"] + result["carry_forward_difference"])
    assert result["strategies"][0]["strategy"] == "agent"
    assert result["strategies"][0]["net_pnl"] == pytest.approx(19)


def test_missing_strategy_and_fee_metadata_still_reconcile():
    result = build_paper_equity_attribution(
        equity=9998, initial_equity=10000,
        closed_positions=[], open_positions=[], fee_rows=[], total_fees=2,
    )
    assert result["carry_forward_difference"] == pytest.approx(0)
    assert result["strategies"][0]["strategy"] == "未归属手续费"
    assert result["strategies"][0]["net_pnl"] == -2


def test_attribution_route_keeps_live_account_out_of_paper_ledger(monkeypatch):
    from web.api import trading_balances as module
    monkeypatch.setattr(module.trading_api.execution_engine, "is_paper_mode", lambda: False)
    getter = AsyncMock(side_effect=AssertionError("must not value paper ledger in live mode"))
    monkeypatch.setattr(module.trading_api.execution_engine, "get_account_equity_snapshot", getter)
    result = asyncio.run(module.get_balance_attribution())
    assert result["mode"] == "live"
    getter.assert_not_called()


def test_attribution_route_reads_only_paper_positions(monkeypatch):
    from web.api import trading_balances as module
    api = module.trading_api
    monkeypatch.setattr(api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(api.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=10010))
    monkeypatch.setattr(api.execution_engine, "_paper_fee_applied_orders", set())
    monkeypatch.setattr(api.execution_engine, "_paper_total_fees_usd", 0)
    monkeypatch.setattr(api.execution_engine, "_paper_fee_ledger", {})
    monkeypatch.setattr(api.settings, "PAPER_INITIAL_EQUITY", 10000)
    def positions(*, scope):
        assert scope == "paper"
        return []
    monkeypatch.setattr(api.position_manager, "get_closed_positions", positions)
    monkeypatch.setattr(api.position_manager, "get_all_positions", positions)
    result = asyncio.run(module.get_balance_attribution())
    assert result["carry_forward_difference"] == 10
