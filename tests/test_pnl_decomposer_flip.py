from datetime import datetime

import pytest

from core.accounting.pnl_decomposer import PnLDecomposer


def test_long_to_short_flip_preserves_overfill_qty_and_closed_snapshot() -> None:
    pnl = PnLDecomposer()
    opened_at = datetime(2026, 1, 1, 0, 0, 0)
    closed_at = datetime(2026, 1, 1, 0, 1, 0)

    pnl.on_fill("BTC/USDT", "buy", 2.0, 100.0, timestamp=opened_at)
    pnl.on_fill("BTC/USDT", "sell", 3.0, 110.0, fee=3.0, slippage_cost=0.6, timestamp=closed_at)

    open_position = pnl.position_snapshot("BTC/USDT")
    assert open_position is not None
    assert open_position["side"] == "short"
    assert open_position["qty"] == pytest.approx(1.0)
    assert open_position["entry_price"] == pytest.approx(110.0)
    assert open_position["avg_entry_price"] == pytest.approx(110.0)

    closed = pnl.closed_trades()
    assert len(closed) == 1
    assert closed[0]["side"] == "long"
    assert closed[0]["qty"] == pytest.approx(2.0)
    assert closed[0]["entry_price"] == pytest.approx(100.0)
    assert closed[0]["avg_entry_price"] == pytest.approx(100.0)
    assert closed[0]["realized"]["gross_pnl"] == pytest.approx(20.0)
    assert closed[0]["realized"]["fee"] == pytest.approx(2.0)
    assert closed[0]["realized"]["slippage_cost"] == pytest.approx(0.4)
    assert closed[0]["realized"]["net_pnl"] == pytest.approx(17.6)


def test_short_to_long_flip_preserves_overfill_qty_and_realized_pnl() -> None:
    pnl = PnLDecomposer()

    pnl.on_fill("ETH/USDT", "sell", 5.0, 50.0)
    pnl.on_fill("ETH/USDT", "buy", 7.0, 40.0, fee=7.0, slippage_cost=1.4)

    open_position = pnl.position_snapshot("ETH/USDT")
    assert open_position is not None
    assert open_position["side"] == "long"
    assert open_position["qty"] == pytest.approx(2.0)
    assert open_position["entry_price"] == pytest.approx(40.0)
    assert open_position["avg_entry_price"] == pytest.approx(40.0)

    closed = pnl.closed_trades()
    assert len(closed) == 1
    assert closed[0]["side"] == "short"
    assert closed[0]["qty"] == pytest.approx(5.0)
    assert closed[0]["entry_price"] == pytest.approx(50.0)
    assert closed[0]["avg_entry_price"] == pytest.approx(50.0)
    assert closed[0]["realized"]["gross_pnl"] == pytest.approx(50.0)
    assert closed[0]["realized"]["fee"] == pytest.approx(5.0)
    assert closed[0]["realized"]["slippage_cost"] == pytest.approx(1.0)
    assert closed[0]["realized"]["net_pnl"] == pytest.approx(44.0)


def test_full_close_closed_trade_keeps_qty_and_entry_prices() -> None:
    pnl = PnLDecomposer()

    pnl.on_fill("SOL/USDT", "buy", 1.0, 100.0)
    pnl.on_fill("SOL/USDT", "buy", 3.0, 120.0)
    pnl.on_fill("SOL/USDT", "sell", 4.0, 130.0)

    assert pnl.position_snapshot("SOL/USDT") is None

    closed = pnl.closed_trades()
    assert len(closed) == 1
    assert closed[0]["qty"] == pytest.approx(4.0)
    assert closed[0]["entry_price"] == pytest.approx(115.0)
    assert closed[0]["avg_entry_price"] == pytest.approx(115.0)
    assert closed[0]["realized"]["gross_pnl"] == pytest.approx(60.0)
