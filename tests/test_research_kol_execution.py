import pandas as pd
import pytest

from scripts.research_kol_execution import COST, Engine, Plan, numbers, parse_next10x, parse_zetoshi

T0 = pd.Timestamp("2026-01-01 00:00")
M = pd.Timedelta(minutes=1)


def bars(rows):
    return pd.DataFrame(rows, columns=["open", "high", "low", "close"],
                        index=pd.date_range(T0, periods=len(rows), freq="1min"))


def run(plan, rows, finer=None):
    eng = Engine(plan, T0, T0 + pd.Timedelta(days=1), T0 + pd.Timedelta(hours=1), finer=finer)
    eng.run(bars(rows), M)
    return eng


def test_market_long_two_targets_even_split():
    e = run(Plan(1, True, [], [101, 102], [0.5, 0.5], 95, False),
            [[100, 100.5, 99.5, 100], [100, 101.2, 99.9, 101], [101, 102.1, 100.8, 102]])
    assert e.status == "closed" and e.exit_reason == "tp2"
    assert e.pnl == pytest.approx(0.5 * 0.01 + 0.5 * 0.02 - 2 * COST)


def test_tp1_before_limit_fill_cancels():
    e = run(Plan(1, False, [99], [101], [1.0], 95, False), [[100, 100.2, 99.5, 100], [100, 101.5, 100, 101]])
    assert e.status == "cancelled" and e.exit_reason == "tp1_before_entry" and not e.fills


def test_marketable_limit_fills_at_open():
    e = run(Plan(1, False, [100], [103], [1.0], 95, False), [[99, 99.5, 98.8, 99.2]])
    assert e.fills[0][1] == 99


def test_same_bar_target_and_stop_takes_stop_without_finer_data():
    e = run(Plan(1, True, [], [101], [1.0], 99, False), [[100, 100.1, 99.9, 100], [100, 101.5, 98.5, 100]])
    assert e.exit_reason == "stop" and e.ambiguous_unresolved == 1
    assert e.pnl == pytest.approx(-0.01 - 2 * COST)


def test_finer_bars_resolve_target_first():
    fine = bars([[100, 101.2, 99.95, 101], [101, 101, 98.5, 99]])
    e = run(Plan(1, True, [], [101], [1.0], 99, False), [[100, 100.1, 99.9, 100], [100, 101.5, 98.5, 100]],
            finer=lambda t, bl: fine)
    assert e.exit_reason == "tp1" and e.refined == 1


def test_breakeven_after_tp1():
    e = run(Plan(1, True, [], [101, 103], [0.5, 0.5], 98, True),
            [[100, 100.1, 99.9, 100], [100, 101.1, 100.2, 101], [101, 101, 99.8, 100]])
    assert e.exit_reason == "stop_be"
    assert e.pnl == pytest.approx(0.5 * 0.01 - 2 * COST)


def test_short_range_partial_fill_then_target():
    e = run(Plan(-1, False, [101, 102], [99], [1.0], 105, False),
            [[100, 100.2, 99.9, 100], [100, 101.3, 100, 101], [101, 101, 98.9, 99]])
    assert len(e.fills) == 1 and e.fills[0][1] == 101 and e.exit_reason == "tp1"
    assert e.pnl / 0.5 == pytest.approx((1 - 99 / 101) - 2 * COST)


def test_liquidation_before_wide_stop():
    e = run(Plan(1, True, [], [101], [1.0], 90, False, liq_leverage=50),
            [[100, 100.1, 99.9, 100], [100, 100, 97.5, 98]])
    assert e.exit_reason == "liquidated"


def test_number_parsing():
    assert numbers("TP 74.5K") == [74500.0]
    assert numbers("Target : 65,xxx - 66,000") == [66000.0]
    assert numbers("0,84") == [0.84]
    assert numbers("LEV 25X") == []


def test_channel_templates():
    n = parse_next10x({"message_id": 1, "published_at": "2026-01-01T00:00:00+00:00", "edited_at": None,
                       "text": "#KMNOUSDT\n\nLONG Below :  0.02100\n\nMAX 3x-10x LEVERAGE Hold\n\n"
                               "TAKE PROFIT: 0.02200 0.02300 | 0.024000\n\n\nSl 0.018700"})
    assert (n.symbol, n.legs, n.targets, n.stop) == ("KMNO", [0.021], [0.022, 0.023, 0.024], 0.0187)
    z = parse_zetoshi({"message_id": 2, "published_at": "2026-01-01T00:00:00+00:00", "edited_at": None,
                       "text": "LONG - $CLO 0.07$\n\nUse 5x Maximum Leverage\n\nTake Profit 1-  0.08$\n"
                               "Take Profit 2 - 0.10$\nTake Profit 3 - 0.12$\n\nStop lose : 0.05$"})
    assert (z.symbol, z.side, z.legs, z.targets, z.stop, z.leverage) == ("CLO", 1, [0.07], [0.08, 0.1, 0.12], 0.05, 5.0)
