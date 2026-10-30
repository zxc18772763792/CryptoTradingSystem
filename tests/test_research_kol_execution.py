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


def test_liquidation_forfeits_the_whole_margin():
    e = run(Plan(1, True, [], [101], [1.0], 90, False, liq_leverage=20),
            [[100, 100.1, 99.9, 100], [100, 100, 95.4, 96]])
    assert e.exit_reason == "liquidated"
    assert e.pnl == pytest.approx(-1 / 20 - 2 * COST)


def test_unlevered_short_without_stop_loses_at_most_its_notional():
    e = run(Plan(-1, True, [], [90], [1.0], None, False), [[100, 100.1, 99.9, 100], [100, 230, 99, 220]])
    assert e.exit_reason == "liquidated"
    assert e.pnl == pytest.approx(-1.0 - 2 * COST)


def reference(side, legs, targets, weights, stop, be, rows):
    """Deliberately naive re-implementation: adverse moves first inside a bar, then targets."""
    fav = (lambda hi, lo: hi) if side > 0 else (lambda hi, lo: lo)
    adv = (lambda hi, lo: lo) if side > 0 else (lambda hi, lo: hi)
    reached = (lambda p, lvl: p >= lvl) if side > 0 else (lambda p, lvl: p <= lvl)
    against = (lambda p, lvl: p <= lvl) if side > 0 else (lambda p, lvl: p >= lvl)
    filled, qty, avg, pnl, done, cur_stop = [], 0.0, None, 0.0, 0, stop
    open_legs = list(legs) if legs else None
    for i, (o, hi, lo, c) in enumerate(rows):
        if i == 0 and open_legs is None:
            filled.append(o); qty, avg, pnl = 1.0, o, -COST
            open_legs = []
        if i == 0:
            for lp in list(open_legs):
                if not reached(o, lp) or o == lp:
                    filled.append(o); open_legs.remove(lp)
            if filled and avg is None:
                avg = sum(filled) / len(filled); qty = len(filled) / len(legs); pnl = -COST * qty
        if qty == 0 and done == 0 and not filled:
            if reached(fav(hi, lo), targets[0]):
                return 0.0, "cancelled"
        for lp in sorted(open_legs, key=lambda x: -side * x):
            if against(adv(hi, lo), lp):
                w = 1 / len(legs)
                avg = (avg * qty + (o if not reached(o, lp) else lp) * w) / (qty + w) if qty else (o if not reached(o, lp) else lp)
                qty += w; pnl -= COST * w; filled.append(lp); open_legs.remove(lp)
        if qty and cur_stop is not None and against(adv(hi, lo), cur_stop):
            px = o if against(o, cur_stop) else cur_stop
            pnl += side * (px / avg - 1) * qty - COST * qty
            return pnl / (len(filled) / (len(legs) if legs else 1)), "stop"
        while qty and done < len(targets) and reached(fav(hi, lo), targets[done]):
            total = len(filled) / (len(legs) if legs else 1)
            w = qty if done == len(targets) - 1 else weights[done] * total
            px = o if reached(o, targets[done]) else targets[done]
            pnl += side * (px / avg - 1) * w - COST * w
            qty -= w; done += 1
            if done == 1:
                open_legs = []
                if be:
                    cur_stop = avg
        if qty <= 1e-12 and done:
            return pnl / (len(filled) / (len(legs) if legs else 1)), f"tp{done}"
    return None, "open"


@pytest.mark.parametrize("seed", range(300))
def test_engine_matches_naive_reference_on_random_paths(seed):
    import numpy as np

    rng = np.random.default_rng(seed)
    side = 1 if seed % 2 else -1
    n = 120
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.004, n)))
    opens = np.r_[100.0, closes[:-1]]
    hi = np.maximum(opens, closes) * (1 + rng.uniform(0, 0.003, n))
    lo = np.minimum(opens, closes) * (1 - rng.uniform(0, 0.003, n))
    rows = [list(map(float, r)) for r in zip(opens, hi, lo, closes)]
    kind = seed % 3
    legs = [] if kind == 0 else [100 - side * 0.5] if kind == 1 else [100 - side * 0.3, 100 - side * 0.9]
    targets = [100 + side * d for d in (0.8, 1.6, 2.5)]
    weights = [1 / 3] * 3
    stop = 100 - side * 2.0
    be = seed % 5 == 0
    # skip paths with a bar that touches both a target and the stop: ordering is unknowable there
    t1 = targets[0]
    if any((r[1] >= t1 and r[2] <= stop) if side > 0 else (r[2] <= t1 and r[1] >= stop) for r in rows):
        pytest.skip("ambiguous bar")
    want, why = reference(side, legs, targets, weights, stop, be, rows)
    e = run(Plan(side, kind == 0, legs, targets, weights, stop, be), rows)
    if why == "cancelled":
        assert e.status == "cancelled"
    elif why == "open":
        assert e.status in ("open", "pending")
    else:
        assert e.status == "closed"
        filled = sum(w for *_, w in e.fills)
        assert e.pnl / filled == pytest.approx(want, abs=1e-9), (why, e.exit_reason, e.events)


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
