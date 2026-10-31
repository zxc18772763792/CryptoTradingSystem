from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.research_telegram_new_kols import (
    availability, call_group, claim_audit, classify_post, evaluate_rows, parse_plan, resolve_symbol, signal_rows,
    simulate_plan, verdict,
)
from scripts.review_telegram_signals import SymbolPanel

FUTURES = {"BTC", "ETH", "SOL", "LINK", "KMNO", "KERNEL", "2Z", "SUI", "PHA", "DOGE", "1000PEPE"}
SPOT = {"OSMO", "LINK"}


def post(text, mid=1, published="2026-06-01T10:00:00+00:00", edited=None, reply_to=None):
    return {"message_id": mid, "text": text, "published_at": published, "edited_at": edited,
            "reply_to_id": reply_to, "forwarded": False, "text_sha256": "x"}


def label(text, **kw):
    return classify_post(post(text, **kw), FUTURES, SPOT)


def only_signal(result):
    assert len(result["signals"]) == 1, result
    s = result["signals"][0]
    return s["symbol"], s["venue"], s["side"]


@pytest.mark.parametrize("text, kind, expected", [
    ("❗️Buying some $LINK here!\n\nBroke the triangle.", "direct_call", ("LINK", "futures", 1)),
    ("📊 FUTURES (BINANCE)\n\n#KMNOUSDT\n\nLONG Below : 0.02100\n\nTAKE PROFIT: 0.02200 | 0.02300\n\nSl 0.018700",
     "trade_plan", ("KMNO", "futures", 1)),
    ("KERNEL buy and hold big Move soon 🤑🚀", "direct_call", ("KERNEL", "futures", 1)),
    ("2Z buy and hold big Move soon 🤑🚀", "direct_call", ("2Z", "futures", 1)),
    ("OSMOUSDT buy and hold big Move soon", "direct_call", ("OSMO", "spot", 1)),
    ("$PEPE buy and hold big Move soon", "direct_call", ("1000PEPE", "futures", 1)),
    ("SHORT SIGNAL | $PHA\n\nEntry : 0.05\nStoploss : 0.06", "trade_plan", ("PHA", "futures", -1)),
    ("Long #DOGE 10x lev here ↗️", "direct_call", ("DOGE", "futures", 1)),
])
def test_explicit_calls_are_parsed_with_symbol_venue_and_side(text, kind, expected):
    result = label(text)
    assert result["label"] == kind
    assert only_signal(result) == expected


@pytest.mark.parametrize("text", [
    "GTCUSDT profit 75% 🤑🚀",
    "🏁 $LINK reached the first target, printed +13% profit",
    "BTC trade tp hit",
    "$BTC LONG IN PROFIT 🤝",
    "Join Copy trading here : https://example.invalid/?invite_code=X",
])
def test_results_and_promotions_never_become_calls(text):
    assert label(text)["label"] == "recap_or_promo"


@pytest.mark.parametrize("text", [
    "🟢 #BTC Liquidated Short: $66.7K at $77,547 - hyperlens",
    "Nothing new here, the company has been selling $BTC every month since June.",
    "A major whale opens a 10x leveraged long worth $4.5M (41.67M $DOGE), the largest DOGE long.",
    "Short #PHA here is tantamount to suicide.",
    "$266M in long positions were liquidated as $BTC plunged below $76,000.",
])
def test_news_and_positioning_phrases_are_not_calls(text):
    result = label(text)
    assert result["label"] not in ("direct_call", "trade_plan"), result


def test_news_footer_is_stripped_instead_of_marking_everything_promo():
    result = label("🚨 Exchange X secures a licence.\n\nJOIN FREE VIP GROUP - @somebot")
    assert result == {**result, "label": "other", "reason": "news"}


def test_btc_is_inferred_only_from_a_five_digit_level_in_a_call_without_ticker():
    result = label("TAKING A SHORT HERE AT $62900 ! \nSL -  $63510")
    assert result["label"] == "trade_plan" and result["inferred_symbol"]
    assert only_signal(result) == ("BTC", "futures", -1)
    assert label("I WILL TAKE A SHORT OF GOLD HERE $4702 \nSTOP LOSS - $4710")["label"] == "direction_without_symbol"


def test_price_level_that_contradicts_the_ticker_is_ambiguous():
    result = label("Will long here with super tight stop loss $65690\n\nSAME TIME # ETH #SOL")
    assert result["label"] == "ambiguous" and result["reason"] == "price_level_conflicts_with_ticker"


def test_followups_conditionals_and_both_directions_are_kept_apart():
    assert label("I am holding Bitcoin LONG from $78,000 and adding.")["label"] == "followup_view"
    cond = label("❗️Buying some $SUI here!\n\nPlaced bids lower in case we get a retest.")
    assert cond["label"] == "direct_call" and cond["conditional"]
    assert call_group(cond) == "conditional_call"
    assert call_group(label("❗️Buying some $SUI here!")) == "fresh_call"
    assert label("Long $ETH now, short $SOL here")["label"] == "ambiguous"


def test_unknown_ticker_keeps_the_call_but_marks_it_untradable():
    assert resolve_symbol("CASHCAT", FUTURES, SPOT) == (None, "not_in_local_universe")
    assert resolve_symbol("Bitcoin", FUTURES, SPOT) == ("BTC", "futures")


def test_availability_uses_edit_time_and_flags_late_edits():
    at, late = availability(post("x", edited="2026-06-01T10:00:40+00:00"))
    assert at == pd.Timestamp("2026-06-01T10:00:40Z") and not late
    at, late = availability(post("x", edited="2026-06-01T12:00:00+00:00"))
    assert at == pd.Timestamp("2026-06-01T12:00:00Z") and late
    assert availability(post("x")) == (pd.Timestamp("2026-06-01T10:00:00Z"), False)


def synthetic_panel(opens, start="2026-01-01"):
    index = pd.date_range(start, periods=len(opens), freq="h", tz="UTC")
    opens = np.asarray(opens, float)
    bars = pd.DataFrame({"open": opens, "high": opens * 1.001, "low": opens * .999, "close": opens, "volume": 1.},
                        index=index)
    return SymbolPanel(bars, pd.DataFrame({"funding_rate": 0.}, index=index))


def sealed_with(text, available, side_symbol=("ETH", "futures"), late=False):
    result = classify_post(post(text), FUTURES, SPOT)
    result["signals"][0].update(symbol=side_symbol[0], venue=side_symbol[1])
    return {"channels": {"demo": {"posts": [{**result, "available_at": available, "late_edit": late,
                                             "published_at": available, "forwarded": False, "reply_to_id": None}]}}}


def test_short_is_entered_after_availability_and_scored_entry_minus_exit():
    opens = np.full(24 * 80, 100.0)
    signal_hour = 24 * 40
    opens[signal_hour + 2:] = 90.0  # the drop happens after the post becomes tradable
    panel = synthetic_panel(opens)
    available = str(panel.index[signal_hour] + pd.Timedelta(seconds=30))
    signals = signal_rows(sealed_with("SHORT SIGNAL | $ETH\nEntry : 100\nStoploss : 110", available))
    rows, status = evaluate_rows(signals, {("ETH", "futures"): panel}, horizons=(4,))
    (row,) = rows
    # available hh:00:30 -> +60s -> next full hour, never the bar the post appeared in
    assert row["entry_at"] == panel.index[signal_hour + 1]
    assert row["gross"] == pytest.approx(.10)
    assert row["net"] == pytest.approx(.10 - .002)
    assert row["run_up_24h"] == pytest.approx(0.0)
    # null: same coin/side/horizon at other starts, overlap with the signal window excluded
    assert row["null_n"] > 50 and row["null_mean"] < row["net"]
    assert status[("demo", 4, "ok")] == 1


def test_spot_only_short_and_missing_symbol_are_reported_not_scored():
    panel = synthetic_panel(np.full(24 * 80, 100.0))
    available = str(panel.index[24 * 40])
    signals = signal_rows(sealed_with("Short #OSMO here", available, ("OSMO", "spot")))
    rows, status = evaluate_rows(signals, {("OSMO", "spot"): panel}, horizons=(4,))
    assert rows == [] and status[("demo", 4, "short_without_perp")] == 1
    missing = signal_rows(sealed_with("Long #ETH here", available, (None, "not_in_local_universe")))
    rows, status = evaluate_rows(missing, {}, horizons=(4,))
    assert rows == [] and status[("demo", 4, "symbol_not_in_local_universe")] == 1


def test_verdict_requires_sample_cluster_interval_and_robustness():
    good = {"n_independent": 40, "bh_q": .05, "p_greater_than_null": .01, "excess_week_bootstrap_95": [.001, .02],
            "mean_without_best": .004, "mean_net_80bps": .002}
    assert verdict(good) == "passes_screen"
    assert verdict({**good, "n_independent": 29}) == "insufficient_sample"
    assert verdict({**good, "excess_week_bootstrap_95": [-.001, .02]}) == "no_convincing_edge"
    assert verdict({**good, "excess_week_bootstrap_95": None}) == "no_convincing_edge"
    assert verdict({**good, "mean_without_best": -.001}) == "no_convincing_edge"


def test_claim_audit_compares_claimed_profit_with_unlevered_move():
    opens = np.full(24 * 20, 1.0)
    panel = synthetic_panel(opens)
    panel.high[24 * 5 + 1: 24 * 5 + 10] = 1.10  # +10% peak after entry
    available = str(panel.index[24 * 5] + pd.Timedelta(seconds=5))
    sealed = sealed_with("Long #ETH here", available)
    sealed["channels"]["demo"]["posts"][0]["message_id"] = 7
    claim_time = str(panel.index[24 * 6])
    histories = {"demo": [post("ETHUSDT profit 50% 🤑🚀", mid=8, published=claim_time, reply_to=7)]}
    (row,) = claim_audit(sealed, histories, {("ETH", "futures"): panel})
    assert row["claimed"] == pytest.approx(.5)
    assert row["mfe_to_claim"] == pytest.approx(.10)
    assert row["implied_leverage"] == pytest.approx(5.0)


@pytest.mark.parametrize("text, tp1, sl", [
    ("#GLMUSDT\n\nLONG Below : 0.08750\n\nTAKE PROFIT:0.09000 |0.09300\n\nSl .0.08400", .09, .084),
    ("ETHUSDT — SHORT 🔴\n\n🎯 Entry: ~2,700 or above\n✅ TP1• 2,650\n⚠️ SL:\n• 2,750", 2650, 2750),
    ("LONG SIGNAL | #BTC\n\nEntry : $61,500\nTarget : $65,000 - 66,xxx 🎯\nStoploss : <$58,000 ⛔️", 65000, 58000),
    ("EVAAUSDT – LONG\n\nTargets: 🎯\n\n**0.77\n0.8**\n\nStop-loss: 0.6", .77, .6),
    ("Long #REZ here . Target : 80 - 100%", None, None),
    ("TAKING A SHORT HERE AT $62900 ! \nSL -  $63510\nTP - 1/1 and 1/2 !", None, 63510),
])
def test_plan_parser_reads_first_target_and_stop_but_not_percent_or_r_multiples(text, tp1, sl):
    assert parse_plan(text) == {"tp1": pytest.approx(tp1) if tp1 else None, "sl": pytest.approx(sl) if sl else None}


def plan_panel(opens, highs, lows):
    index = pd.date_range("2026-01-01", periods=len(opens), freq="h", tz="UTC")
    bars = pd.DataFrame({"open": opens, "high": highs, "low": lows, "close": opens, "volume": 1.}, index=index)
    return SymbolPanel(bars, pd.DataFrame({"funding_rate": 0.}, index=index))


def test_plan_simulation_takes_stop_first_and_fills_gaps_at_the_open():
    n = 200
    opens, highs, lows = np.full(n, 100.), np.full(n, 100.5), np.full(n, 99.5)
    highs[3], lows[3] = 106., 94.  # both touched in one bar -> stop first
    p = plan_panel(opens, highs, lows)
    r = simulate_plan(p, p.index[0], 1, tp1=105., sl=95.)
    assert r["exit_reason"] == "stop" and r["gross"] == pytest.approx(-.05) and r["held_hours"] == 4
    opens2, highs2, lows2 = np.full(n, 100.), np.full(n, 100.5), np.full(n, 99.5)
    opens2[5], highs2[5], lows2[5] = 90., 90.5, 89.5  # gap through the stop
    r = simulate_plan(plan_panel(opens2, highs2, lows2), p.index[0], 1, tp1=105., sl=95.)
    assert r["exit_reason"] == "stop" and r["gross"] == pytest.approx(-.10)
    r = simulate_plan(p, p.index[0], -1, tp1=95., sl=105.)  # short: same bar -> stop (high) first
    assert r["exit_reason"] == "stop" and r["gross"] == pytest.approx(-.05)
    assert simulate_plan(p, p.index[0], 1, tp1=99., sl=95.)["status"] == "invalid_geometry_at_entry"
    assert simulate_plan(p, p.index[0], 1, tp1=180., sl=95.)["status"] == "implausible_levels"
    quiet = plan_panel(np.full(n, 100.), np.full(n, 100.5), np.full(n, 99.5))
    r = simulate_plan(quiet, quiet.index[0], 1, tp1=105., sl=95.)
    assert r["exit_reason"] == "timeout" and r["held_hours"] == 168 and r["net"] == pytest.approx(-.002)
