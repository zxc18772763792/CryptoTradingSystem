"""Failure cases found while auditing the October research additions."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
import json
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.ai import research_loop_v3 as loop
from core.news import x_archive
from core.news import x_pacing
from core.research import announcement_short_tracker as announcements
from core.research import coinglass_composite_tracker as composite
from core.research import xs_return_evaluation as evaluation
from core.research import xs_reversal_tracker as reversal
from core.research import upbit_caution_tracker as caution


def test_partial_composite_book_is_not_a_completed_return():
    formed = datetime(2026, 10, 11, tzinfo=timezone.utc)
    record = {"formed_at": formed.isoformat(), "main": {
        "status": "open", "turnover": 1.0,
        "longs": {"LONG": {"entry": 10.0, "score": 1.}},
        "shorts": {"SHORT": {"entry": 10.0, "score": -1.}, "MISSING_LOSER": {"entry": 10.0, "score": -2.}},
    }}

    class Client:
        async def get(self, *args, **kwargs):
            return SimpleNamespace(status_code=200, json=lambda: [])

    asyncio.run(composite.settle(Client(), record, formed + timedelta(days=9), {"LONG": 11., "SHORT": 9.}))
    assert record["main"]["status"] == "unresolved"
    assert record["main"].get("net_pct") is None
    summary = composite.summary({"books": {"2026-10-11": record}})
    assert summary["forward_completed"] == 0
    assert summary["main"]["unresolved"] == 1


def test_legacy_partial_composite_results_do_not_enter_retirement():
    record = {"main": {"status": "closed", "missing_positions": 1, "net_pct": 10.,
                       "long_leg_pct": 5., "short_leg_pct": 5., "settled_at": "2026-10-19T00:00:00+00:00"}}
    summary = composite.summary({"books": {"2026-10-11": record}})
    assert summary["forward_completed"] == 0
    assert summary["forward_mean_net_pct"] is None
    assert summary["retirement"]["n"] == 0


def test_legacy_partial_reversal_results_do_not_enter_retirement():
    record = {"status": "closed", "missing_positions": 1, "net_per_position_pct": 10.,
              "longs": {}, "shorts": {}}
    summary = reversal.summary({"days": {"2026-10-11": record}})
    assert summary["forward_completed"] == 0
    assert summary["retirement"]["n"] == 0


def _forward_world(monkeypatch, tmp_path, *, scored_days=200):
    monkeypatch.setattr(loop, "STATE_PATH", tmp_path / "state.json")
    item = {"fingerprint": "test", "name": "test", "status": "forward_observing",
            "frozen_at": "2026-10-15T12:00:00+00:00", "frozen_panel_end": "2026-10-01"}
    loop.update_state(lambda state: state["frozen"].append(item))
    dates = pd.date_range("2026-10-01", "2027-07-01")
    ctx = SimpleNamespace(dates=dates, label_ok=np.ones((len(dates), 30), dtype=bool), panel=object())
    periods_seen = []

    def context(panel, periods):
        periods_seen.append(periods)
        return object()

    monkeypatch.setattr(evaluation, "EvalContext", context)
    monkeypatch.setattr(evaluation, "evaluate", lambda *a, **kw: {
        "forward": {"status": "ok", "days": scored_days, "z": 5., "net_per_book": .01}})
    return ctx, periods_seen


def test_forward_period_starts_after_freeze_even_when_panel_is_stale(monkeypatch, tmp_path):
    ctx, seen = _forward_world(monkeypatch, tmp_path)
    loop.observe_forward(ctx, loop.ResearchLoopV3Config(), log=lambda _: None)
    assert seen[0]["forward"][0] == pd.Timestamp("2026-10-16")


def test_forward_review_waits_for_enough_scored_days(monkeypatch, tmp_path):
    ctx, _ = _forward_world(monkeypatch, tmp_path, scored_days=60)
    loop.observe_forward(ctx, loop.ResearchLoopV3Config(), log=lambda _: None)
    item = loop.load_state()["frozen"][0]
    assert item["status"] == "forward_observing"
    assert "60/182" in item["forward_note"]


def test_concurrent_loop_updates_keep_round_and_configuration(monkeypatch, tmp_path):
    path = tmp_path / "state.json"
    loop.update_state(lambda state: None, path)
    entered, release, second_done = threading.Event(), threading.Event(), threading.Event()

    def round_update(state):
        entered.set()
        assert release.wait(5)
        state["rounds"].append({"round_id": "audit"})

    def config_update():
        loop.update_state(lambda state: state["config"].update(enabled=True), path)
        second_done.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(loop.update_state, round_update, path)
        assert entered.wait(5)
        second = pool.submit(config_update)
        second_done.wait(.25)
        release.set()
        first.result(timeout=5)
        second.result(timeout=5)
    result = loop.load_state(path)
    assert result["config"]["enabled"] is True
    assert result["rounds"] == [{"round_id": "audit"}]


def test_x_archive_retries_posts_after_an_append_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(x_archive, "_seen", {})
    original_open = Path.open
    post = {"tweet_id": "123", "text": "$BTC", "timestamp": "2026-10-25T00:00:00Z"}

    def fail_append(path, mode="r", *args, **kwargs):
        if path == tmp_path / "alpha.jsonl" and mode == "a":
            raise OSError("synthetic transient disk error")
        return original_open(path, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", fail_append)
        with pytest.raises(OSError, match="transient"):
            x_archive.observe("alpha", [post], directory=tmp_path)
    assert x_archive.observe("alpha", [post], directory=tmp_path)["new"] == 1
    assert len(x_archive.load_archive(directory=tmp_path)) == 1


def test_x_ledger_compaction_preserves_request_provenance(monkeypatch, tmp_path):
    monkeypatch.setattr(x_pacing, "_COMPACT_BYTES", 1)
    path = tmp_path / "requests.jsonl"
    x_pacing.record(2, "live", path=path, now=1000.)
    assert json.loads(path.read_text(encoding="utf-8"))["who"] == "live"


def test_shared_file_lock_preserves_updates_from_two_processes(tmp_path):
    target = tmp_path / "counter.txt"
    target.write_text("0", encoding="utf-8")
    source = """
from pathlib import Path
import sys, time
from core.utils.file_lock import advisory_file_lock
path = Path(sys.argv[1])
for _ in range(20):
    with advisory_file_lock(path):
        value = int(path.read_text(encoding='utf-8'))
        time.sleep(.002)
        path.write_text(str(value + 1), encoding='utf-8')
"""
    processes = [subprocess.Popen([sys.executable, "-c", source, str(target)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    try:
        for process in processes:
            _, error = process.communicate(timeout=20)
            assert process.returncode == 0, error.decode("utf-8", "replace")
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    assert target.read_text(encoding="utf-8") == "40"


def test_x_live_panel_never_uses_posts_seen_after_its_entry():
    from scripts import research_x_panel as panel

    days = pd.date_range("2026-10-24", periods=3, tz="UTC")
    event = {"handle": "alpha", "coin": "AAA", "created": days[0] + pd.Timedelta(hours=23),
             "observed_at": days[1] + pd.Timedelta(hours=1), "provenance": "live", "dir": 1, "n_coins": 1}
    tables = panel.signal_tables([event], {"alpha": days[0]}, days, ["AAA"])
    assert tables["s1"].to_numpy().sum() == 0


def test_x_events_carry_first_receipt_for_forward_availability():
    from scripts import research_x_kol as kol

    record = {"handle": "alpha", "author": "alpha", "tweet_id": "123", "text": "long $AAA",
              "created_at": "2026-10-24T23:00:00Z", "first_seen_at": "2026-10-25T01:00:00Z", "provenance": "live"}
    event, = kol.build_events([record], {"AAA": "AAA_USDT"})
    assert event.get("observed_at") == pd.Timestamp(record["first_seen_at"])


def test_telegram_successful_cycle_refreshes_its_read_only_summary(monkeypatch, tmp_path):
    from scripts import telegram_forward_cycle as wrapper
    from scripts import report_telegram_forward as report

    calls = []

    async def successful_cycle(args, store):
        calls.append("cycle")

    monkeypatch.setattr(wrapper, "_original_dispatch", successful_cycle, raising=False)
    monkeypatch.setattr(report, "summarize", lambda root: calls.append(root))
    asyncio.run(wrapper.cli.dispatch(SimpleNamespace(command="cycle"), SimpleNamespace(root=tmp_path)))
    assert calls == ["cycle", tmp_path]


def test_caution_funding_at_entry_is_not_earned_by_the_new_position():
    day = 86_400_000
    bars = [[i * day, 100., 101., 99., 100.] for i in range(8)]
    settlements = [{"fundingTime": i * day, "fundingRate": ".01" if i == 1 else ".02" if i == 8 else "0"}
                   for i in range(1, 9)]
    result = caution.evaluate_trade(bars, settlements, 0, 8 * day)
    assert result["funding"] == pytest.approx(.02)
    from scripts.upbit_short_backtest import score_short_trade

    assert score_short_trade(bars, settlements, 0)[1] == pytest.approx(.02)


def test_stopped_caution_backtest_needs_no_post_exit_funding():
    from scripts.upbit_short_backtest import score_short_trade

    day = 86_400_000
    bars = [[i * day, 100., 150. if i == 1 else 101., 99., 100.] for i in range(8)]
    assert score_short_trade(bars, [{"fundingTime": 2 * day, "fundingRate": ".01"}], 0)[1:] == (.01, True)
    assert score_short_trade(bars[:2], [{"fundingTime": 2 * day, "fundingRate": ".01"}], 0)[1:] == (.01, True)


def test_development_forward_labels_cannot_reach_the_holdout():
    from tests.test_research_loop_v3 import _panel

    ctx = evaluation.EvalContext(_panel())
    last_dev_entry = ctx.dates[ctx.periods["dev"]].max()
    assert last_dev_entry + pd.Timedelta(days=7) < evaluation.HOLDOUT_START


def test_model_panel_summary_uses_only_fully_development_labels(monkeypatch):
    from core.research import xs_daily_panel

    seen = []
    monkeypatch.setattr(xs_daily_panel, "anonymized_summary", lambda panel, end: seen.append(end) or {})
    loop.build_prompt(loop._empty_state(), object(), loop.ResearchLoopV3Config())
    assert seen[0] + pd.Timedelta(days=7) < evaluation.HOLDOUT_START


def test_old_development_feedback_that_crossed_the_split_is_withheld(monkeypatch):
    from core.research import xs_daily_panel

    monkeypatch.setattr(xs_daily_panel, "anonymized_summary", lambda *a, **kw: {})
    state = loop._empty_state()
    state["ledger"]["old"] = {"name": "old", "direction": "high", "expr": {"col": "close"},
                               "dev": {"period": ["2020-07-01", "2023-12-31"], "z": 999}}
    prompt = json.loads(loop.build_prompt(state, object(), loop.ResearchLoopV3Config()))
    feedback, = prompt["your_previous_formulas_development_results"]
    assert feedback["dev"] == {}


def test_new_telegram_kol_futures_require_actual_funding():
    from scripts.research_telegram_new_kols import evaluate_rows, signal_rows
    from tests.test_research_telegram_new_kols import sealed_with, synthetic_panel

    panel = synthetic_panel(np.full(24 * 80, 100.))
    panel.funding_known[:] = False
    signals = signal_rows(sealed_with("Long #ETH here", str(panel.index[24 * 40])))
    rows, status = evaluate_rows(signals, {("ETH", "futures"): panel}, horizons=(4,))
    assert rows == []
    assert status[("demo", 4, "funding_unavailable")] == 1


def test_new_telegram_kol_null_also_requires_funding_coverage():
    from scripts.research_telegram_new_kols import evaluate_rows, signal_rows
    from tests.test_research_telegram_new_kols import sealed_with, synthetic_panel

    panel = synthetic_panel(np.full(24 * 80, 100.))
    panel.funding_known[:] = False
    panel.funding_known[24 * 40 + 1:24 * 40 + 5] = True
    signals = signal_rows(sealed_with("Long #ETH here", str(panel.index[24 * 40])))
    rows, status = evaluate_rows(signals, {("ETH", "futures"): panel}, horizons=(4,))
    assert rows == []
    assert status[("demo", 4, "null_sample_below_50")] == 1


def test_new_telegram_kol_plan_can_stop_before_the_full_horizon_matures():
    from scripts.research_telegram_new_kols import simulate_plan
    from tests.test_research_telegram_new_kols import plan_panel

    lows = np.full(20, 99.)
    lows[3] = 94.
    panel = plan_panel(np.full(20, 100.), np.full(20, 101.), lows)
    panel.funding_known[:] = True
    result = simulate_plan(panel, panel.index[0], 1, 105., 95.)
    assert result["status"] == "ok" and result["exit_reason"] == "stop"


def test_new_telegram_kol_plan_keeps_unknown_funding_unavailable():
    from scripts.research_telegram_new_kols import simulate_plan
    from tests.test_research_telegram_new_kols import plan_panel

    lows = np.full(200, 99.)
    lows[3] = 94.
    panel = plan_panel(np.full(200, 100.), np.full(200, 101.), lows)
    panel.funding_known[:] = False
    assert simulate_plan(panel, panel.index[0], 1, 105., 95.)["status"] == "funding_unavailable"


def test_new_telegram_kol_null_uses_the_same_collateral_floor():
    from scripts.research_telegram_new_kols import evaluate_rows, signal_rows
    from tests.test_research_telegram_new_kols import sealed_with, synthetic_panel

    panel = synthetic_panel(np.full(24 * 80, 100.))
    panel.outcomes = lambda hours, side: (np.full(len(panel.index), -1.), np.zeros(len(panel.index)))
    signals = signal_rows(sealed_with("Long #ETH here", str(panel.index[24 * 40])))
    rows, _ = evaluate_rows(signals, {("ETH", "futures"): panel}, horizons=(4,))
    assert rows[0]["net"] == -1.
    assert rows[0]["null_values"].min() == -1.


def test_telegram_funding_nan_inside_a_holding_window_stays_unknown():
    from scripts.review_telegram_signals import SymbolPanel

    index = pd.date_range("2026-10-01", periods=12, freq="h", tz="UTC")
    bars = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100.}, index=index)
    funding = pd.DataFrame({"funding_rate": 0.}, index=index)
    funding.iloc[2, 0] = np.nan
    panel = SymbolPanel(bars, funding)
    assert np.isnan(panel.outcomes(4, 1)[1][0])


class _AnnouncementClient:
    def __init__(self, *, missing_bar=False):
        self.missing_bar = missing_bar

    async def get(self, url, params=None, **kwargs):
        if url.endswith("/klines"):
            rows = [[stamp, "1", "1", "0.9", "0.9"]
                    for stamp in range(params["startTime"], params["endTime"] + 1, 60_000)]
            if self.missing_bar:
                del rows[1]
            return SimpleNamespace(status_code=200, json=lambda: rows)
        assert url.endswith("/fundingRate")
        return SimpleNamespace(status_code=200, json=lambda: [])


def _announcement_trade():
    entry = int(datetime(2026, 10, 11, tzinfo=timezone.utc).timestamp() * 1000)
    return {"symbol": "AAAUSDT", "token": "AAA", "status": "open", "entry_price": 1.,
            "entry_ms": entry, "exit_due_ms": entry + 4 * 3_600_000, "stop_pct": .3}


def test_announcement_missing_stop_bars_do_not_make_a_profit():
    trade = _announcement_trade()
    now = datetime.fromtimestamp(trade["exit_due_ms"] / 1000, timezone.utc) + timedelta(minutes=5)
    asyncio.run(announcements._settle(_AnnouncementClient(missing_bar=True), trade, now))
    assert trade["status"] == "open"
    assert trade.get("return_pct") is None


def test_announcement_feed_outage_does_not_block_existing_settlements(monkeypatch, tmp_path):
    trade = _announcement_trade()
    now = datetime.fromtimestamp(trade["exit_due_ms"] / 1000, timezone.utc) + timedelta(minutes=5)
    state_path = tmp_path / announcements.KINDS["binance_delist"]["state_path"].name
    announcements._save(state_path, {"started_at": "2026-10-10T00:00:00+00:00", "trades": {"AAA|n1": trade}})
    monkeypatch.setattr(announcements, "_now", lambda: now)

    async def feed_unavailable(client):
        raise RuntimeError("synthetic notice feed outage")

    async def no_shadow(*args, **kwargs):
        return {}

    monkeypatch.setattr(announcements, "_poll", feed_unavailable)
    monkeypatch.setattr(announcements.event_risk_shadow, "apply_to_tracker", no_shadow)
    with pytest.raises(RuntimeError, match="feed outage"):
        asyncio.run(announcements.tick(_AnnouncementClient(), state_dir=tmp_path))
    state = announcements._load(state_path, {})
    assert state["trades"]["AAA|n1"]["status"] == "closed"
