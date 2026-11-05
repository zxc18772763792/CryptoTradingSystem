import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import retirement, tracker_registry as registry, unlock_t7_big_ledger as L

NOW = datetime(2026, 11, 5, 8, tzinfo=timezone.utc)


def trade(token="AAA", size=12.0, entry_day="2026-11-08", exit_day="2026-11-14", **kw):
    return {"token": token, "symbol": f"{token}USDT", "unlock_date": "2026-11-15", "size_pct": size, "insider": True,
            "entry_day": entry_day, "exit_day": exit_day, "late": False, "status": "scheduled",
            "discovered_at": "2026-09-28T15:41:00+00:00", **kw}


def closed(hedged=4.0, status="closed", **kw):
    return {"status": status, "hedged_return_pct": hedged, "short_return_pct": hedged + 1, "funding_pct": .1,
            "entry_price": 1.0, "exit_price": .9, "exit_day_actual": "2026-11-14", "basket_n": 120, **kw}


@pytest.fixture
def world(tmp_path):
    source = tmp_path / "source.json"
    output = tmp_path / "ledger"

    def write(trades, updated_at=None):
        source.write_text(json.dumps({"started_at": "2026-09-28T15:41:00+00:00", "trades": trades,
                                      "updated_at": updated_at or L.now_utc().isoformat()}), encoding="utf-8")

    return source, output, write


def outcome_rows(st):
    return {k: e.get("outcome") for k, e in st["events"].items()}


@pytest.mark.parametrize(("size", "book"), [(10, "big"), (10.0, "big"), (45.56, "big"), (9.99, "small"), (5, "small"),
                                            (4.99, None), (float("nan"), None), (None, None), ("x", None)])
def test_book_threshold_is_exact(size, book):
    assert L.book_of(size) == book


def test_entry_close_is_midnight_after_the_entry_day():
    assert L.entry_close("2026-11-08") == datetime(2026, 11, 9, tzinfo=timezone.utc)


def test_a_discovered_trade_is_eligible_only_until_its_entry_close():
    activated = datetime(2026, 11, 5, 7, tzinfo=timezone.utc)
    assert L.eligible(trade(entry_day="2026-11-05"), activated)
    assert not L.eligible(trade(entry_day="2026-11-04"), activated)
    assert not L.eligible(trade(late=True), activated)
    assert not L.eligible(trade(symbol=None), activated)
    assert not L.eligible(trade(size=4.0), activated)
    assert not L.eligible({"token": "BAD"}, activated)


def test_activation_records_the_split_and_only_runs_once(world):
    source, output, write = world
    write({"OLD|1": trade(entry_day="2026-10-16", status="closed"), "NEW|1": trade(entry_day="2030-01-01")})
    rec = L.Ledger(output, source).activate()
    assert rec["data"]["eligible_at_activation"] == ["NEW|1"]
    assert rec["data"]["excluded_at_activation"] == ["OLD|1"]
    assert rec["data"]["source_module_sha256"] == L.sha(L.SOURCE_MODULE.read_bytes())
    with pytest.raises(RuntimeError, match="already exists"):
        L.Ledger(output, source).activate()


def test_activation_refuses_a_stale_or_empty_source(world):
    source, output, write = world
    write({"A|1": trade()}, updated_at="2026-01-01T00:00:00+00:00")
    with pytest.raises(RuntimeError, match="stale"):
        L.Ledger(output, source).activate()
    write({})
    with pytest.raises(RuntimeError, match="no trades"):
        L.Ledger(output, source).activate()
    assert not (output / "journal.jsonl").exists()


def test_cycle_reads_the_source_without_writing_it_and_freezes_the_book(world):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01"), "B|1": trade("BBB", size=7.0, entry_day="2030-01-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    before, stamp = source.read_bytes(), source.stat().st_mtime_ns
    report = ledger.cycle()
    assert report["outcome"] == "ok" and report["new_events"] == 2
    assert source.read_bytes() == before and source.stat().st_mtime_ns == stamp
    st = L.state(L.Journal(output))
    assert st["events"]["A|1"]["event"]["book"] == "big" and st["events"]["B|1"]["event"]["book"] == "small"
    assert st["events"]["A|1"]["event"]["seen_before_entry"] is True
    write({"A|1": trade(size=6.0, entry_day="2030-01-01"), "B|1": trade("BBB", size=20.0, entry_day="2030-01-01")})
    assert L.Ledger(output, source).cycle()["new_events"] == 0
    st = L.state(L.Journal(output))
    assert st["events"]["A|1"]["event"]["book"] == "big" and st["events"]["B|1"]["event"]["book"] == "small"


def test_settlement_counts_stops_and_schedule_changes_and_waits_for_the_hedge(world):
    source, output, write = world
    keys = ["C|1", "S|1", "H|1", "P|1", "U|1", "F|1", "M|1"]
    write({k: trade(k[0], entry_day="2030-01-01") for k in keys})
    ledger = L.Ledger(output, source)
    ledger.activate()
    ledger.cycle()
    pending = {"status": "closed", "hedged_return_pct": None, "hedge_settlement": {"version": 2, "status": "pending"}}
    unresolved = {"status": "closed", "hedged_return_pct": None, "short_return_pct": 2.0,
                  "hedge_settlement": {"version": 2, "status": "unresolved"}}
    write({"C|1": trade("C", entry_day="2030-01-01", **closed(3.0)),
           "S|1": trade("S", entry_day="2030-01-01", **closed(-40.0, status="stopped")),
           "H|1": trade("H", entry_day="2030-01-01", **closed(1.0, status="schedule_changed")),
           "P|1": trade("P", entry_day="2030-01-01", **pending),
           "U|1": trade("U", entry_day="2030-01-01", **unresolved),
           "F|1": trade("F", entry_day="2030-01-01", status="entry_failed")})
    assert L.Ledger(output, source).cycle()["settled"] == 6
    out = outcome_rows(L.state(L.Journal(output)))
    assert out["C|1"]["hedged_return_pct"] == 3.0 and out["S|1"]["status"] == "stopped"
    assert out["H|1"]["status"] == "schedule_changed"
    assert out["P|1"] is None
    assert out["U|1"]["void"] == "no_hedged_return" and out["F|1"]["void"] == "source_entry_failed"
    assert out["M|1"]["void"] == "source_missing"
    # a recorded outcome is never rebuilt from a later source file
    write({"C|1": trade("C", entry_day="2030-01-01", **closed(99.0))})
    L.Ledger(output, source).cycle()
    assert outcome_rows(L.state(L.Journal(output)))["C|1"]["hedged_return_pct"] == 3.0


def test_a_batch_counts_once_every_trade_of_that_day_has_an_outcome():
    def row(day, out):
        return {"event": {"entry_day": day}, **({"outcome": out} if out is not None else {})}

    events = [row("d1", {"hedged_return_pct": 2.0}), row("d1", {"hedged_return_pct": 4.0}),
              row("d2", {"hedged_return_pct": 9.0}), row("d2", None),
              row("d3", {"hedged_return_pct": -1.0}), row("d3", {"void": "source_missing"}),
              row("d4", {"void": "source_missing"})]
    assert L.batches(events) == [("d1", 3.0), ("d3", -1.0)]


def rows(values, start=0):
    return [(f"2027-{start + i:04d}", float(v)) for i, v in enumerate(values)]


ACTIVATED = "2026-11-05T00:00:00+00:00"
SOON = datetime(2026, 12, 1, tzinfo=timezone.utc)


def test_review_waits_and_shows_no_p_value_before_it_is_due():
    out = L.evaluate(rows([5] * 29), rows([1] * 29), ACTIVATED, SOON)
    assert out["verdict"] == "collecting" and out["p_absolute"] is None and out["p_dose"] is None


def test_verdicts_at_the_review():
    high = [8, 12, 6, 14, 9, 7, 11, 5, 13, 10] * 3
    low = [1, -2, 3, -1, 2, 0, -3, 1, 2, -1] * 3
    supported = L.evaluate(rows(high), rows(low), ACTIVATED, SOON)
    assert supported["verdict"] == "supported" and supported["p_absolute"] <= .10 and supported["p_dose"] <= .10
    assert L.evaluate(rows([-1] * 30), rows(low), ACTIVATED, SOON)["verdict"] == "rejected"
    assert L.evaluate(rows(low), rows(high), ACTIVATED, SOON)["verdict"] == "rejected"
    close = [5, -3, 4, -2, 3, -1, 2, 6, -4, 1] * 3
    inconclusive = L.evaluate(rows(close), rows([x - .1 for x in close]), ACTIVATED, SOON)
    assert inconclusive["verdict"] == "inconclusive" and inconclusive["p_dose"] > .10


def test_review_by_time_needs_a_minimum_sample():
    late = datetime(2028, 6, 1, tzinfo=timezone.utc)
    assert L.evaluate(rows([5] * 14), rows([1] * 20), ACTIVATED, late)["verdict"] == "insufficient"
    assert L.evaluate(rows([5] * 20), rows([1] * 9), ACTIVATED, late)["verdict"] == "insufficient"
    assert L.evaluate(rows([5, 6] * 10), rows([1, 0] * 10), ACTIVATED, late)["review_due"] is True


def test_review_by_count_uses_the_first_batches_only():
    out = L.evaluate(rows([5] * 30 + [-100] * 5), rows([1] * 40), ACTIVATED, SOON)
    assert out["big_batches"] == 30 and out["big_mean_pct"] == 5.0
    assert out["cutoff_entry_day"] == "2027-0029" and out["small_batches"] == 30


def test_the_review_is_journaled_once_and_frozen(world):
    source, output, write = world
    keys = {}
    for i in range(30):
        keys[f"B{i}|1"] = trade(f"B{i}", entry_day=f"20{30 + i}-01-01")
        keys[f"S{i}|1"] = trade(f"S{i}", size=6.0, entry_day=f"20{30 + i}-01-01")
    write(keys)
    ledger = L.Ledger(output, source)
    ledger.activate()
    ledger.cycle()
    for i, (k, t) in enumerate(list(keys.items())):
        keys[k] = {**t, **closed((5.0 if k[0] == "B" else 0.5) + i % 3)}
    write(keys)
    report = L.Ledger(output, source).cycle()
    assert report["reviewed"] is True
    first = L.summary(output)["evaluation"]
    assert first["review_due"] is True and first["big_batches"] == 30 and first["verdict"] != "collecting"
    assert L.Ledger(output, source).cycle()["reviewed"] is False
    recs = L.Journal(output).records()
    assert sum(r["kind"] == "review" for r in recs) == 1
    assert L.summary(output)["evaluation"] == first


def test_changing_a_pinned_file_stops_the_journal(world, monkeypatch):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    monkeypatch.setattr(L, "pins", lambda: {"protocol": "changed", "ledger": "changed"})
    report = L.Ledger(output, source).cycle()
    assert report["outcome"] == "drift_stopped"
    assert L.summary(output)["status"] == "drift_stopped"
    assert sum(r["kind"] == "event" for r in L.Journal(output).records()) == 0


def test_a_tampered_journal_is_reported_and_a_torn_tail_is_ignored(world):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    ledger.cycle()
    path = output / "journal.jsonl"
    good = path.read_bytes()
    path.write_bytes(good + b'{"partial')
    assert len(L.Journal(output).records()) == 2
    path.write_bytes(good.replace(b"AAA", b"ZZZ", 1))
    assert L.summary(output)["status"] == "journal_broken"


def test_second_writer_is_turned_away(world):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    with L.exclusive(output) as mine:
        assert mine
        assert L.Ledger(output, source).cycle() == {"outcome": "busy"}


def test_run_once_writes_a_heartbeat_and_idles_before_activation(world, tmp_path):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01")})
    beat = tmp_path / "runtime" / "state.json"
    assert L.run_once(output, source, beat)["outcome"] == "not_activated"
    assert json.loads(beat.read_text(encoding="utf-8"))["outcome"] == "not_activated"
    L.Ledger(output, source).activate()
    assert L.run_once(output, source, beat)["new_events"] == 1
    source.unlink()
    failed = L.run_once(output, source, beat)
    assert failed["outcome"] == "failed" and failed["error_type"] == "FileNotFoundError"


def test_summary_splits_the_books_and_counts_running_trades(world):
    source, output, write = world
    write({"A|1": trade("A", entry_day="2030-01-01"), "B|1": trade("B", size=20.0, entry_day="2030-01-01"),
           "C|1": trade("C", size=6.0, entry_day="2030-02-01"), "D|1": trade("D", entry_day="2030-03-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    ledger.cycle()
    write({"A|1": trade("A", entry_day="2030-01-01", **closed(10.0)),
           "B|1": trade("B", size=20.0, entry_day="2030-01-01", **closed(-30.0, status="stopped")),
           "C|1": trade("C", size=6.0, entry_day="2030-02-01", **closed(2.0)),
           "D|1": trade("D", entry_day="2030-03-01", status="open")})
    L.Ledger(output, source).cycle()
    out = L.summary(output, now=datetime(2030, 3, 5, tzinfo=timezone.utc))
    big, small = out["books"]["big"], out["books"]["small"]
    assert big["trades"] == 2 and big["batches"] == 1 and big["batch_mean_pct"] == -10.0
    assert big["open"] == 1 and big["waiting"] == 0 and big["stops"] == 1 and big["worst_trade_pct"] == -30.0
    assert small["trades"] == 1 and small["batch_mean_pct"] == 2.0
    assert out["evaluation"]["verdict"] == "insufficient"


def test_registry_row_shows_the_big_book_with_both_books_in_the_reason(world, monkeypatch, tmp_path):
    source, output, write = world
    write({"A|1": trade("A", entry_day="2030-01-01"), "C|1": trade("C", size=6.0, entry_day="2030-02-01")})
    ledger = L.Ledger(output, source)
    ledger.activate()
    ledger.cycle()
    write({"A|1": trade("A", entry_day="2030-01-01", **closed(10.0)),
           "C|1": trade("C", size=6.0, entry_day="2030-02-01", **closed(2.0))})
    L.Ledger(output, source).cycle()
    beat = tmp_path / "state.json"
    beat.write_text(json.dumps({"at": L.now_utc().isoformat(), "outcome": "ok",
                                "source_updated_at": L.now_utc().isoformat()}), encoding="utf-8")
    monkeypatch.setattr(L, "OUTPUT", output)
    monkeypatch.setattr(L, "STATE_PATH", beat)
    monkeypatch.setattr(L.summary, "__defaults__", (output, None))
    spec = next(s for s in registry.TRACKERS if s["id"] == "unlock_t7_big_v1")
    row = registry.tracker_row(spec)
    assert row["n"] == 1 and row["min_n"] == 30 and row["mean_pct"] == 10.0 and row["win_rate"] == 1.0
    assert row["verdict"] == "collecting" and row["verdict_label"] == "积累样本"
    assert "≥10%：1 批，均值 +10.00%" in row["verdict_reason"] and "5–10%：1 批，均值 +2.00%" in row["verdict_reason"]
    assert row["runner_active"] is True and row["open_marks"] == []
    assert row["gross_notional_multiplier"] == 2 and row["return_basis"] == "single_leg_notional"
    assert row["metric_kind"] == "net_return" and row["metric_unit_label"] == "%/批"
    assert row["backtest_mean_pct"] == 5.74


def test_registry_row_before_activation_is_labelled_not_blank(tmp_path, monkeypatch):
    monkeypatch.setattr(L, "STATE_PATH", tmp_path / "none.json")
    monkeypatch.setattr(L.summary, "__defaults__", (tmp_path / "empty", None))
    out = L.registry_summary()
    assert out["retirement"]["verdict"] == "not_activated" and out["tracker_status_label"] == "未激活"


def test_a_stale_source_shows_in_the_status(world, monkeypatch, tmp_path):
    source, output, write = world
    write({"A|1": trade(entry_day="2030-01-01")})
    L.Ledger(output, source).activate()
    beat = tmp_path / "state.json"
    old = (L.now_utc() - timedelta(hours=30)).isoformat()
    beat.write_text(json.dumps({"at": L.now_utc().isoformat(), "outcome": "ok", "source_updated_at": old}), encoding="utf-8")
    monkeypatch.setattr(L, "STATE_PATH", beat)
    monkeypatch.setattr(L.summary, "__defaults__", (output, None))
    assert L.registry_summary()["tracker_status_label"] == "t−7 原账本超过一天未更新"


def test_protocol_parameters_match_the_module_and_the_retirement_rule():
    protocol = json.loads(L.PROTOCOL.read_text(encoding="utf-8"))
    p = protocol["parameters"]
    assert protocol["protocol_id"] == L.PROTOCOL_ID
    assert (p["big_min_size_pct"], p["small_min_size_pct"]) == (L.BIG_MIN_PCT, L.SMALL_MIN_PCT)
    assert (p["review_big_batches"], p["review_after_days"]) == (L.REVIEW_BATCHES, L.REVIEW_AFTER_DAYS)
    assert (p["min_big_batches"], p["min_small_batches"]) == (L.MIN_BIG, L.MIN_SMALL)
    assert (p["permutations"], p["seed"], p["alpha"]) == (L.PERMUTATIONS, L.SEED, L.ALPHA)
    rule = retirement.RULES[L.TRACKER_ID]
    assert rule["min_n"] == L.REVIEW_BATCHES and rule["diagnostic_only"] is True and rule["unit"] == "batch"
    assert rule["backtest_mean_pct"] == protocol["development_evidence"]["big"]["batch_mean_pct"]
