from datetime import datetime, timedelta, timezone
import json

import pytest

from core.news.alastar_observation import observe_records
from core.research import alastar_forward_paper as paper


def dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def rows(start, count, step_return=0.):
    return [[int((start+i*paper.STEP).timestamp()*1000), 100*(1+step_return)**i,
             100*(1+step_return)**i, 100*(1+step_return)**i, 100*(1+step_return)**i] for i in range(count)]


def record(number=1, **flags):
    return {"source_id": f"telegram:alastartrades:{number}", "content": "BTC fresh long thesis",
            "published_at": "2026-10-21T01:00:45Z", "payload": {"message_id": number, **flags}}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    clock = [dt("2026-10-21T01:02:30Z")]
    monkeypatch.setattr(paper, "now_utc", lambda: clock[0])
    archive, output = tmp_path / "archive", tmp_path / "paper"
    protocol = tmp_path / "protocol.json"
    protocol.write_text(json.dumps({"protocol_id": "alastar_fresh_call_1h_paper_v2",
        "documented_at_utc": "2026-10-21T00:00:00Z", "book": {"holding_minutes": 60,
        "fixed_initial_notional_fraction": .2, "maximum_concurrent_positions": 5,
        "round_trip_cost_bps": 20, "stress_round_trip_cost_bps": [40, 80],
        "one_position_per_symbol": True, "no_discretionary_exit": True},
        "clock": {"maximum_entry_delay_after_publication_minutes": 30},
        "control": {"minimum_null_starts": 2000}}), encoding="utf-8")
    for at in ("00:55", "00:56", "00:57", "00:58"):
        observe_records([], archive, mode="live_public_preview",
                        observed_at=f"2026-10-21T{at}:00Z", raw_preview=b"public html evidence")
    label = {"source_id": "telegram:alastartrades:1", "reviewer_or_model_version": "test-reviewer",
             "rule_version": "v2", "events": [{"symbol": "BTCUSDT", "direction": 1, "confidence": .8,
             "is_fresh_entry_view": True, "reaction_type": "immediate_view", "original_evidence_quote": "fresh long",
             "requires_unavailable_confirmation": False, "opposed_directions_in_message": False}]}
    journal = paper.PaperJournal(output, archive, protocol)
    clock[0] = dt("2026-10-21T00:58:30Z")
    journal.activate({"ok": True, "audit_sha256": "synthetic_verified_null"}, {"ok": True})
    for at in ("01:01", "01:02"):
        observe_records([record()], archive, mode="live_public_preview",
                        observed_at=f"2026-10-21T{at}:00Z", raw_preview=b"public html evidence")
    clock[0] = dt("2026-10-21T01:02:30Z")
    return journal, label, clock


def test_freeze_uses_host_clock_and_verified_first_receipt(setup):
    journal, label, clock = setup
    r = journal.freeze(label)["payload"]
    assert r["label_frozen_at_utc"] == clock[0].isoformat()
    assert r["first_received_at_utc"] == "2026-10-21T01:01:00+00:00"
    assert r["first_receipt_run_file"] == "runs/000000000005.json"
    assert r["events"][0]["planned_entry_at"] == "2026-10-21T01:05:00+00:00"
    assert r["events"][0]["planned_exit_at"] == "2026-10-21T02:05:00+00:00"
    assert r["events"][0]["status"] == "planned"
    with pytest.raises(ValueError, match="already frozen"):
        journal.freeze(label)


def test_external_timestamp_cannot_backdate_label(setup):
    journal, label, _ = setup
    label["label_frozen_at_utc"] = "2020-01-01T00:00:00Z"
    with pytest.raises(ValueError, match="cannot supply"):
        journal.freeze(label)
    assert journal.status()["journal_records"] == 1  # activation only


def test_exact_quote_required(setup):
    journal, label, _ = setup
    label["events"][0]["original_evidence_quote"] = "invented signal"
    with pytest.raises(ValueError, match="exact first"):
        journal.freeze(label)


@pytest.mark.parametrize("change", [
    {"reaction_type": "context_only"}, {"is_fresh_entry_view": False},
    {"requires_unavailable_confirmation": True}, {"opposed_directions_in_message": True}, {"confidence": .64},
])
def test_background_updates_and_unconfirmed_conditions_are_not_v2_entries(setup, change):
    journal, label, _ = setup
    label["events"][0].update(change)
    assert journal.freeze(label)["payload"]["events"][0]["status"] == "skipped"


def test_collector_outage_blocks_new_freeze(setup):
    journal, label, clock = setup
    clock[0] += timedelta(minutes=11)
    r = journal.freeze(label)["payload"]
    assert "collector_health_gate_failed" in r["events"][0]["exclusion_reasons"]


def test_late_interpretation_never_gets_original_entry(setup):
    journal, label, clock = setup
    clock[0] += timedelta(minutes=30)
    e = journal.freeze(label)["payload"]["events"][0]
    assert "skipped_entry_too_late" in e["exclusion_reasons"] and e["status"] == "skipped"


class Provider:
    def __init__(self):
        self.calls = 0
        self.fail = False

    def klines(self, symbol, start, end):
        self.calls += 1
        if self.fail:
            raise RuntimeError("missing official data")
        n = int((end-start)/paper.STEP)+1
        out = rows(start, n)
        if n == 13:
            out[-1][1:5] = [110., 110., 110., 110.]
        return out, [{"test_official_evidence": True}]

    def funding(self, symbol, start, end):
        return [{"fundingTime": int((start+timedelta(minutes=30)).timestamp()*1000),
                 "fundingRate": ".01", "markPrice": "100", "symbol": symbol}], {"complete_query_coverage": True}


def test_no_early_or_late_reconstructed_entry(setup):
    journal, label, clock = setup
    journal.freeze(label)
    provider = Provider()
    assert journal.tick(provider) == [] and provider.calls == 0
    clock[0] = dt("2026-10-21T01:05:31Z")
    actions = journal.tick(provider)
    assert actions[0]["kind"] == "missed" and provider.calls == 0
    assert journal.status()["counts"] == {"skipped": 1}


def test_missing_entry_data_is_not_zero_fill_and_eventually_skips(setup):
    journal, label, clock = setup
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    provider = Provider(); provider.fail = True
    assert journal.tick(provider)[0]["kind"] == "entry_unavailable"
    assert journal.status()["counts"] == {"planned": 1}
    clock[0] += timedelta(seconds=31)
    assert journal.tick(provider)[0]["kind"] == "missed"


def test_later_edit_does_not_remove_frozen_trade_and_funding_is_separate(setup):
    journal, label, clock = setup
    journal.freeze(label)
    edited = record(telegram_edited=True); edited["content"] += " edited later"
    observe_records([edited], journal.archive, mode="live_public_preview",
                    observed_at="2026-10-21T01:03:00Z", raw_preview=b"new public text")
    clock[0] = dt("2026-10-21T01:05:01Z")
    provider = Provider()
    assert journal.tick(provider)[0]["kind"] == "entry"
    clock[0] = dt("2026-10-21T02:11:00Z")
    settled = journal.tick(provider)[0]["payload"]
    assert settled["net_price_only"] == pytest.approx(.098)
    assert settled["net_with_funding"] == pytest.approx(.088)
    assert settled["primary_excess_price_only"] == pytest.approx(.1)
    assert settled["primary_null"]["n"] == 8629
    assert journal.status()["counts"] == {"completed": 1}
    assert journal.tick(provider) == []


def test_same_symbol_cannot_stack_even_with_another_fresh_post(setup):
    journal, label, clock = setup
    journal.freeze(label)
    observe_records([record(2)], journal.archive, mode="live_public_preview",
                    observed_at="2026-10-21T01:02:01Z", raw_preview=b"second source")
    label["source_id"] = "telegram:alastartrades:2"
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    actions = journal.tick(Provider())
    assert [r["kind"] for r in actions] == ["entry", "missed"]
    assert actions[1]["payload"]["reason"] == "overlap_same_symbol"


def test_pre_entry_null_never_uses_future_prices_or_interpolates_gaps():
    entry = dt("2026-10-21T01:05:00Z")
    history = rows(entry-timedelta(days=30), 8641, .0001)
    baseline = paper.pre_entry_null(history, entry, 1)
    assert baseline["n"] == 8629
    assert baseline["mean_net_price_only"] == pytest.approx(1.0001**12-1-.002)
    future = rows(entry+paper.STEP, 100, .5)
    assert paper.pre_entry_null(history+future, entry, 1) == baseline
    fewer = paper.pre_entry_null(history[:100], entry, 1)
    assert fewer["status"] == "insufficient_pre_entry_history" and fewer["n"] == 88
    with_gap = paper.pre_entry_null(history[:4000]+history[4001:], entry, 1)
    assert with_gap["n"] < baseline["n"]


def test_known_liquidation_survives_later_gap_and_final_bar_future_is_ignored():
    start = dt("2026-10-21T01:05:00Z")
    raw = rows(start, 13)
    raw[-1][2] = 1000000.  # final endpoint high belongs to the next hour
    result = paper.price_return(paper.bars_by_time(raw), start, -1)
    assert not result["liquidated_1x_approx"]
    raw[2][2] = 200.
    result = paper.price_return(paper.bars_by_time(raw[:3]), start, -1)
    assert result["net_price_only"] == -1. and result["liquidated_1x_approx"]


def test_zero_volume_carried_bar_is_not_an_executable_print():
    start = dt("2026-10-21T01:05:00Z")
    raw = rows(start, 13)
    raw = [r+[1., 0, 1., 1] for r in raw]
    raw[0][5] = 0
    assert paper.price_return(paper.bars_by_time(raw), start, 1) is None


def test_journal_tampering_and_protocol_drift_stop_processing(setup):
    journal, label, clock = setup
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    journal.tick(Provider())
    first = journal.output/"journal/000000000001.json"
    first.write_bytes(first.read_bytes()+b" ")
    with pytest.raises(RuntimeError, match="hash"):
        journal.status()


def test_protocol_change_after_activation_stops_existing_book(setup):
    journal, _, _ = setup
    protocol = json.loads(journal.protocol_path.read_bytes())
    protocol["note"] = "changed after registration"
    journal.protocol_path.write_text(json.dumps(protocol), encoding="utf-8")
    changed = paper.PaperJournal(journal.output, journal.archive, journal.protocol_path)
    with pytest.raises(RuntimeError, match="protocol"):
        changed.status()


def test_unactivated_book_cannot_accept_real_labels(setup):
    journal, label, _ = setup
    inactive = paper.PaperJournal(journal.output.parent/"inactive", journal.archive, journal.protocol_path)
    with pytest.raises(RuntimeError, match="not activated"):
        inactive.freeze(label)
    with pytest.raises(RuntimeError, match="not activated"):
        inactive.tick(Provider())


def test_tail_edit_is_detected_even_without_a_following_record(setup):
    journal, _, _ = setup
    tail = journal.output/"journal/000000000001.json"
    tail.write_bytes(tail.read_bytes()+b" ")
    with pytest.raises(RuntimeError, match="checkpoint"):
        journal.status()


def test_historical_import_is_saved_as_excluded_not_promoted(setup):
    journal, label, _ = setup
    observe_records([record(2)], journal.archive, observed_at="2026-10-21T01:02:01Z")
    label["source_id"] = "telegram:alastartrades:2"
    r = journal.freeze(label)["payload"]["events"][0]
    assert r["status"] == "skipped" and "not_a_timely_live_first_receipt" in r["exclusion_reasons"]


def test_response_crossing_capture_deadline_cannot_fill(setup):
    journal, label, clock = setup
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    class Slow(Provider):
        def klines(self, *args):
            result = super().klines(*args)
            clock[0] += timedelta(seconds=31)
            return result
    assert journal.tick(Slow())[0]["kind"] == "entry_unavailable"
    assert journal.tick(Provider())[0]["kind"] == "missed"


def test_funding_coverage_failure_does_not_become_zero(setup):
    journal, label, clock = setup
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    class UnknownFunding(Provider):
        def funding(self, *args):
            return [], {"complete_query_coverage": False}
    provider = UnknownFunding()
    journal.tick(provider)
    clock[0] = dt("2026-10-21T02:11:00Z")
    assert journal.tick(provider)[0]["kind"] == "settlement_unavailable"
    assert journal.status()["counts"] == {"open": 1}


def test_missing_null_does_not_hide_closed_trade_or_lock_its_collateral(setup):
    journal, label, clock = setup
    journal.freeze(label)
    clock[0] = dt("2026-10-21T01:05:01Z")
    journal.tick(Provider())
    class NoNull(Provider):
        def klines(self, symbol, start, end):
            if end-start > timedelta(hours=1):
                raise RuntimeError("null unavailable")
            return super().klines(symbol, start, end)
    clock[0] = dt("2026-10-21T02:11:00Z")
    r = journal.tick(NoNull())[0]
    assert r["kind"] == "settlement" and r["payload"]["primary_excess_price_only"] is None
    assert r["payload"]["net_with_funding"] == pytest.approx(.088)
    frozen_settlement = (journal.output/"journal"/f"{r['sequence']:012d}.json").read_bytes()
    assert journal.status()["counts"] == {"completed": 1}
    assert journal.tick(Provider())[0]["kind"] == "comparison"
    assert journal.status()["primary_measured_trades"] == 1
    assert (journal.output/"journal"/f"{r['sequence']:012d}.json").read_bytes() == frozen_settlement


def test_short_funding_boundary_credit_is_not_assumed(setup):
    journal, label, clock = setup
    label["events"][0]["direction"] = -1
    journal.freeze(label)
    class BoundaryCredit(Provider):
        def funding(self, symbol, start, end):
            return [{"symbol": symbol, "fundingTime": int(start.timestamp()*1000),
                     "fundingRate": ".01", "markPrice": "100"}], {"complete_query_coverage": True}
    provider = BoundaryCredit()
    clock[0] = dt("2026-10-21T01:05:01Z")
    journal.tick(provider)
    clock[0] = dt("2026-10-21T02:11:00Z")
    p = journal.tick(provider)[0]["payload"]
    assert p["net_with_funding"] == pytest.approx(-.102)
    assert p["funding_return_lower_bound"] == 0
    assert p["primary_excess_price_only"] == pytest.approx(-.1)


def test_five_positions_and_loss_exhausted_cash_prevent_new_entry(setup):
    journal, label, clock = setup
    first = label["events"][0]
    label["events"] = [{**first, "symbol": f"X{i}USDT"} for i in range(6)]
    journal.freeze(label)
    class Collapse(Provider):
        def klines(self, symbol, start, end):
            raw, evidence = super().klines(symbol, start, end)
            if end-start == timedelta(hours=1):
                raw[0][3] = .01
            return raw, evidence
    provider = Collapse()
    clock[0] = dt("2026-10-21T01:05:01Z")
    actions = journal.tick(provider)
    assert [r["kind"] for r in actions].count("entry") == 5
    assert actions[-1]["payload"]["reason"] == "five_positions"
    clock[0] = dt("2026-10-21T02:11:00Z")
    journal.tick(provider)
    assert journal.status()["counts"] == {"completed": 5, "skipped": 1}
    new = record(2); new["published_at"] = "2026-10-21T02:10:45Z"
    for at in ("02:08", "02:09", "02:10", "02:11"):
        observe_records([new] if at == "02:11" else [], journal.archive, mode="live_public_preview",
                        observed_at=f"2026-10-21T{at}:00Z", raw_preview=b"new fresh source")
    clock[0] = dt("2026-10-21T02:11:30Z")
    label["source_id"] = new["source_id"]; label["events"] = [first]
    journal.freeze(label)
    clock[0] = dt("2026-10-21T02:15:01Z")
    assert journal.tick(provider)[0]["payload"]["reason"] == "insufficient_realised_collateral"


def test_predeclared_review_gates_do_not_promote_early_success():
    activation = dt("2026-10-21T00:58:30Z")
    def book(n, excess):
        return {str(i): {"event_id": str(i), "status": "completed", "entry": {"entry_at": (activation+timedelta(hours=i)).isoformat()},
                        "settlement": {"primary_excess_price_only": excess}} for i in range(n)}
    assert paper.review_gate(book(30,.5), activation, activation+timedelta(days=3)) is None
    assert paper.review_gate(book(40,-.001), activation, activation+timedelta(days=3)) == "futility40trade_stop"
    assert paper.review_gate(book(40,None), activation, activation+timedelta(days=3)) == "futility40trade_data_incomplete"
    assert paper.review_gate(book(40,.001), activation, activation+timedelta(days=3)) is None
    assert paper.review_gate(book(66,.001), activation, activation+timedelta(days=3)) == "confirmatory66trade_review_due"
    assert paper.review_gate({}, activation, activation.replace(year=2028)) == "maximum24months_reached"


def test_open_positions_reserve_the_40_and66_sample_boundaries():
    activation = dt("2026-10-21T00:58:30Z")
    events = {str(i): {"event_id": str(i), "status": "completed", "entry": {"entry_at": activation.isoformat()},
                       "settlement": {"primary_excess_price_only": .01}} for i in range(39)}
    events["open"] = {"event_id": "open", "status": "open"}
    assert paper.review_gate(events, activation, activation+timedelta(days=2)) == "await40trade_futility_review"
    events.update({str(i): {"event_id": str(i), "status": "completed", "entry": {"entry_at": activation.isoformat()},
                           "settlement": {"primary_excess_price_only": .01}} for i in range(39,65)})
    assert paper.review_gate(events, activation, activation+timedelta(days=2)) == "await66trade_confirmatory_review"
