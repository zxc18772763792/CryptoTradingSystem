from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json

import pytest

from scripts.observe_alastar_signals import (
    MAX_INPUT_BYTES, _archive_lock, capture_public_preview, import_jsonl, observe_records,
    verify_archive,
)


def message(message_id=1, text="  #BTC long\nkeep  spacing\n", published="2026-10-21T01:00:00Z", **flags):
    return {"source_id": f"telegram:alastartrades:{message_id}",
            "url": f"https://t.me/alastartrades/{message_id}", "content": text,
            "published_at": published, "collected_at": "2020-01-01T00:00:00Z",
            "payload": {"message_id": message_id, "channel": "alastartrades", "original_text": text, **flags}}


def state(path):
    return json.loads((path / "state.json").read_text(encoding="utf-8"))


def live(path, records, when="2026-10-21T01:01:00Z"):
    return observe_records(records, path, mode="live_public_preview", observed_at=when, raw_preview=b"<html>original evidence</html>")


def test_cached_timestamp_cannot_become_prospective_or_replaced_by_later_live_fetch(tmp_path):
    record = message()
    record["collected_at"] = record["published_at"]
    observe_records([record], tmp_path, observed_at="2026-10-21T01:01:00Z")
    first = state(tmp_path)["messages"][record["source_id"]]["first"]
    assert first["mode"] == "historical_import"
    assert first["observed_at_utc"] == "2026-10-21T01:01:00+00:00"
    assert first["source_collected_at"] == record["published_at"]
    assert first["exclusion_reasons"] == ["historical_import"]
    live(tmp_path, [record], "2026-10-21T01:02:00Z")
    current = state(tmp_path)
    assert current["revision_count"] == 1
    assert current["candidate_review_count"] == 0
    assert current["messages"][record["source_id"]]["first"] == first


def test_live_timestamp_normalisation_cutoff_and_no_executable_side(tmp_path):
    result = live(tmp_path, [message(published="2026-10-21T09:00:00+08:00")], "2026-10-21T09:10:00+08:00")
    assert result["candidate_review_count"] == 1
    candidate = json.loads((tmp_path / "candidate_review_queue.json").read_text(encoding="utf-8"))["candidates"][0]
    assert candidate["published_at_utc"] == "2026-10-21T01:00:00+00:00"
    assert candidate["first_observed_at_utc"] == "2026-10-21T01:10:00+00:00"
    assert candidate["review_status"] == "human_review_required"
    assert candidate["execution_authorized"] is False
    assert "side" not in candidate


@pytest.mark.parametrize(("published", "when", "flag"), [
    ("2026-10-21T01:00:00Z", "2026-10-21T01:10:00.001Z", "first_observation_more_than_10_minutes_late"),
    ("2026-10-21T01:01:01Z", "2026-10-21T01:01:00Z", "future_publication"),
    ("2026-10-21T01:00:00", "2026-10-21T01:01:00Z", "invalid_or_timezone_missing_publication"),
    ("bad date", "2026-10-21T01:01:00Z", "invalid_or_timezone_missing_publication"),
])
def test_late_future_and_timezone_missing_dates_are_retained_but_excluded(tmp_path, published, when, flag):
    record = message(published=published)
    live(tmp_path, [record], when)
    current = state(tmp_path)
    assert current["candidate_review_count"] == 0
    assert flag in current["messages"][record["source_id"]]["first"]["exclusion_reasons"]
    assert current["messages"][record["source_id"]]["first"]["published_at_raw"] == published


@pytest.mark.parametrize("flags,reason", [({"telegram_edited": True}, "visible_edit"), ({"text_truncated": True}, "truncated_source_text")])
def test_visible_edits_and_previously_truncated_inputs_cannot_be_prospective(tmp_path, flags, reason):
    live(tmp_path, [message(**flags)])
    current = state(tmp_path)
    assert current["candidate_review_count"] == 0
    assert reason in current["messages"]["telegram:alastartrades:1"]["first"]["exclusion_reasons"]


def test_identical_inputs_are_idempotent_but_each_observation_has_an_immutable_run_log(tmp_path):
    record = message()
    live(tmp_path, [record, record])
    old_runs = {path.name: path.read_bytes() for path in (tmp_path / "runs").glob("*.json")}
    result = live(tmp_path, [record], "2026-10-21T01:02:00Z")
    assert result["run_count"] == 2
    assert result["message_count"] == result["revision_count"] == result["candidate_review_count"] == 1
    assert result["new_revision_count"] == 0
    for name, raw in old_runs.items():
        assert (tmp_path / "runs" / name).read_bytes() == raw
    second = json.loads((tmp_path / result["run_file"]).read_text(encoding="utf-8"))
    assert second["observations"][0]["status"] == "duplicate"
    assert second["previous_run_sha256"] == hashlib.sha256(next(iter(old_runs.values()))).hexdigest()
    snapshot = state(tmp_path)["messages"][record["source_id"]]["first"]
    assert snapshot["text"] == record["content"]
    assert snapshot["text_sha256"] == hashlib.sha256(record["content"].encode("utf-8")).hexdigest()
    assert snapshot["observed_at_utc"] == "2026-10-21T01:01:00+00:00"


def test_revisions_and_reversions_preserve_first_text_and_remain_out_of_current_queue(tmp_path):
    original = message(text=" #BTC long\n")
    changed = message(text="#BTC short", telegram_edited=True)
    live(tmp_path, [original])
    live(tmp_path, [changed], "2026-10-21T01:02:00Z")
    live(tmp_path, [original], "2026-10-21T01:03:00Z")
    current = state(tmp_path)
    item = current["messages"][original["source_id"]]
    assert len(item["revisions"]) == 2
    assert item["first"]["text"] == " #BTC long\n"
    assert item["first"]["prospective_eligible_at_first_observation"] is True
    assert current["candidate_review_count"] == 0
    assert "message_has_observed_revisions" in item["current_candidate_exclusion_reasons"]
    revised = [revision for revision in item["revisions"].values() if revision["text"] == "#BTC short"][0]
    assert "revision_after_first_observation" in revised["exclusion_reasons"]


def test_no_original_text_loss_and_content_difference_is_preserved(tmp_path):
    original = "\n " + "BTC bullish 🐳\n" * 2000 + "  "
    record = message(text="short display")
    record["payload"]["original_text"] = original
    live(tmp_path, [record])
    first = state(tmp_path)["messages"][record["source_id"]]["first"]
    assert first["text"] == original
    assert first["raw_message"]["content"] == "short display"
    assert first["raw_message"]["payload"]["original_text"] == original


def test_derived_state_is_rebuilt_from_runs_after_interrupted_projection(tmp_path):
    live(tmp_path, [message()])
    (tmp_path / "state.json").write_text("broken projection", encoding="utf-8")
    (tmp_path / "candidate_review_queue.json").unlink()
    live(tmp_path, [message(2)], "2026-10-21T01:02:00Z")
    assert state(tmp_path)["message_count"] == 2
    assert (tmp_path / "candidate_review_queue.json").exists()


def test_archive_lock_rejects_parallel_writer_without_touching_runs(tmp_path):
    with _archive_lock(tmp_path):
        with pytest.raises(RuntimeError, match="archive lock"):
            live(tmp_path, [message()])
    assert not list((tmp_path / "runs").glob("*.json"))
    assert live(tmp_path, [message()])["message_count"] == 1


def test_broken_hash_chain_stops_instead_of_rewriting_evidence(tmp_path):
    live(tmp_path, [message()])
    result = live(tmp_path, [message(2)], "2026-10-21T01:02:00Z")
    first_path = sorted((tmp_path / "runs").glob("*.json"))[0]
    first_path.write_bytes(first_path.read_bytes() + b" ")
    with pytest.raises(RuntimeError, match="hash chain"):
        verify_archive(tmp_path)
    assert len(list((tmp_path / "runs").glob("*.json"))) == result["run_count"]


def test_corrupted_latest_revision_is_detected_without_a_later_chain_link(tmp_path):
    result = live(tmp_path, [message()])
    path = tmp_path / result["run_file"]
    run = json.loads(path.read_text(encoding="utf-8"))
    run["new_revisions"][0]["text"] = "changed without matching hash"
    path.write_text(json.dumps(run), encoding="utf-8")
    with pytest.raises(RuntimeError, match="tail hash chain"):
        live(tmp_path, [message(2)])
    with pytest.raises(RuntimeError, match="revision content/hash"):
        verify_archive(tmp_path)


def test_incremental_checkpoint_does_not_read_earlier_receipts_each_time(tmp_path, monkeypatch):
    from pathlib import Path
    live(tmp_path, [message()])
    live(tmp_path, [message()], "2026-10-21T01:02:00Z")
    live(tmp_path, [message()], "2026-10-21T01:03:00Z")
    first = tmp_path / "runs" / "000000000001.json"
    original = Path.read_bytes

    def guarded_read(path):
        if path == first:
            raise AssertionError("Earlier receipt should not be replayed for a valid checkpoint")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read)
    result = live(tmp_path, [message()], "2026-10-21T01:04:00Z")
    assert result["run_count"] == 4
    assert result["revision_count"] == 1


def test_incremental_recovery_replays_only_committed_uncheckpointed_runs(tmp_path, monkeypatch):
    import core.news.alastar_observation as archive
    live(tmp_path, [message()])
    original = archive._projections
    monkeypatch.setattr(archive, "_projections", lambda *args: (_ for _ in ()).throw(OSError("simulated interruption")))
    with pytest.raises(OSError, match="interruption"):
        live(tmp_path, [message(2)], "2026-10-21T01:02:00Z")
    assert len(list((tmp_path / "runs").glob("*.json"))) == 2
    assert state(tmp_path)["run_count"] == 1
    monkeypatch.setattr(archive, "_projections", original)
    result = live(tmp_path, [message(3)], "2026-10-21T01:03:00Z")
    assert result["run_count"] == result["message_count"] == 3
    verified = verify_archive(tmp_path)
    assert verified["verified_full_chain"] is True
    assert verified["last_run_sha256"] == result["last_run_sha256"]


def test_duplicate_page_with_only_view_counter_changes_does_not_save_another_html(tmp_path):
    live(tmp_path, [message()])
    result = observe_records([message()], tmp_path, mode="live_public_preview", observed_at="2026-10-21T01:02:00Z",
                             raw_preview=b"<html>same message, changed view counter</html>")
    run = json.loads((tmp_path / result["run_file"]).read_text(encoding="utf-8"))
    assert run["raw_preview"]["persisted"] is False
    assert run["raw_preview"]["file"] is None
    assert run["raw_preview"]["sha256"] == hashlib.sha256(b"<html>same message, changed view counter</html>").hexdigest()
    assert len(list((tmp_path / "evidence").glob("*.html"))) == 1


def test_full_verification_checks_saved_html_evidence_hashes(tmp_path):
    live(tmp_path, [message()])
    evidence_path = next((tmp_path / "evidence").glob("*.html"))
    evidence_path.write_bytes(b"changed raw html")
    with pytest.raises(RuntimeError, match="preview evidence hash"):
        verify_archive(tmp_path)


@pytest.mark.parametrize("change", ["channel", "source_id", "url", "flag"])
def test_foreign_or_ambiguous_identity_fails_before_commit(tmp_path, change):
    record = message()
    if change == "channel":
        record["payload"]["channel"] = "AlastarTrades1"
    elif change == "source_id":
        record["source_id"] = "telegram:alastartrades:2"
    elif change == "url":
        record["url"] = "https://t.me/AlastarTrades1/1"
    else:
        record["payload"]["telegram_edited"] = "false"
    with pytest.raises(ValueError):
        live(tmp_path, [record])
    assert not list((tmp_path / "runs").glob("*.json"))


def test_naive_observer_time_is_an_error_and_live_requires_preview_evidence(tmp_path):
    with pytest.raises(ValueError, match="timezone"):
        observe_records([message()], tmp_path, observed_at="2026-10-21T01:01:00")
    with pytest.raises(ValueError, match="raw public preview"):
        observe_records([message()], tmp_path, mode="live_public_preview")


def test_import_bounds_do_not_silently_drop_records(tmp_path):
    path = tmp_path / "messages.jsonl"
    path.write_text(json.dumps(message()) + "\n" + json.dumps(message(2)) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no partial import"):
        import_jsonl(path, max_records=1)
    records, evidence = import_jsonl(path)
    assert len(records) == 2
    assert evidence["cached_timestamps_are_not_first_seen"] is True
    assert evidence["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    with path.open("wb") as stream:
        stream.truncate(MAX_INPUT_BYTES + 1)
    with pytest.raises(ValueError, match="20 MiB"):
        import_jsonl(path)


def test_fresh_public_capture_retains_full_caption_and_actual_receipt_timestamp(tmp_path, monkeypatch):
    text = "X" * 25000
    published = datetime.now(timezone.utc).isoformat()
    html = (f'<div class="tgme_widget_message" data-post="alastartrades/1">'
            f'<div class="js-message_text">{text}</div>'
            f'<a class="tgme_widget_message_date"><time datetime="{published}"></time></a></div>').encode("utf-8")
    monkeypatch.setattr("scripts.collect_alastar_history._fetch_page", lambda *args: html)
    before = datetime.now(timezone.utc)
    result = capture_public_preview(tmp_path)
    after = datetime.now(timezone.utc)
    observed = datetime.fromisoformat(result["observed_at_utc"])
    assert before <= observed <= after
    first = state(tmp_path)["messages"]["telegram:alastartrades:1"]["first"]
    assert first["text"] == text
    assert first["text_truncated"] is False
    assert result["candidate_review_count"] == 1
    evidence = next((tmp_path / "evidence").glob("*.html"))
    assert evidence.read_bytes() == html


def test_receipt_latency_reports_first_live_delays_poll_wait_and_outages(tmp_path):
    from core.news.alastar_observation import receipt_latency
    observe_records([message(1, published="2026-10-21T00:00:00Z")], tmp_path, observed_at="2026-10-21T00:30:00Z")
    live(tmp_path, [message(1, published="2026-10-21T00:00:00Z")], "2026-10-21T01:00:00Z")
    live(tmp_path, [message(2, published="2026-10-21T01:01:00Z")], "2026-10-21T01:02:00Z")  # 120s gap
    live(tmp_path, [message(2, published="2026-10-21T01:01:00Z")], "2026-10-21T01:06:00Z")  # 240s gap
    live(tmp_path, [message(3, published="2026-10-21T01:10:00Z")], "2026-10-21T01:26:00Z")  # 1200s outage
    report = receipt_latency(tmp_path, now="2026-10-21T02:26:00Z")
    first = report["live_first_receipts"]
    assert first["count"] == 2  # the historically imported message is never a latency sample
    assert [s["delay_seconds"] for s in first["samples"]] == [60.0, 960.0]
    assert first["within_cutoff_count"] == 1
    assert report["live_run_count"] == 4 and report["seconds_since_last_live_run"] == 3600.0
    assert [o["seconds"] for o in report["outages"]] == [1200.0]
    normal = report["poll_wait_excluding_outages"]
    assert normal["gap_count"] == 2
    assert normal["expected_wait_seconds"] == pytest.approx((120 ** 2 + 240 ** 2) / (2 * 360))
    assert normal["share_received_within_seconds"]["180"] == pytest.approx((120 + 180) / 360)
    assert report["poll_wait_all_gaps"]["gap_max_seconds"] == 1200.0
    saved = json.loads((tmp_path / "receipt_latency.json").read_text(encoding="utf-8"))
    assert saved["last_run_sha256"] == state(tmp_path)["last_run_sha256"]
    assert state(tmp_path)["run_count"] == 5  # read-only over the immutable runs


def test_media_only_post_is_archived_but_never_a_text_candidate(tmp_path):
    record = message(470, text="", has_photo=True, media_only=True)
    result = live(tmp_path, [record])
    assert result["candidate_review_count"] == 0
    first = state(tmp_path)["messages"][record["source_id"]]["first"]
    assert first["media_only"] is True and first["text"] == ""
    assert "media_only_no_text" in first["exclusion_reasons"]
    assert verify_archive(tmp_path)["verified_full_chain"] is True
    with pytest.raises(ValueError):
        live(tmp_path, [message(471, text="")])  # empty text without the media-only flag
    with pytest.raises(ValueError):
        live(tmp_path, [message(472, text="", media_only=True)])  # media-only claim without a photo


def test_text_revision_hash_is_unchanged_by_the_media_only_field(tmp_path):
    import hashlib
    from core.news.alastar_observation import _json_bytes, _normalise
    record = message(473)
    version = {"text": record["content"], "published_at_raw": record["published_at"],
               "telegram_edited": False, "text_truncated": False}
    assert _normalise(record)["revision_sha256"] == hashlib.sha256(_json_bytes(version)).hexdigest()


def test_other_channel_archive_is_separate_and_cannot_mix_channels(tmp_path):
    from core.news.alastar_observation import OTHER_CHANNELS_ROOT, archive_dir, receipt_latency
    assert archive_dir("@OtherChan") == OTHER_CHANNELS_ROOT / "otherchan"
    other = {"source_id": "telegram:otherchan:5", "url": "https://t.me/otherchan/5", "content": "hello",
             "published_at": "2026-10-21T01:00:00Z",
             "payload": {"message_id": 5, "channel": "otherchan", "original_text": "hello"}}
    out = tmp_path / "other"
    observe_records([other], out, mode="live_public_preview", observed_at="2026-10-21T01:01:00Z",
                    raw_preview=b"<html>other</html>", channel="otherchan")
    assert json.loads((out / "runs" / "000000000001.json").read_text(encoding="utf-8"))["channel"] == "otherchan"
    assert verify_archive(out, channel="otherchan")["channel"] == "otherchan"
    assert receipt_latency(out, channel="otherchan", now="2026-10-21T01:02:00Z")["live_first_receipts"]["count"] == 1
    with pytest.raises(RuntimeError):
        verify_archive(out)  # read as the Alastar archive
    with pytest.raises(ValueError):
        observe_records([message(1)], out, channel="otherchan")  # an Alastar message into another channel's archive
