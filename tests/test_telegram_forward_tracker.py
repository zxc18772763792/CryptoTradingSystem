from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json

import pytest

from core.research import retirement, telegram_forward_runner as runner, telegram_forward_tracker as tracker
from core.research import tracker_registry

NOW = datetime(2026, 10, 24, 6, 0, tzinfo=timezone.utc)
BOOK_EMPTY = {"decisions": 0, "mature_settled": 0, "pending": 0, "unavailable": 0, "mean_net_per_notional": None,
              "mean_excess_vs_same_entry_long": None, "win_rate": None, "status": "collecting_forward_evidence"}


def _write(base, name, value):
    (base / name).write_text(json.dumps(value), encoding="utf-8")


def _seed(base, *, checked_at, books=None, sources=None):
    _write(base, "activation.json", {"at": "2026-10-24T02:51:47+00:00"})
    _write(base, "forward_status.json", {"checked_at": checked_at.isoformat(), "sources": sources or {
        "cryptoquant_official": {"member": True, "complete_batch": True, "difference_gap": False},
        "BinanceChinese": {"member": True, "complete_batch": True, "difference_gap": False}}})
    names = tracker.CRYPTOQUANT_BOOKS + tracker.CHAT_BOOKS
    _write(base, "forward_summary.json", {"books": {n: dict(BOOK_EMPTY, **(books or {}).get(n, {})) for n in names}})


@pytest.fixture()
def base(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_FORWARD_DIR", str(tmp_path))
    return tmp_path


def test_empty_books_are_not_a_zero_percent_return(base):
    _seed(base, checked_at=NOW - timedelta(minutes=2))
    out = tracker.summary("cryptoquant", now=NOW)
    assert out["retirement"]["n"] == 0 and out["retirement"]["mean_pct"] is None
    assert out["runner_active"] is True and out["tracker_status_label"].startswith("运行中")
    assert [r["book"] for r in out["ledgers"]] == ["cq_follow"]


def test_chat_row_uses_hourly_follow_as_primary_and_lists_all_four_books(base):
    _seed(base, checked_at=NOW - timedelta(minutes=1), books={
        "chat_follow": {"decisions": 3, "mature_settled": 2, "mean_net_per_notional": 0.004, "win_rate": 0.5},
        "chat_6h_follow": {"mature_settled": 9, "mean_net_per_notional": -0.1}})
    out = tracker.summary("binance_chat", now=NOW)
    assert out["retirement"]["n"] == 2 and out["retirement"]["mean_pct"] == 0.4
    assert len(out["ledgers"]) == 4 and out["forward_win_rate"] == 0.5


def test_stale_status_reads_as_interrupted_not_running(base):
    _seed(base, checked_at=NOW - timedelta(hours=2, minutes=30))
    out = tracker.summary("binance_chat", now=NOW)
    assert out["runner_active"] is False and "采集已中断" in out["tracker_status_label"]


def test_error_newer_than_last_success_is_reported(base):
    _seed(base, checked_at=NOW - timedelta(minutes=3))
    _write(base, "last_error.json", {"status": "failed", "error_type": "FloodWaitError",
                                      "at": (NOW - timedelta(minutes=1)).isoformat(),
                                      "retry_at": (NOW + timedelta(minutes=5)).isoformat()})
    out = tracker.summary("cryptoquant", now=NOW)
    assert out["tracker_status_label"] == "采集异常" and out["last_error"]["error_type"] == "FloodWaitError"


def test_source_gap_is_visible(base):
    _seed(base, checked_at=NOW - timedelta(minutes=1), sources={
        "BinanceChinese": {"member": True, "complete_batch": False, "difference_gap": True}})
    assert "缺口" in tracker.summary("binance_chat", now=NOW)["tracker_status_label"]


def test_missing_directory_never_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_FORWARD_DIR", str(tmp_path / "absent"))
    out = tracker.summary("cryptoquant", now=NOW)
    assert out["tracker_status_label"] == "未激活" and out["retirement"]["n"] == 0


def test_both_trackers_are_registered_with_rules_and_measurement():
    ids = {s["id"]: s for s in tracker_registry.TRACKERS}
    for tid in ("telegram_cryptoquant_24h", "telegram_binance_chat_sentiment"):
        assert tid in ids and ids[tid]["rule_key"] in retirement.RULES
        assert retirement.RULES[tid]["diagnostic_only"] is True
        row = tracker_registry.tracker_row(ids[tid])
        assert row["group"] == "Telegram信号" and row["is_paper"] is True and row["min_n"] == 100


def test_runner_skips_unactivated_recent_and_cooldown(base):
    assert runner.should_run(NOW, base) == "not_activated"
    _seed(base, checked_at=NOW - timedelta(seconds=30))
    assert runner.should_run(NOW, base) == "recent_success"
    _seed(base, checked_at=NOW - timedelta(minutes=10))
    assert runner.should_run(NOW, base) == "run"
    _write(base, "last_error.json", {"retry_at": (NOW + timedelta(minutes=3)).isoformat()})
    assert runner.should_run(NOW, base) == "cooldown"
    _write(base, "last_error.json", {"retry_at": (NOW - timedelta(minutes=3)).isoformat()})
    assert runner.should_run(NOW, base) == "run"


def test_runner_classifies_cli_failures_by_error_type():
    ok = json.dumps({"status": "active_paper_observation"})
    assert runner._classify(0, ok) == "ok"
    for kind, expected in (("CooldownActive", "cooldown"), ("FloodWaitError", "cooldown"),
                           ("PermissionError", "busy"), ("RuntimeError", "failed")):
        assert runner._classify(1, json.dumps({"status": "failed", "error_type": kind})) == expected
    assert runner._classify(1, "garbage") == "failed"


def test_worker_marks_persistent_lock_as_unhealthy(base, tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "STATE_PATH", tmp_path / "runner.json")
    monkeypatch.setattr(runner, "POLL_SECONDS", 0.01)
    monkeypatch.setattr(runner, "should_run", lambda *a, **k: "run")

    async def busy(*a, **k):
        return {"returncode": 1, "outcome": "busy", "error_type": "PermissionError"}

    monkeypatch.setattr(runner, "run_cycle", busy)
    beats: list[bool] = []

    async def drive():
        stop = asyncio.Event()
        task = asyncio.create_task(runner.worker(stop, heartbeat=beats.append))
        while len(beats) < 4:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    asyncio.run(drive())
    assert beats[:2] == [True, True] and beats[2] is False
    assert json.loads((tmp_path / "runner.json").read_text(encoding="utf-8"))["enabled"] is False


def test_cycle_wrapper_pages_funding_requests_without_changing_the_frozen_cli():
    import scripts.telegram_forward_cycle as wrapper

    calls = []
    series = [{"fundingTime": 1000 + i} for i in range(230)]

    def fake(endpoint, params=None):
        calls.append(dict(params or {}))
        start = int((params or {}).get("startTime", 0))
        return [r for r in series if r["fundingTime"] >= start][: int((params or {}).get("limit", 1000))]

    saved = wrapper._original
    wrapper._original = fake
    try:
        rows = wrapper.paged_public_json("/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "startTime": 0, "limit": 1000})
        assert [r["fundingTime"] for r in rows] == [r["fundingTime"] for r in series]
        assert [c["limit"] for c in calls] == [100, 100, 100]
        calls.clear()
        wrapper.paged_public_json("/fapi/v1/klines", {"limit": 1000})
        assert calls == [{"limit": 1000}]  # other endpoints pass through untouched
    finally:
        wrapper._original = saved

    assert wrapper.cli.public_json is wrapper.paged_public_json
    assert runner.SCRIPT.name == "telegram_forward_cycle.py" and runner.SCRIPT.exists()
