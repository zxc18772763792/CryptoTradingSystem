from __future__ import annotations

import importlib
import threading
import time

# core.trading re-exports an instance under this name: load the module itself
pm = importlib.import_module("core.trading.position_manager")


def _slow_disk(monkeypatch, delay=0.3, calls=None):
    real = pm._write_atomic

    def slow(path, text):
        if calls is not None:
            calls.append(text)
        time.sleep(delay)
        real(path, text)

    monkeypatch.setattr(pm, "_write_atomic", slow)


def test_throttled_write_does_not_block_the_caller_on_a_slow_disk(tmp_path, monkeypatch):
    _slow_disk(monkeypatch, delay=0.5)
    writer = pm._StateFileWriter()
    target = tmp_path / "positions_paper.json"
    started = time.perf_counter()
    writer.submit(target, '{"v": 1}')
    assert time.perf_counter() - started < 0.1  # the event loop is not held for the disk write
    assert writer.wait_idle(5.0)
    assert target.read_text(encoding="utf-8") == '{"v": 1}'


def test_older_background_snapshot_never_overwrites_a_newer_forced_one(tmp_path, monkeypatch):
    gate = threading.Event()
    real = pm._write_atomic

    def held_first(path, text):
        if text == "old":
            gate.wait(5.0)  # the background write is still in flight ...
        real(path, text)

    monkeypatch.setattr(pm, "_write_atomic", held_first)
    writer = pm._StateFileWriter()
    target = tmp_path / "positions_paper.json"
    writer.submit(target, "old")
    time.sleep(0.1)  # the writer thread has taken "old" and is blocked inside the write
    done = threading.Event()
    threading.Thread(target=lambda: (writer.write_now(target, "new"), done.set()), daemon=True).start()
    time.sleep(0.1)
    gate.set()  # ... then finishes after the forced write was requested
    assert done.wait(5.0) and writer.wait_idle(5.0)
    assert target.read_text(encoding="utf-8") == "new"


def test_bursts_coalesce_and_the_latest_snapshot_wins(tmp_path, monkeypatch):
    calls = []
    _slow_disk(monkeypatch, delay=0.2, calls=calls)
    writer = pm._StateFileWriter()
    target = tmp_path / "positions_paper.json"
    for i in range(30):
        writer.submit(target, f'{{"v": {i}}}')
    assert writer.wait_idle(10.0)
    assert target.read_text(encoding="utf-8") == '{"v": 29}'
    assert len(calls) <= 3  # 30 ticks, a handful of disk writes


def test_forced_persist_is_on_disk_when_flush_returns(tmp_path, monkeypatch):
    monkeypatch.setattr(pm.settings, "CACHE_PATH", str(tmp_path), raising=False)
    manager = pm.PositionManager()
    manager._storage_root = tmp_path / "runtime_state"
    manager._dirty = True
    manager.flush()
    assert (tmp_path / "runtime_state" / f"positions_{manager._scope}.json").exists()
