from __future__ import annotations

import json
import multiprocessing
import threading
from pathlib import Path

import pytest

from core.trading.account_manager import AccountManager


def _new_manager(path: str | Path) -> AccountManager:
    manager = AccountManager.__new__(AccountManager)
    manager._thread_lock = threading.RLock()
    manager._accounts = {}
    manager._file = Path(path)
    manager._file_signature = None
    manager._file.parent.mkdir(parents=True, exist_ok=True)
    manager._load()
    return manager


def _create_account_worker(path: str, account_id: str, barrier, errors) -> None:
    try:
        manager = _new_manager(path)
        # Windows spawn imports the full ``core.trading`` package in every
        # child; on a cold antivirus/cache run this can exceed 15 seconds.
        barrier.wait(timeout=45)
        manager.create_account(account_id, account_id, "binance")
    except BaseException as exc:  # pragma: no cover - reported to parent process
        errors.put(f"{type(exc).__name__}: {exc}")


def test_concurrent_process_creates_do_not_lose_accounts(tmp_path: Path):
    path = tmp_path / "accounts.json"
    _new_manager(path)

    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(5)
    errors = ctx.Queue()
    processes = [
        ctx.Process(
            target=_create_account_worker,
            args=(str(path), f"account-{index}", barrier, errors),
        )
        for index in range(4)
    ]
    for process in processes:
        process.start()
    barrier.wait(timeout=45)
    for process in processes:
        process.join(timeout=60)
        assert not process.is_alive()
        assert process.exitcode == 0

    reported_errors = []
    while not errors.empty():
        reported_errors.append(errors.get_nowait())
    assert reported_errors == []

    payload = json.loads(path.read_text(encoding="utf-8"))
    ids = {row["account_id"] for row in payload["accounts"]}
    assert ids == {"main", "account-0", "account-1", "account-2", "account-3"}
    assert json.loads(path.with_suffix(".json.bak").read_text(encoding="utf-8")) == payload


def test_corrupt_primary_recovers_from_last_known_good_backup(tmp_path: Path):
    path = tmp_path / "accounts.json"
    manager = _new_manager(path)
    manager.create_account("recovered", "Recovered", "binance", mode="live")
    path.write_text("", encoding="utf-8")

    recovered = _new_manager(path)

    assert recovered.get_account_mode("recovered") == "live"
    assert json.loads(path.read_text(encoding="utf-8"))["accounts"]


def test_corrupt_primary_without_backup_fails_closed(tmp_path: Path):
    path = tmp_path / "accounts.json"
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="unable to load accounts configuration"):
        _new_manager(path)

    assert path.read_text(encoding="utf-8") == "not-json"
