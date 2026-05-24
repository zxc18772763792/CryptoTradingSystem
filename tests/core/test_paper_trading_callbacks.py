from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.backtest import paper_trading


def test_log_task_failure_ignores_cancelled_task(monkeypatch):
    calls = []

    def fake_error(message: str) -> None:
        calls.append(message)

    task = SimpleNamespace(
        cancelled=lambda: True,
        exception=lambda: (_ for _ in ()).throw(AssertionError("exception() should not be called")),
    )
    monkeypatch.setattr(paper_trading.logger, "error", fake_error)

    paper_trading._log_task_failure(task, context="Paper trading loop")

    assert calls == []


def test_log_task_failure_logs_task_exception(monkeypatch):
    calls = []

    def fake_error(message: str) -> None:
        calls.append(message)

    task = SimpleNamespace(cancelled=lambda: False, exception=lambda: RuntimeError("boom"))
    monkeypatch.setattr(paper_trading.logger, "error", fake_error)

    paper_trading._log_task_failure(task, context="Paper trading loop")

    assert calls == ["Paper trading loop exited with an error: RuntimeError('boom')"]


def test_log_task_failure_surfaces_exception_probe_errors(monkeypatch):
    calls = []

    def fake_error(message: str) -> None:
        calls.append(message)

    task = SimpleNamespace(cancelled=lambda: False, exception=lambda: (_ for _ in ()).throw(RuntimeError("probe failed")))
    monkeypatch.setattr(paper_trading.logger, "error", fake_error)

    paper_trading._log_task_failure(task, context="Paper trading loop")

    assert calls == [
        "Paper trading loop done callback failed while probing task state: RuntimeError('probe failed')"
    ]
