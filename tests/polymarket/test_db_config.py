from __future__ import annotations

from types import SimpleNamespace

from prediction_markets.polymarket import db as pm_db


def test_configure_pm_db_disposes_previous_engine_before_reconfigure(monkeypatch):
    created = []
    disposed = []

    def _fake_create(url: str):
        created.append(url)
        return SimpleNamespace(url=url)

    def _fake_dispose(engine) -> None:
        disposed.append(engine.url)

    monkeypatch.setattr(pm_db, "_create_pm_engine", _fake_create)
    monkeypatch.setattr(pm_db, "_dispose_pm_engine_sync", _fake_dispose)
    monkeypatch.setattr(pm_db, "async_sessionmaker", lambda engine, **kwargs: ("factory", engine.url))
    monkeypatch.setattr(pm_db, "pm_engine", None)
    monkeypatch.setattr(pm_db, "PMSessionLocal", None)
    monkeypatch.setattr(pm_db, "_PM_DATABASE_URL", "")

    first = pm_db.configure_pm_db("sqlite+aiosqlite:///first.db")
    second = pm_db.configure_pm_db("sqlite+aiosqlite:///second.db")
    third = pm_db.configure_pm_db("sqlite+aiosqlite:///second.db")

    assert first.endswith("first.db")
    assert second.endswith("second.db")
    assert third.endswith("second.db")
    assert len(created) == 2
    assert created[0].endswith("/first.db")
    assert created[1].endswith("/second.db")
    assert len(disposed) == 1
    assert disposed[0].endswith("/first.db")
