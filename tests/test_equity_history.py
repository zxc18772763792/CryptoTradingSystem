from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.trading.account_snapshot import AccountSnapshotManager
from web import main as web_main
from web.api import trading as trading_api
from web.api import trading_balances

ROOT = Path(__file__).resolve().parents[1]


def _rows(minutes):
    start = datetime(2026, 9, 6, 2, 46)
    return [SimpleNamespace(timestamp=start + timedelta(minutes=m), total_usd=10000 - m) for m in minutes]


def test_downsampling_keeps_the_whole_range_instead_of_the_newest_rows():
    rows = _rows(range(0, 50000, 7))  # ~5 weeks of 7-minute snapshots
    kept = AccountSnapshotManager._downsample(rows, 100)

    assert len(kept) <= 101
    assert kept[0] is rows[0] and kept[-1] is rows[-1]
    assert [r.timestamp for r in kept] == sorted(r.timestamp for r in kept)
    gaps = [(b.timestamp - a.timestamp).total_seconds() for a, b in zip(kept[1:], kept[2:])]
    assert max(gaps) < 3 * min(gaps)  # evenly spread over time
    assert AccountSnapshotManager._downsample(rows[:50], 100) == rows[:50]


def test_history_route_passes_max_points_only_when_asked(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(trading_balances.router, prefix="/api/trading")
    client = TestClient(app)
    calls = []

    async def fake_get_history(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(trading_api.account_snapshot_manager, "get_history", fake_get_history)
    headers = {"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"}

    assert client.get("/api/trading/balances/history?hours=17520&max_points=1500&mode=paper", headers=headers).status_code == 200
    assert client.get("/api/trading/balances/history?hours=72&limit=500&mode=paper", headers=headers).status_code == 200
    assert calls[0]["max_points"] == 1500 and calls[0]["hours"] == 17520
    assert "max_points" not in calls[1] and calls[1]["limit"] == 500


def test_equity_worker_records_local_paper_equity_without_exchange_calls(monkeypatch):
    from core.trading import account_snapshot

    stop = asyncio.Event()
    recorded = []

    async def fake_record(*, total_usd, exchanges, mode):
        recorded.append((total_usd, exchanges, mode))
        stop.set()

    monkeypatch.setattr(web_main.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(web_main.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=9800.5))
    monkeypatch.setattr(account_snapshot.account_snapshot_manager, "record_snapshot", fake_record)
    asyncio.run(asyncio.wait_for(web_main._equity_snapshot_worker(stop), timeout=5))

    assert recorded == [(9800.5, {}, "paper")]
    assert "equity_snapshot" in web_main._build_runtime_task_factories(FastAPI())


def test_equity_worker_leaves_live_equity_to_balance_reads(monkeypatch):
    from core.trading import account_snapshot

    stop = asyncio.Event()
    record = AsyncMock()
    monkeypatch.setattr(web_main.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(account_snapshot.account_snapshot_manager, "record_snapshot", record)
    monkeypatch.setattr(web_main, "_touch_runtime_task", lambda name, success=False: stop.set())
    asyncio.run(asyncio.wait_for(web_main._equity_snapshot_worker(stop), timeout=5))

    record.assert_not_called()


def test_dashboard_equity_chart_is_a_pannable_full_history_plot():
    html = (ROOT / "web" / "templates" / "index.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert '<div id="equity-chart"' in html and '<canvas id="equity-chart"' not in html
    assert "chart.umd" not in html  # Chart.js only drew this chart
    assert "max_points=${EQUITY_HISTORY_POINTS}" in js and "hours=${EQUITY_HISTORY_HOURS}" in js
    assert "rangeselector" in js and "label:'全部'" in js and "uirevision" in js
