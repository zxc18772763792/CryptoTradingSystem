import os
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.ops.service.api import create_router
from prediction_markets.polymarket import db as pm_db


def _quote(token_id: str, minute: int, bid: float, ask: float):
    midpoint = (bid + ask) / 2.0
    return {
        "ts": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
        "market_id": f"m-{token_id}",
        "token_id": token_id,
        "outcome": "YES",
        "price": midpoint,
        "bid": bid,
        "ask": ask,
        "midpoint": midpoint,
        "spread": ask - bid,
        "depth1": 100.0,
        "depth5": 500.0,
        "fetched_at": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
    }


def test_ops_polymarket_status_route(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(create_router())

    async def fake_status(app):
        return {"markets_count": 1, "subscriptions_count": 2}

    from core.ops.service import api as ops_api

    monkeypatch.setattr(ops_api, "_build_polymarket_status", fake_status)

    client = TestClient(app)
    resp = client.get("/ops/polymarket/status", headers={"X-OPS-TOKEN": "test-token"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["data"]["markets_count"] == 1


def test_ops_polymarket_paper_order_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes(
            [
                {
                    "ts": datetime(2026, 3, 3, tzinfo=timezone.utc),
                    "market_id": "m1",
                    "token_id": "tok_yes",
                    "outcome": "YES",
                    "price": 0.49,
                    "bid": 0.48,
                    "ask": 0.50,
                    "midpoint": 0.49,
                    "spread": 0.02,
                    "depth1": 100.0,
                    "depth5": 500.0,
                    "fetched_at": datetime(2026, 3, 3, tzinfo=timezone.utc),
                }
            ]
        )

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        reset = client.post("/ops/polymarket/paper/reset", json={"account_id": "ops", "initial_cash": 100}, headers=headers)
        assert reset.status_code == 200
        assert reset.json()["data"]["cash"] == 100

        order = client.post(
            "/ops/polymarket/paper/order",
            json={
                "account_id": "ops",
                "market_id": "m1",
                "token_id": "tok_yes",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.51,
                "size": 10,
            },
            headers=headers,
        )
        assert order.status_code == 200
        payload = order.json()
        assert payload["ok"] is True
        assert payload["data"]["status"] == "FILLED"

        positions = client.get("/ops/polymarket/paper/positions?account_id=ops", headers=headers)
        assert positions.status_code == 200
        body = positions.json()
        assert body["data"]["count"] == 1
        assert body["data"]["items"][0]["size"] == 10

        summary = client.get("/ops/polymarket/paper/summary?account_id=ops", headers=headers)
        assert summary.status_code == 200
        summary_body = summary.json()
        assert summary_body["ok"] is True
        assert summary_body["data"]["cash"] == 95.0
        assert summary_body["data"]["positions_value"] == 4.9
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_paper_strategy_once_dry_run(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_strategy_dry.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        resp = client.post(
            "/ops/polymarket/paper/strategy_once",
            json={
                "account_id": "ops_strategy",
                "token_ids": ["tok_a"],
                "strategy": "threshold",
                "initial_cash": 100.0,
                "order_size": 10.0,
                "buy_below": 0.45,
                "sell_above": 0.60,
                "max_order_notional": 100.0,
                "max_position_notional": 100.0,
            },
            headers=headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["data"]["dry_run"] is True
        assert body["data"]["decisions"][0]["action"] == "BUY"
        assert body["data"]["orders"] == []
        assert asyncio.run(pm_db.list_paper_orders("ops_strategy")) == []
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_paper_strategy_once_execute(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_strategy_exec.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        resp = client.post(
            "/ops/polymarket/paper/strategy_once",
            json={
                "account_id": "ops_strategy",
                "token_ids": ["tok_a"],
                "strategy": "threshold",
                "initial_cash": 100.0,
                "order_size": 10.0,
                "buy_below": 0.45,
                "sell_above": 0.60,
                "max_order_notional": 100.0,
                "max_position_notional": 100.0,
                "execute": True,
            },
            headers=headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["data"]["dry_run"] is False
        assert body["data"]["orders"][0]["status"] == "FILLED"
        assert body["data"]["summary"]["cash"] == 95.9
        assert len(asyncio.run(pm_db.list_paper_orders("ops_strategy"))) == 1
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_profile_promote_and_strategy_once_profile_path(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_profile.db').as_posix()}")

    import asyncio
    import json

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])

    asyncio.run(seed())
    report_path = tmp_path / "wf_report.json"
    report_path.write_text(
        json.dumps(
            {
                "base_config": {
                    "strategy": "threshold",
                    "initial_cash": 100.0,
                    "order_size": 10.0,
                    "max_order_notional": 100.0,
                    "max_position_notional": 100.0,
                    "fee_rate": 0.0,
                },
                "token_ids": ["tok_a"],
                "summary": {
                    "segments": 2,
                    "total_test_net_pnl": 4.2,
                    "avg_test_net_pnl": 2.1,
                    "worst_test_net_pnl": 2.1,
                    "positive_segments": 2,
                    "selection_counts": [{"params": {"buy_below": 0.45, "sell_above": 0.60}, "segments": 2}],
                },
            }
        ),
        encoding="utf-8",
    )
    profile_path = tmp_path / "profile.json"
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        promoted = client.post(
            "/ops/polymarket/paper/profile/promote",
            json={
                "report_path": str(report_path),
                "account_id": "profile_ops",
                "output_path": str(profile_path),
            },
            headers=headers,
        )
        assert promoted.status_code == 200
        promoted_body = promoted.json()
        assert promoted_body["ok"] is True
        assert promoted_body["data"]["profile"]["safe_to_execute"] is True
        assert promoted_body["data"]["profile"]["params"] == {"buy_below": 0.45, "sell_above": 0.6}
        assert os.path.exists(promoted_body["data"]["paths"]["profile_path"])

        executed = client.post(
            "/ops/polymarket/paper/strategy_once",
            json={"profile_path": str(profile_path), "execute": True},
            headers=headers,
        )
        assert executed.status_code == 200
        body = executed.json()
        assert body["ok"] is True
        assert body["data"]["config"]["account_id"] == "profile_ops"
        assert body["data"]["orders"][0]["status"] == "FILLED"
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_profile_promote_rejects_failed_guardrails(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_profile_reject.db').as_posix()}")

    import asyncio
    import json

    async def seed():
        await pm_db.init_pm_db()

    asyncio.run(seed())
    report_path = tmp_path / "bad_wf_report.json"
    report_path.write_text(
        json.dumps(
            {
                "base_config": {"strategy": "threshold", "order_size": 10.0},
                "token_ids": ["tok_a"],
                "summary": {
                    "segments": 1,
                    "positive_segments": 0,
                    "total_test_net_pnl": -2.0,
                    "worst_test_net_pnl": -2.0,
                    "selection_counts": [{"params": {"buy_below": 0.45, "sell_above": 0.60}, "segments": 1}],
                },
            }
        ),
        encoding="utf-8",
    )
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        rejected = client.post(
            "/ops/polymarket/paper/profile/promote",
            json={"report_path": str(report_path), "account_id": "bad", "output_path": str(tmp_path / "bad_profile.json")},
            headers=headers,
        )
        assert rejected.status_code == 200
        rejected_body = rejected.json()
        assert rejected_body["ok"] is False
        assert "profile promotion rejected" in rejected_body["error"]

        unsafe = client.post(
            "/ops/polymarket/paper/profile/promote",
            json={
                "report_path": str(report_path),
                "account_id": "bad",
                "output_path": str(tmp_path / "bad_profile.json"),
                "allow_unsafe": True,
            },
            headers=headers,
        )
        assert unsafe.status_code == 200
        unsafe_body = unsafe.json()
        assert unsafe_body["ok"] is True
        assert unsafe_body["data"]["profile"]["safe_to_execute"] is False
        assert unsafe_body["data"]["profile"]["validation"]["failures"]
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_replay_batch_route_auto_selects_tokens(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_replay_batch.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes(
            [
                _quote("tok_a", 0, 0.39, 0.41),
                _quote("tok_a", 1, 0.62, 0.64),
                _quote("tok_b", 0, 0.49, 0.51),
                _quote("tok_b", 1, 0.52, 0.54),
            ]
        )

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        resp = client.post(
            "/ops/polymarket/replay/batch",
            json={
                "top_n": 2,
                "min_quotes": 2,
                "since": "2026-03-03T00:00:00Z",
                "until": "2026-03-03T00:02:00Z",
                "strategy": "threshold",
                "account_prefix": "opsbatchtest",
                "initial_cash": 100.0,
                "order_size": 10.0,
                "buy_below": 0.45,
                "sell_above": 0.60,
                "max_order_notional": 100.0,
                "max_position_notional": 100.0,
            },
            headers=headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["data"]["token_ids"] == ["tok_a", "tok_b"]
        assert body["data"]["best"]["token_id"] == "tok_a"
        assert body["data"]["rows"][0]["net_pnl"] == 2.1
        assert "results" not in body["data"]
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_replay_grid_route_writes_report(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_replay_grid.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes(
            [
                _quote("tok_a", 0, 0.39, 0.41),
                _quote("tok_a", 1, 0.62, 0.64),
            ]
        )

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        resp = client.post(
            "/ops/polymarket/replay/grid",
            json={
                "token_ids": ["tok_a"],
                "since": "2026-03-03T00:00:00Z",
                "until": "2026-03-03T00:02:00Z",
                "strategy": "threshold",
                "account_prefix": "opsgridtest",
                "initial_cash": 100.0,
                "order_size": 10.0,
                "buy_below_grid": "0.35,0.45",
                "sell_above_grid": "0.60",
                "max_order_notional": 100.0,
                "max_position_notional": 100.0,
                "write_report": True,
                "output_dir": str(tmp_path / "reports"),
                "name": "ops_grid",
            },
            headers=headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["data"]["best"]["params"] == {"buy_below": 0.45, "sell_above": 0.6}
        assert body["data"]["best"]["total_net_pnl"] == 2.1
        assert body["data"]["paths"]["json_path"].endswith("ops_grid.json")
        assert os.path.exists(body["data"]["paths"]["json_path"])
        assert "reports" not in body["data"]
    finally:
        asyncio.run(pm_db.close_pm_db())


def test_ops_polymarket_replay_walk_forward_route(monkeypatch, tmp_path):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'ops_pm_replay_wf.db').as_posix()}")

    import asyncio

    async def seed():
        await pm_db.init_pm_db()
        await pm_db.insert_quotes(
            [
                _quote("tok_a", 0, 0.39, 0.41),
                _quote("tok_a", 1, 0.62, 0.64),
                _quote("tok_a", 2, 0.39, 0.41),
                _quote("tok_a", 3, 0.62, 0.64),
                _quote("tok_a", 4, 0.39, 0.41),
                _quote("tok_a", 5, 0.62, 0.64),
            ]
        )

    asyncio.run(seed())
    app = FastAPI()
    app.include_router(create_router())
    client = TestClient(app)
    headers = {"X-OPS-TOKEN": "test-token"}

    try:
        resp = client.post(
            "/ops/polymarket/replay/walk_forward",
            json={
                "token_ids": ["tok_a"],
                "since": "2026-03-03T00:00:00Z",
                "until": "2026-03-03T00:06:00Z",
                "strategy": "threshold",
                "account_prefix": "opswftest",
                "initial_cash": 100.0,
                "order_size": 10.0,
                "buy_below_grid": "0.35,0.45",
                "sell_above_grid": "0.60",
                "train_minutes": 2,
                "test_minutes": 2,
                "step_minutes": 2,
                "max_order_notional": 100.0,
                "max_position_notional": 100.0,
                "write_report": True,
                "output_dir": str(tmp_path / "reports"),
                "name": "ops_wf",
            },
            headers=headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["data"]["summary"]["segments"] == 2
        assert body["data"]["summary"]["total_test_net_pnl"] == 4.2
        assert body["data"]["rows"][0]["selected_params"] == {"buy_below": 0.45, "sell_above": 0.6}
        assert body["data"]["paths"]["markdown_path"].endswith("ops_wf.md")
        assert os.path.exists(body["data"]["paths"]["markdown_path"])
        assert "reports" not in body["data"]
    finally:
        asyncio.run(pm_db.close_pm_db())
