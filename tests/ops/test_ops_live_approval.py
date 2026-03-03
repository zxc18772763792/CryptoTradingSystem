from __future__ import annotations

from datetime import timedelta

from core.ops.service import api as ops_api


def test_ops_live_approval_flow(client, ops_app, ops_headers, monkeypatch):
    started = {"count": 0}

    async def fake_start_live():
        started["count"] += 1

    monkeypatch.setattr(ops_api, "_ensure_live_mode_started", fake_start_live)

    arm_resp = client.post("/ops/trading/arm_live", headers=ops_headers)
    assert arm_resp.status_code == 200
    approval_code = arm_resp.json()["data"]["approval_code"]
    assert approval_code

    missing_resp = client.post("/ops/trading/start_live", headers=ops_headers)
    assert missing_resp.status_code == 403

    wrong_resp = client.post(
        "/ops/trading/start_live",
        headers={**ops_headers, "X-OPS-APPROVAL": "wrong-code"},
    )
    assert wrong_resp.status_code == 403

    expired_code = "EXPIRED001"
    ops_app.state.live_approvals[expired_code] = {
        "approval_code": expired_code,
        "issued_at": ops_api._now_utc() - timedelta(seconds=180),
        "expires_at": ops_api._now_utc() - timedelta(seconds=1),
        "actor": "pytest",
        "used": False,
    }
    expired_resp = client.post(
        "/ops/trading/start_live",
        headers={**ops_headers, "X-OPS-APPROVAL": expired_code},
    )
    assert expired_resp.status_code == 403

    ok_resp = client.post(
        "/ops/trading/start_live",
        headers={**ops_headers, "X-OPS-APPROVAL": approval_code},
    )
    assert ok_resp.status_code == 200
    assert ok_resp.json()["ok"] is True
    assert started["count"] == 1

    reused_resp = client.post(
        "/ops/trading/start_live",
        headers={**ops_headers, "X-OPS-APPROVAL": approval_code},
    )
    assert reused_resp.status_code == 403
