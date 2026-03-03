from __future__ import annotations

import time
from pathlib import Path

from core.ops.service import api as ops_api


def test_ops_research_background_job_and_latest(client, ops_app, ops_headers, tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "research.csv"
    md_path = tmp_path / "research.md"

    async def fake_run_strategy_research(config):
        csv_path.write_text("strategy,return\nema,1.0\n", encoding="utf-8")
        md_path.write_text("# report\n", encoding="utf-8")
        return {
            "csv_path": str(csv_path),
            "markdown_path": str(md_path),
            "best": {"strategy": "EMAStrategy", "return_pct": 1.0},
        }

    monkeypatch.setattr(ops_api, "run_strategy_research", fake_run_strategy_research)

    response = client.post(
        "/ops/research/run",
        headers=ops_headers,
        json={
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "days": 10,
            "timeframes": ["1m", "5m"],
            "background": True,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    job_id = payload["data"]["job_id"]

    deadline = time.time() + 2.0
    job_payload = None
    while time.time() < deadline:
        job_response = client.get(f"/ops/research/job/{job_id}", headers=ops_headers)
        assert job_response.status_code == 200
        job_payload = job_response.json()["data"]
        if job_payload["status"] in {"completed", "failed"}:
            break
        time.sleep(0.05)

    assert job_payload is not None
    assert job_payload["status"] == "completed"

    latest_response = client.get("/ops/research/latest", headers=ops_headers)
    assert latest_response.status_code == 200
    latest_payload = latest_response.json()
    assert latest_payload["ok"] is True
    assert latest_payload["data"]["job_id"] == job_id
    assert latest_payload["data"]["csv_path"] == str(csv_path)
