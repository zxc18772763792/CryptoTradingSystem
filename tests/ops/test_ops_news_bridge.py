from __future__ import annotations

from core.ops.service import api as ops_api


def test_ops_news_pull_bridges_to_service(client, ops_headers, monkeypatch):
    captured = {}

    def fake_cfg():
        return {"defaults": {"news_sources": ["jin10", "rss"]}}

    async def fake_pull(cfg, payload):
        captured["cfg"] = cfg
        captured["payload"] = payload
        return {"pulled_count": 3, "queued_count": 2, "source_stats": {"jin10": {"pulled": 3}}}

    monkeypatch.setattr(ops_api, "load_service_config", fake_cfg)
    monkeypatch.setattr(ops_api, "run_ingest_pull_now", fake_pull)

    response = client.post(
        "/ops/news/pull_now",
        headers=ops_headers,
        json={"since_minutes": 60, "max_records": 50, "query": "btc"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["pulled_count"] == 3
    assert captured["payload"].since_minutes == 60
    assert captured["payload"].query == "btc"


def test_ops_worker_run_once_bridges_to_worker(client, ops_headers, monkeypatch):
    calls = {"pull": None, "llm": None}

    def fake_cfg():
        return {"defaults": {"news_sources": ["jin10", "rss"]}}

    async def fake_pull_cycle(cfg, sources):
        calls["pull"] = {"cfg": cfg, "sources": sources}
        return {"pulled_count": 5}

    async def fake_process_llm(cfg, limit):
        calls["llm"] = {"cfg": cfg, "limit": limit}
        return {"processed": 4}

    async def fake_list_source_states():
        return [{"source": "jin10", "error_count": 0}]

    async def fake_llm_stats():
        return {"pending_total": 1, "done": 2}

    monkeypatch.setattr(ops_api, "load_service_config", fake_cfg)
    monkeypatch.setattr(ops_api, "run_pull_cycle", fake_pull_cycle)
    monkeypatch.setattr(ops_api, "process_llm_batch", fake_process_llm)
    monkeypatch.setattr(ops_api.news_db, "list_source_states", fake_list_source_states)
    monkeypatch.setattr(ops_api.news_db, "get_llm_queue_stats", fake_llm_stats)

    response = client.post(
        "/ops/news/worker_run_once",
        headers=ops_headers,
        json={"sources": ["jin10"], "llm_limit": 7},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["pull"]["pulled_count"] == 5
    assert payload["data"]["llm"]["processed"] == 4
    assert calls["pull"]["sources"] == ["jin10"]
    assert calls["llm"]["limit"] == 7
