from __future__ import annotations

import json
from urllib.parse import urlparse

import scripts.selfcheck_paper_longrun as paper_longrun


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else str(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _fake_request_factory(routes, seen):
    def _fake_request(method, url, headers=None, json=None, timeout=None):
        parsed = urlparse(url)
        key = (method.upper(), parsed.path)
        seen.append({
            "method": method.upper(),
            "path": parsed.path,
            "headers": dict(headers or {}),
            "json": json,
            "timeout": timeout,
        })
        response = routes.get(key)
        if response is None:
            raise AssertionError(f"unexpected request: {key}")
        return response

    return _fake_request


def test_selfcheck_main_happy_path(monkeypatch, capsys):
    routes = {
        ("GET", "/health"): FakeResponse(200, {"status": "healthy", "timestamp": "2026-04-13T00:00:00Z"}),
        ("GET", "/api/status"): FakeResponse(
            200,
            {
                "status": "running",
                "paper_trading": True,
                "engine_running": True,
                "risk": {"trading_halted": False},
            },
        ),
        ("GET", "/ops/health"): FakeResponse(
            200,
            {
                "ok": True,
                "service": "ops",
                "engine_running": True,
                "trading_mode": "paper",
                "risk_halted": False,
                "news_llm_queue_pending": 0,
            },
        ),
        ("GET", "/ops/status"): FakeResponse(
            200,
            {
                "ok": True,
                "data": {
                    "execution_engine": {"mode": "paper", "queue_worker_alive": True, "conditional_orders_count": 0},
                    "risk_manager": {"trading_halted": False},
                },
            },
        ),
        ("POST", "/ops/news/worker_run_once"): FakeResponse(
            200,
            {
                "ok": True,
                "data": {
                    "source_states": [],
                    "llm_queue": {"pending_total": 0},
                },
            },
        ),
    }
    seen = []
    monkeypatch.setattr(paper_longrun.requests, "request", _fake_request_factory(routes, seen))

    exit_code = paper_longrun.main(
        [
            "--base-url",
            "http://127.0.0.1:8000",
            "--token",
            "test-token",
            "--timeout",
            "3",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "paper-longrun selfcheck: PASS" in captured.err
    payload = json.loads(captured.out)
    assert payload["overall_ok"] is True
    assert payload["safe_state"]["ok"] is True
    assert [item["name"] for item in payload["checks"]] == [
        "process_health",
        "web_status",
        "ops_health",
        "ops_status",
        "run_once_probe",
    ]
    assert any(call["headers"].get("X-OPS-TOKEN") == "test-token" for call in seen)
    assert any(call["json"] == {"sources": [], "llm_limit": 1, "pull_only": True, "llm_only": True} for call in seen)


def test_selfcheck_fails_when_safe_state_is_unsafe(monkeypatch, capsys):
    routes = {
        ("GET", "/health"): FakeResponse(200, {"status": "healthy", "timestamp": "2026-04-13T00:00:00Z"}),
        ("GET", "/api/status"): FakeResponse(
            200,
            {
                "status": "running",
                "paper_trading": False,
                "engine_running": True,
                "risk": {"trading_halted": True},
            },
        ),
        ("GET", "/ops/health"): FakeResponse(
            200,
            {
                "ok": True,
                "service": "ops",
                "engine_running": True,
                "trading_mode": "live",
                "risk_halted": True,
            },
        ),
        ("GET", "/ops/status"): FakeResponse(
            200,
            {
                "ok": True,
                "data": {
                    "execution_engine": {"mode": "live", "queue_worker_alive": True, "conditional_orders_count": 0},
                    "risk_manager": {"trading_halted": True},
                },
            },
        ),
        ("POST", "/ops/news/worker_run_once"): FakeResponse(
            200,
            {"ok": True, "data": {"source_states": [], "llm_queue": {"pending_total": 0}}},
        ),
    }
    monkeypatch.setattr(paper_longrun.requests, "request", _fake_request_factory(routes, []))

    exit_code = paper_longrun.main(
        [
            "--base-url",
            "http://127.0.0.1:8000",
            "--token",
            "test-token",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "safe_state: FAIL" in captured.err
    payload = json.loads(captured.out)
    assert payload["overall_ok"] is False
    assert payload["safe_state"]["ok"] is False
    assert payload["checks"][1]["ok"] is False
    assert "unsafe runtime state" in payload["checks"][1]["error"]


def test_selfcheck_returns_nonzero_when_run_once_fails(monkeypatch, capsys):
    routes = {
        ("GET", "/health"): FakeResponse(200, {"status": "healthy", "timestamp": "2026-04-13T00:00:00Z"}),
        ("GET", "/api/status"): FakeResponse(
            200,
            {
                "status": "running",
                "paper_trading": True,
                "engine_running": True,
                "risk": {"trading_halted": False},
            },
        ),
        ("GET", "/ops/health"): FakeResponse(200, {"ok": True, "service": "ops", "engine_running": True, "trading_mode": "paper"}),
        ("GET", "/ops/status"): FakeResponse(
            200,
            {
                "ok": True,
                "data": {
                    "execution_engine": {"mode": "paper", "queue_worker_alive": True, "conditional_orders_count": 0},
                    "risk_manager": {"trading_halted": False},
                },
            },
        ),
        ("POST", "/ops/news/worker_run_once"): FakeResponse(500, {"ok": False, "error": "boom"}),
    }
    monkeypatch.setattr(paper_longrun.requests, "request", _fake_request_factory(routes, []))

    exit_code = paper_longrun.main(
        [
            "--base-url",
            "http://127.0.0.1:8000",
            "--token",
            "test-token",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["overall_ok"] is False
    assert payload["checks"][-1]["name"] == "run_once_probe"
    assert payload["checks"][-1]["ok"] is False
    assert "run-once endpoint failed" in payload["checks"][-1]["error"]
