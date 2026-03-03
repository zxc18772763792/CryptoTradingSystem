from __future__ import annotations


def test_ops_auth_requires_token(client):
    response = client.get("/ops/health")
    assert response.status_code == 401


def test_ops_auth_rejects_wrong_token(client):
    response = client.get("/ops/health", headers={"X-OPS-TOKEN": "wrong"})
    assert response.status_code == 401


def test_ops_auth_accepts_valid_token(client, ops_headers):
    response = client.get("/ops/health", headers=ops_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["service"] == "ops"
