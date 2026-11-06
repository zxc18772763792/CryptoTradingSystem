"""Keep the existing forward protocol on its registered classifier model."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from scripts import telegram_forward_cycle as wrapper
from scripts import telegram_classifier_transport as adapter
from core.news.eventizer import async_glm_client as clients


def test_forward_classifier_never_uses_different_model_backup(tmp_path, monkeypatch):
    called_models = []

    class RoutedClient:
        def __init__(self, cfg=None):
            self._model = "gemini-2.5-flash"
            self._base_url = "https://primary.invalid/v1"
            self._api_key = "synthetic-primary-secret"
            # A sticky news failover may prefer the differently-modelled backup.
            self._endpoint_targets = [
                {"model": "gpt-6-luna", "base_url": "https://backup.invalid/v1", "api_key": "synthetic-backup-secret"},
                {"model": "gemini-2.5-flash", "base_url": "https://primary.invalid/v1", "api_key": "synthetic-primary-secret"},
            ]

        def _available_endpoint_targets(self):
            return list(self._endpoint_targets)

        async def chat_completions(self, messages, **kwargs):
            model = self._available_endpoint_targets()[0]["model"]
            called_models.append(model)
            return {"_cts_openai_target": {"model": model},
                    "choices": [{"message": {"content": json.dumps({"labels": [{"id": "0", "kind": "technical", "events": []}]})}}]}, "none"

    row = {"source_id": "synthetic:1", "text_sha256": "synthetic-hash", "channel": "BinanceChinese", "mode": "forward",
           "content": "synthetic private message must not appear in transport diagnostics"}
    saved = []
    store = SimpleNamespace(root=tmp_path, rows=lambda *_: [row], labels=lambda *_: {},
                            state=lambda key, default=None: {"classifier_model": "gemini-2.5-flash"} if key == "activation" else default,
                            save_label=lambda *args, **kwargs: saved.append(args), db=SimpleNamespace(commit=Mock()))
    monkeypatch.setattr(clients, "AsyncGLMClient", RoutedClient)
    monkeypatch.setattr(wrapper.cli, "rule_kind", lambda _: "needs_model")
    monkeypatch.setattr(wrapper.cli, "enforce_eligibility", lambda value, _: value)
    result = asyncio.run(wrapper.cli.label(store))
    assert result["failures"] == [] and len(saved) == 1
    assert called_models == ["gemini-2.5-flash"]
    # Restore the shared class after this isolated child-process adapter call.
    assert clients.AsyncGLMClient is RoutedClient


@pytest.mark.parametrize("targets", [[], [{"model": "gpt-6-luna", "api_key": "synthetic", "base_url": "https://backup.invalid"}]])
def test_no_matching_model_stops_before_any_request(targets):
    class Client:
        def __init__(self, cfg=None):
            self._endpoint_targets = targets
        def _available_endpoint_targets(self):
            return self._endpoint_targets
    observed = []
    pinned = adapter.protocol_client(Client, "gemini-2.5-flash", observed.append)
    with pytest.raises(adapter.FrozenClassifierUnavailable):
        pinned({})
    assert observed == [{"kind": "routing", "matching_endpoints": 0}]


def test_reply_model_guard_also_applies_to_semantic_review():
    class Client:
        def __init__(self, cfg=None):
            self._endpoint_targets = [{"model": "gemini-2.5-flash", "api_key": "synthetic-secret", "base_url": "https://primary.invalid"}]
        def _available_endpoint_targets(self):
            return self._endpoint_targets
        async def chat_completions(self, messages, **kwargs):
            return {"_cts_openai_target": {"model": "gpt-6-luna"}, "private_response": "not for diagnostics"}, "none"
    records = []
    client = adapter.protocol_client(Client, "gemini-2.5-flash", records.append)()
    with pytest.raises(adapter.FrozenClassifierMismatch):
        asyncio.run(client.chat_completions([{"role": "system", "content": "semantic review"}]))
    assert records[-1] == {"kind": "request", "outcome": "model_mismatch", "model_matched": False}
    assert "private_response" not in json.dumps(records)


def test_same_model_timeout_is_not_retried_or_rerouted():
    calls = []
    class Client:
        def __init__(self, cfg=None):
            self._endpoint_targets = [{"model": "gemini-2.5-flash", "api_key": "synthetic-secret", "base_url": "https://primary.invalid"}]
        def _available_endpoint_targets(self):
            return self._endpoint_targets
        async def chat_completions(self, messages, **kwargs):
            calls.append(kwargs)
            return {}, "timeout"
    client = adapter.protocol_client(Client, "gemini-2.5-flash", lambda _: None)()
    assert asyncio.run(client.chat_completions([], timeout=75)) == ({}, "timeout")
    assert calls == [{"timeout": 75}]


def test_failure_restores_shared_class_and_records_no_message_or_secret(tmp_path, monkeypatch):
    original = clients.AsyncGLMClient
    store = SimpleNamespace(root=tmp_path, state=lambda *_: {"classifier_model": "gemini-2.5-flash"})
    async def failed(store, **kwargs):
        assert clients.AsyncGLMClient is not original
        raise RuntimeError("synthetic private message and secret must not be logged")
    with pytest.raises(RuntimeError):
        asyncio.run(adapter.label_with_protocol_client(failed, store))
    assert clients.AsyncGLMClient is original
    diagnostic = json.loads((tmp_path/"classifier_transport_status.json").read_text(encoding="utf-8"))
    assert diagnostic["outcome"] == "failed" and diagnostic["error_type"] == "RuntimeError"
    assert "synthetic private message" not in json.dumps(diagnostic)


def test_cancelled_label_also_restores_shared_class(tmp_path):
    original = clients.AsyncGLMClient
    store = SimpleNamespace(root=tmp_path, state=lambda *_: {"classifier_model": "gemini-2.5-flash"})
    async def cancelled(store, **kwargs):
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(adapter.label_with_protocol_client(cancelled, store))
    assert clients.AsyncGLMClient is original


def test_historical_labelling_keeps_original_path(tmp_path):
    store = SimpleNamespace(root=tmp_path, state=lambda *_: {"classifier_model": "gemini-2.5-flash"})
    label = AsyncMock(return_value={"historical": True})
    assert asyncio.run(adapter.label_with_protocol_client(label, store, historical=True)) == {"historical": True}
    label.assert_awaited_once_with(store, historical=True)
    assert not (tmp_path/"classifier_transport_status.json").exists()
