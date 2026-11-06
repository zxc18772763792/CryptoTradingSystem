"""Protocol-scoped model routing outside the frozen Telegram research code.

The news client can fail over to a different model. The forward protocol cannot:
classification and semantic review must both use its registered classifier.
This adapter is installed only inside the isolated, study-locked cycle child.
"""
from datetime import datetime, timezone
import json


class FrozenClassifierUnavailable(RuntimeError):
    pass


class FrozenClassifierMismatch(RuntimeError):
    pass


def protocol_client(base_client, expected_model, observe):
    class ProtocolClient(base_client):
        def __init__(self, cfg=None):
            super().__init__(cfg)
            targets = [target for target in self._available_endpoint_targets()
                       if str(target.get("model") or "").strip() == expected_model]
            observe({"kind": "routing", "matching_endpoints": len(targets)})
            if not targets:
                raise FrozenClassifierUnavailable("Frozen classifier has no configured matching endpoint")
            self._endpoint_targets = targets
            self._model = expected_model
            self._base_url = targets[0]["base_url"]
            self._api_key = targets[0]["api_key"]

        async def chat_completions(self, messages, **kwargs):
            try:
                response, error = await super().chat_completions(messages, **kwargs)
            except Exception as exc:
                observe({"kind": "request", "outcome": "exception", "error_type": type(exc).__name__})
                raise
            if error != "none":
                observe({"kind": "request", "outcome": "unavailable",
                         "error_type": error if error in {"timeout", "rate_limit", "other"} else "other"})
                return response, error
            if not isinstance(response, dict):
                observe({"kind": "request", "outcome": "invalid_response"})
                raise ValueError("Invalid classifier response")
            target = response.get("_cts_openai_target") or {}
            actual = str(target.get("model") or response.get("model") or self._model)
            matched = actual == expected_model
            observe({"kind": "request", "outcome": "ok" if matched else "model_mismatch",
                     "model_matched": matched})
            if not matched:
                # Also guard the semantic-review call, which otherwise had no model check.
                raise FrozenClassifierMismatch("Classifier reply does not match the frozen model")
            return response, error

    return ProtocolClient


async def label_with_protocol_client(original_label, store, *, historical=False):
    activation = store.state("activation", {}) or {}
    expected = str(activation.get("classifier_model") or "").strip()
    if historical or not expected:
        return await original_label(store, historical=historical)

    from core.news.eventizer import async_glm_client as clients

    original_client = clients.AsyncGLMClient
    events = []
    started = datetime.now(timezone.utc).isoformat()
    outcome, error_type = "failed", None
    clients.AsyncGLMClient = protocol_client(original_client, expected, events.append)
    try:
        result = await original_label(store, historical=historical)
        outcome = "ok"
        return result
    except Exception as exc:
        error_type = type(exc).__name__
        raise
    finally:
        clients.AsyncGLMClient = original_client
        # Derived diagnostics only: never save message text, prompts, provider
        # bodies, endpoint URLs, credentials, author IDs or frozen decisions.
        diagnostic = {"started_at": started, "at": datetime.now(timezone.utc).isoformat(),
                      "classifier_model": expected, "outcome": outcome, "error_type": error_type,
                      "events": events[-60:], "different_model_fallback_allowed": False}
        try:
            path = store.root / "classifier_transport_status.json"
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text(json.dumps(diagnostic, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(path)
        except OSError:
            # Observability cannot turn a successful frozen label pass into a failure.
            pass
