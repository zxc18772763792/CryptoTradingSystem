from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Sequence, Tuple


def responses_endpoint(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/responses"
    return f"{base}/v1/responses"


def build_openai_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {str(api_key or '').strip()}",
        "Content-Type": "application/json",
    }


def _normalize_content_parts(content: Any) -> List[Dict[str, str]]:
    if isinstance(content, str):
        text = content.strip()
        return [{"type": "input_text", "text": text}] if text else []

    if isinstance(content, list):
        parts: List[Dict[str, str]] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("content") or "").strip()
                if text:
                    parts.append({"type": "input_text", "text": text})
            else:
                text = str(item or "").strip()
                if text:
                    parts.append({"type": "input_text", "text": text})
        return parts

    text = str(content or "").strip()
    return [{"type": "input_text", "text": text}] if text else []


def build_responses_payload(
    *,
    model: str,
    messages: Sequence[Mapping[str, Any]],
    max_output_tokens: int | None = None,
    temperature: float | None = None,
    text_format: str | Dict[str, Any] | None = None,
    stream: bool | None = None,
    reasoning_effort: str | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": str(model or "").strip(),
        "input": [],
    }

    for message in messages or []:
        role = str(message.get("role") or "user").strip().lower() or "user"
        parts = _normalize_content_parts(message.get("content"))
        if not parts:
            continue
        payload["input"].append({"role": role, "content": parts})

    if max_output_tokens is not None:
        payload["max_output_tokens"] = int(max_output_tokens)
    if temperature is not None:
        payload["temperature"] = float(temperature)
    if text_format:
        fmt = text_format if isinstance(text_format, dict) else {"type": str(text_format)}
        payload["text"] = {"format": fmt}
    if stream is not None:
        payload["stream"] = bool(stream)
    effort = str(reasoning_effort or "").strip().lower()
    if effort in {"minimal", "low", "medium", "high"}:
        payload["reasoning"] = {"effort": effort}

    return payload


def _iter_sse_events(raw_text: str) -> List[Tuple[str, str]]:
    events: List[Tuple[str, str]] = []
    event_name = ""
    data_lines: List[str] = []

    def _flush() -> None:
        nonlocal event_name, data_lines
        if not data_lines:
            event_name = ""
            return
        events.append((event_name, "\n".join(data_lines).strip()))
        event_name = ""
        data_lines = []

    for line in str(raw_text or "").splitlines():
        stripped = line.rstrip("\r")
        if not stripped:
            _flush()
            continue
        if stripped.startswith(":"):
            continue
        if stripped.startswith("event:"):
            event_name = stripped[6:].strip()
            continue
        if stripped.startswith("data:"):
            data_lines.append(stripped[5:].lstrip())
    _flush()
    return events


def parse_responses_body(raw_text: str, *, content_type: str = "") -> Dict[str, Any]:
    raw = str(raw_text or "").strip()
    if not raw:
        return {}

    normalized_content_type = str(content_type or "").strip().lower()

    def _load_json(text: str) -> Dict[str, Any]:
        payload = json.loads(text)
        if isinstance(payload, dict):
            if isinstance(payload.get("response"), dict):
                return dict(payload["response"])
            return payload
        return {}

    if raw.startswith("{") or raw.startswith("[") or "json" in normalized_content_type:
        try:
            parsed = _load_json(raw)
            if parsed:
                return parsed
        except Exception:
            pass

    if "event-stream" in normalized_content_type or raw.startswith("event:") or raw.startswith("data:"):
        deltas: List[str] = []
        last_response: Dict[str, Any] = {}
        last_payload: Dict[str, Any] = {}

        for event_name, data_text in _iter_sse_events(raw):
            if not data_text or data_text == "[DONE]":
                continue
            try:
                payload = json.loads(data_text)
            except Exception:
                payload = {"text": data_text}

            if isinstance(payload, dict):
                if isinstance(payload.get("response"), dict):
                    last_response = dict(payload["response"])
                elif isinstance(payload.get("output"), list) or payload.get("object") == "response" or "output_text" in payload:
                    last_response = payload
                last_payload = payload

            if event_name.endswith("output_text.delta") and isinstance(payload, dict):
                delta = str(payload.get("delta") or payload.get("text") or payload.get("output_text") or "").strip()
                if delta:
                    deltas.append(delta)
            elif event_name.endswith("output_text.done") and isinstance(payload, dict):
                done_text = str(payload.get("text") or payload.get("output_text") or "").strip()
                if done_text and not deltas:
                    deltas.append(done_text)

        if last_response:
            return last_response
        if deltas:
            return {"output_text": "".join(deltas)}
        if last_payload:
            return last_payload

    try:
        return _load_json(raw)
    except Exception:
        return {}


async def read_aiohttp_responses_json(response: Any) -> Dict[str, Any]:
    content_type = str((getattr(response, "headers", {}) or {}).get("content-type") or "").lower()
    try:
        data = await response.json()
    except Exception:
        body = await response.text()
        return parse_responses_body(body, content_type=content_type)

    if isinstance(data, dict):
        if isinstance(data.get("response"), dict):
            return dict(data["response"])
        return data

    body = await response.text()
    return parse_responses_body(body, content_type=content_type)


def read_requests_responses_json(response: Any) -> Dict[str, Any]:
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("content-type") or "").lower()
    try:
        data = response.json()
    except Exception:
        return parse_responses_body(getattr(response, "text", ""), content_type=content_type)

    if isinstance(data, dict):
        if isinstance(data.get("response"), dict):
            return dict(data["response"])
        return data

    return parse_responses_body(getattr(response, "text", ""), content_type=content_type)


def _extract_text_from_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: List[str] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("content") or "").strip()
                if text:
                    chunks.append(text)
            else:
                text = str(item or "").strip()
                if text:
                    chunks.append(text)
        return "\n".join(chunks).strip()
    return str(content or "").strip()


def extract_response_text(data: Any) -> str:
    if isinstance(data, dict) and isinstance(data.get("response"), dict):
        data = data.get("response")
    if not isinstance(data, dict):
        return ""

    direct = data.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else {}
        text = _extract_text_from_content(message.get("content") if isinstance(message, dict) else "")
        if text:
            return text

    output = data.get("output")
    if isinstance(output, list):
        chunks: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if str(part.get("type") or "") not in {"output_text", "text"} and "text" not in part:
                        continue
                    text = str(part.get("text") or "").strip()
                    if text:
                        chunks.append(text)
            else:
                text = _extract_text_from_content(content)
                if text:
                    chunks.append(text)
        joined = "\n".join(chunks).strip()
        if joined:
            return joined

    text = _extract_text_from_content(data.get("content"))
    if text:
        return text
    return ""


def coerce_responses_to_chat_completions(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict) and isinstance(data.get("choices"), list):
        return data
    text = extract_response_text(data)
    if not text:
        return {}
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": text,
                }
            }
        ]
    }
