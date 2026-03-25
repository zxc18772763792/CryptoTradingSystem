from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence


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

    return payload


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
