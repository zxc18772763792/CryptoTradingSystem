from __future__ import annotations

import json
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple
from zoneinfo import ZoneInfo


_RETRYABLE_OPENAI_HTTP_STATUSES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_FAILOVER_OPENAI_HTTP_STATUSES = frozenset(set(_RETRYABLE_OPENAI_HTTP_STATUSES) | {401, 403})
_RESPONSES_TOKEN_PARAM_CANDIDATES = ("max_output_tokens", "max_completion_tokens", "max_tokens", "")
_UNSUPPORTED_PARAMETER_RE = re.compile(r"unsupported parameter:\s*([A-Za-z0-9_]+)", re.IGNORECASE)
_OPENAI_TARGET_STATE_LOCK = threading.Lock()
_OPENAI_TARGET_PREFERRED: Dict[Tuple[str, ...], str] = {}
_OPENAI_FAILOVER_STATE_VERSION = 1
_OPENAI_FAILOVER_DEFAULT_TZ = "Asia/Shanghai"
# Keep failover ordering request-local by default. This avoids global sticky
# state leaking across independent requests/tests.
_ENABLE_CROSS_REQUEST_FAILOVER_CACHE = False


def responses_endpoint(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/responses"
    return f"{base}/v1/responses"


def chat_completions_endpoint(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


def build_openai_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {str(api_key or '').strip()}",
        "Content-Type": "application/json",
    }


def _split_endpoint_candidates(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        raw_items = (
            text.replace("\r", ",")
            .replace("\n", ",")
            .replace(";", ",")
            .split(",")
        )

    items: List[str] = []
    seen: set[str] = set()
    for item in raw_items:
        base_url = str(item or "").strip().rstrip("/")
        if not base_url or base_url in seen:
            continue
        seen.add(base_url)
        items.append(base_url)
    return items


def _split_api_key_candidates(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        raw_items = (
            text.replace("\r", ",")
            .replace("\n", ",")
            .replace(";", ",")
            .split(",")
        )

    items: List[str] = []
    for item in raw_items:
        api_key = str(item or "").strip()
        if api_key:
            items.append(api_key)
    return items


def _split_model_candidates(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        raw_items = (
            text.replace("\r", ",")
            .replace("\n", ",")
            .replace(";", ",")
            .split(",")
        )

    items: List[str] = []
    for item in raw_items:
        model = str(item or "").strip()
        if model:
            items.append(model)
    return items


def openai_endpoint_targets(
    *,
    primary_base_url: Any,
    backup_base_urls: Any = None,
    primary_api_key: Any = "",
    backup_api_key: Any = "",
    primary_model: Any = "",
    backup_model: Any = "",
) -> List[Dict[str, Any]]:
    base_urls = _split_endpoint_candidates(primary_base_url)
    for item in _split_endpoint_candidates(backup_base_urls):
        if item not in base_urls:
            base_urls.append(item)

    primary_keys = _split_api_key_candidates(primary_api_key)
    backup_keys = _split_api_key_candidates(backup_api_key)
    primary_models = _split_model_candidates(primary_model)
    backup_models = _split_model_candidates(backup_model)
    primary_key = primary_keys[0] if primary_keys else str(primary_api_key or "").strip()
    primary_model_name = primary_models[0] if primary_models else str(primary_model or "").strip()
    targets: List[Dict[str, Any]] = []
    for idx, base_url in enumerate(base_urls):
        if idx == 0:
            api_key = primary_key
            model_name = primary_model_name
        else:
            backup_idx = idx - 1
            if backup_keys:
                key_idx = backup_idx if backup_idx < len(backup_keys) else (len(backup_keys) - 1)
                api_key = backup_keys[key_idx]
            else:
                api_key = primary_key
            if backup_models:
                model_idx = backup_idx if backup_idx < len(backup_models) else (len(backup_models) - 1)
                model_name = backup_models[model_idx]
            else:
                model_name = primary_model_name
        if not api_key and backup_keys:
            api_key = backup_keys[-1]
        if not api_key and primary_key:
            api_key = primary_key
        if not model_name and backup_models:
            model_name = backup_models[-1]
        if not model_name and primary_model_name:
            model_name = primary_model_name
        targets.append(
            {
                "index": idx,
                "base_url": base_url,
                "api_key": api_key,
                "model": model_name,
                "is_backup": idx > 0,
            }
        )
    return targets


def is_retryable_openai_status(status: Any) -> bool:
    try:
        return int(status) in _RETRYABLE_OPENAI_HTTP_STATUSES
    except Exception:
        return False


def should_failover_openai_status(status: Any) -> bool:
    try:
        return int(status) in _FAILOVER_OPENAI_HTTP_STATUSES
    except Exception:
        return False


def responses_api_unavailable(status: Any, error_text: Any = "") -> bool:
    try:
        status_code = int(status)
    except Exception:
        status_code = 0

    text = str(error_text or "").strip().lower()
    if status_code in {404, 405, 501}:
        return True
    if "responses" not in text:
        return False
    return any(
        hint in text
        for hint in (
            "not found",
            "unsupported",
            "not support",
            "unknown route",
            "invalid path",
        )
    )


def _canonical_openai_targets(targets: Sequence[Mapping[str, Any]] | None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for pos, target in enumerate(targets or []):
        item = dict(target or {})
        base_url = str(item.get("base_url") or "").strip().rstrip("/")
        if not base_url:
            continue
        try:
            index = int(item.get("index"))
        except Exception:
            index = pos
        item["index"] = index
        item["base_url"] = base_url
        items.append(item)
    items.sort(key=lambda item: int(item.get("index", 0)))
    return items


def _openai_target_state_key(targets: Sequence[Mapping[str, Any]] | None) -> Tuple[str, ...]:
    return tuple(str(item.get("base_url") or "").rstrip("/") for item in _canonical_openai_targets(targets))


def _normalize_openai_failover_scope(scope: Any) -> str:
    return str(scope or "").strip().lower()


def _openai_failover_state_path() -> Path:
    raw = str(os.getenv("OPENAI_FAILOVER_STATE_PATH") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "runtime" / "openai_failover_state.json"


def _scoped_failover_enabled() -> bool:
    return bool(str(os.getenv("OPENAI_FAILOVER_STATE_PATH") or "").strip())


def _openai_failover_now() -> datetime:
    tz_name = str(os.getenv("OPENAI_FAILOVER_TZ") or _OPENAI_FAILOVER_DEFAULT_TZ).strip() or _OPENAI_FAILOVER_DEFAULT_TZ
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


def _openai_failover_today() -> str:
    return _openai_failover_now().date().isoformat()


@contextmanager
def _openai_failover_file_lock(path: Path):
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as handle:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            handle.write(b"0")
            handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_openai_failover_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": {}}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": {}}

    if not isinstance(payload, dict):
        return {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": {}}
    scopes = payload.get("scopes")
    if not isinstance(scopes, dict):
        scopes = {}
    return {
        "version": int(payload.get("version") or _OPENAI_FAILOVER_STATE_VERSION),
        "scopes": scopes,
    }


def _save_openai_failover_state(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp_path, path)


def _build_scope_failover_entry(
    base_urls: Sequence[str],
    *,
    day: str,
    mode: str = "primary",
    preferred_base_url: str = "",
) -> Dict[str, Any]:
    normalized_base_urls = [str(item or "").rstrip("/") for item in base_urls if str(item or "").strip()]
    primary_base_url = normalized_base_urls[0] if normalized_base_urls else ""
    backup_base_urls = normalized_base_urls[1:]
    normalized_mode = str(mode or "primary").strip().lower()
    if normalized_mode != "backup" or not backup_base_urls:
        normalized_mode = "primary"
        preferred = primary_base_url
    else:
        preferred = str(preferred_base_url or "").rstrip("/")
        if preferred not in backup_base_urls:
            preferred = backup_base_urls[0]
    return {
        "day": str(day or ""),
        "mode": normalized_mode,
        "preferred_base_url": preferred,
        "base_urls": normalized_base_urls,
        "updated_at": _openai_failover_now().isoformat(),
    }


def _load_scope_failover_entry(
    scope: str,
    canonical: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    normalized_scope = _normalize_openai_failover_scope(scope)
    base_urls = [str(item.get("base_url") or "").rstrip("/") for item in canonical]
    today = _openai_failover_today()
    default_entry = _build_scope_failover_entry(base_urls, day=today)
    if not _scoped_failover_enabled() or not normalized_scope or len(base_urls) <= 1:
        return default_entry

    path = _openai_failover_state_path()
    with _OPENAI_TARGET_STATE_LOCK:
        with _openai_failover_file_lock(path):
            state = _load_openai_failover_state(path)
            scopes = dict(state.get("scopes") or {})
            raw_entry = scopes.get(normalized_scope)
            if not isinstance(raw_entry, dict):
                raw_entry = {}
            raw_day = str(raw_entry.get("day") or "")
            if raw_day != today:
                raw_mode = "primary"
                raw_preferred = ""
            else:
                raw_mode = str(raw_entry.get("mode") or "primary").strip().lower()
                raw_preferred = str(raw_entry.get("preferred_base_url") or "").rstrip("/")
            entry = _build_scope_failover_entry(
                base_urls,
                day=today,
                mode=raw_mode,
                preferred_base_url=raw_preferred,
            )
            if raw_entry != entry:
                scopes[normalized_scope] = entry
                _save_openai_failover_state(
                    path,
                    {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": scopes},
                )
            return entry


def _remember_scope_failover_state(
    scope: str,
    canonical: Sequence[Mapping[str, Any]],
    *,
    success_base_url: str = "",
    failed_base_url: str = "",
) -> None:
    normalized_scope = _normalize_openai_failover_scope(scope)
    base_urls = [str(item.get("base_url") or "").rstrip("/") for item in canonical]
    if not _scoped_failover_enabled() or not normalized_scope or len(base_urls) <= 1:
        return

    normalized_success = str(success_base_url or "").rstrip("/")
    normalized_failure = str(failed_base_url or "").rstrip("/")
    if normalized_success and normalized_success not in base_urls:
        return
    if normalized_failure and normalized_failure not in base_urls:
        return

    today = _openai_failover_today()
    primary_base_url = base_urls[0]
    backup_base_urls = base_urls[1:]
    path = _openai_failover_state_path()
    with _OPENAI_TARGET_STATE_LOCK:
        with _openai_failover_file_lock(path):
            state = _load_openai_failover_state(path)
            scopes = dict(state.get("scopes") or {})
            raw_entry = scopes.get(normalized_scope)
            if not isinstance(raw_entry, dict):
                raw_entry = {}
            raw_day = str(raw_entry.get("day") or "")
            if raw_day != today:
                raw_mode = "primary"
                raw_preferred = ""
            else:
                raw_mode = str(raw_entry.get("mode") or "primary")
                raw_preferred = str(raw_entry.get("preferred_base_url") or "")
            entry = _build_scope_failover_entry(
                base_urls,
                day=today,
                mode=raw_mode,
                preferred_base_url=raw_preferred,
            )

            if normalized_success:
                if normalized_success == primary_base_url or not backup_base_urls:
                    entry = _build_scope_failover_entry(base_urls, day=today, mode="primary")
                else:
                    entry = _build_scope_failover_entry(
                        base_urls,
                        day=today,
                        mode="backup",
                        preferred_base_url=normalized_success,
                    )
            elif normalized_failure:
                if not backup_base_urls:
                    entry = _build_scope_failover_entry(base_urls, day=today, mode="primary")
                elif normalized_failure == primary_base_url:
                    entry = _build_scope_failover_entry(
                        base_urls,
                        day=today,
                        mode="backup",
                        preferred_base_url=backup_base_urls[0],
                    )
                else:
                    failed_backup_idx = backup_base_urls.index(normalized_failure)
                    next_backup = backup_base_urls[(failed_backup_idx + 1) % len(backup_base_urls)]
                    entry = _build_scope_failover_entry(
                        base_urls,
                        day=today,
                        mode="backup",
                        preferred_base_url=next_backup,
                    )

            scopes[normalized_scope] = entry
            _save_openai_failover_state(
                path,
                {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": scopes},
            )


def _rotate_targets(
    targets: Sequence[Mapping[str, Any]],
    preferred_base_url: str = "",
) -> List[Dict[str, Any]]:
    canonical = _canonical_openai_targets(targets)
    if len(canonical) <= 1:
        return canonical
    normalized_preferred = str(preferred_base_url or "").rstrip("/")
    if not normalized_preferred:
        return canonical
    start_idx = next(
        (idx for idx, item in enumerate(canonical) if str(item.get("base_url") or "").rstrip("/") == normalized_preferred),
        0,
    )
    return canonical[start_idx:] + canonical[:start_idx]


def prioritize_openai_targets(
    targets: Sequence[Mapping[str, Any]] | None,
    *,
    scope: Any = None,
) -> List[Dict[str, Any]]:
    canonical = _canonical_openai_targets(targets)
    if len(canonical) <= 1:
        return canonical
    normalized_scope = _normalize_openai_failover_scope(scope)
    if normalized_scope and _scoped_failover_enabled():
        entry = _load_scope_failover_entry(normalized_scope, canonical)
        if entry.get("mode") == "backup":
            backup_targets = canonical[1:]
            if backup_targets:
                preferred_base_url = str(entry.get("preferred_base_url") or "").rstrip("/")
                return _rotate_targets(backup_targets, preferred_base_url)
        preferred_base_url = str(entry.get("preferred_base_url") or "").rstrip("/")
        return _rotate_targets(canonical, preferred_base_url)
    if not _ENABLE_CROSS_REQUEST_FAILOVER_CACHE:
        return canonical

    key = _openai_target_state_key(canonical)
    with _OPENAI_TARGET_STATE_LOCK:
        preferred_base_url = _OPENAI_TARGET_PREFERRED.get(key, "")

    if not preferred_base_url:
        return canonical

    start_idx = next(
        (idx for idx, item in enumerate(canonical) if str(item.get("base_url") or "").rstrip("/") == preferred_base_url),
        0,
    )
    return canonical[start_idx:] + canonical[:start_idx]


def remember_openai_target_success(
    targets: Sequence[Mapping[str, Any]] | None,
    base_url: Any,
    *,
    scope: Any = None,
) -> None:
    canonical = _canonical_openai_targets(targets)
    normalized_scope = _normalize_openai_failover_scope(scope)
    if normalized_scope and len(canonical) > 1 and _scoped_failover_enabled():
        _remember_scope_failover_state(
            normalized_scope,
            canonical,
            success_base_url=str(base_url or "").strip().rstrip("/"),
        )
        return
    if not _ENABLE_CROSS_REQUEST_FAILOVER_CACHE:
        return

    if len(canonical) <= 1:
        return

    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return

    key = _openai_target_state_key(canonical)
    if not key or normalized not in key:
        return

    with _OPENAI_TARGET_STATE_LOCK:
        _OPENAI_TARGET_PREFERRED[key] = normalized


def remember_openai_target_failure(
    targets: Sequence[Mapping[str, Any]] | None,
    failed_base_url: Any,
    *,
    scope: Any = None,
) -> None:
    canonical = _canonical_openai_targets(targets)
    normalized_scope = _normalize_openai_failover_scope(scope)
    if normalized_scope and len(canonical) > 1 and _scoped_failover_enabled():
        _remember_scope_failover_state(
            normalized_scope,
            canonical,
            failed_base_url=str(failed_base_url or "").strip().rstrip("/"),
        )
        return
    if not _ENABLE_CROSS_REQUEST_FAILOVER_CACHE:
        return

    if len(canonical) <= 1:
        return

    normalized = str(failed_base_url or "").strip().rstrip("/")
    if not normalized:
        return

    base_urls = [str(item.get("base_url") or "").rstrip("/") for item in canonical]
    if normalized not in base_urls:
        return

    failed_idx = base_urls.index(normalized)
    next_base_url = base_urls[(failed_idx + 1) % len(base_urls)]
    key = tuple(base_urls)

    with _OPENAI_TARGET_STATE_LOCK:
        _OPENAI_TARGET_PREFERRED[key] = next_base_url


def reset_openai_target_preferences(scope: Any = None) -> None:
    """Clear relay preference cache used by cross-request failover mode."""
    with _OPENAI_TARGET_STATE_LOCK:
        _OPENAI_TARGET_PREFERRED.clear()
    path = _openai_failover_state_path()
    normalized_scope = _normalize_openai_failover_scope(scope)
    try:
        with _OPENAI_TARGET_STATE_LOCK:
            with _openai_failover_file_lock(path):
                state = _load_openai_failover_state(path)
                scopes = dict(state.get("scopes") or {})
                if normalized_scope:
                    scopes.pop(normalized_scope, None)
                else:
                    scopes.clear()
                if scopes:
                    _save_openai_failover_state(
                        path,
                        {"version": _OPENAI_FAILOVER_STATE_VERSION, "scopes": scopes},
                    )
                elif path.exists():
                    path.unlink()
    except Exception:
        return


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


def _normalize_chat_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("content") or "").strip()
                if text:
                    parts.append(text)
            else:
                text = str(item or "").strip()
                if text:
                    parts.append(text)
        return "\n".join(parts).strip()

    return str(content or "").strip()


def _collect_responses_instructions(messages: Sequence[Mapping[str, Any]]) -> str:
    parts: List[str] = []
    for message in messages or []:
        role = str(message.get("role") or "user").strip().lower() or "user"
        if role not in {"system", "developer"}:
            continue
        content = _normalize_chat_message_content(message.get("content"))
        if content:
            parts.append(content)
    return "\n\n".join(parts).strip()


def build_responses_payload(
    *,
    model: str,
    messages: Sequence[Mapping[str, Any]],
    max_output_tokens: int | None = None,
    output_token_param: str | None = "max_output_tokens",
    temperature: float | None = None,
    text_format: str | Dict[str, Any] | None = None,
    stream: bool | None = None,
    reasoning_effort: str | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": str(model or "").strip(),
        "input": [],
    }
    instructions = _collect_responses_instructions(messages)
    if instructions:
        payload["instructions"] = instructions

    for message in messages or []:
        role = str(message.get("role") or "user").strip().lower() or "user"
        if role in {"system", "developer"}:
            continue
        parts = _normalize_content_parts(message.get("content"))
        if not parts:
            continue
        payload["input"].append({"role": role, "content": parts})

    token_param = str(output_token_param or "").strip()
    if max_output_tokens is not None and token_param:
        payload[token_param] = int(max_output_tokens)
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


def build_chat_completions_payload(
    *,
    model: str,
    messages: Sequence[Mapping[str, Any]],
    max_tokens: int | None = None,
    temperature: float | None = None,
    response_format: str | Dict[str, Any] | None = None,
    stream: bool | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": str(model or "").strip(),
        "messages": [],
    }

    for message in messages or []:
        role = str(message.get("role") or "user").strip().lower() or "user"
        content = _normalize_chat_message_content(message.get("content"))
        if not content:
            continue
        payload["messages"].append({"role": role, "content": content})

    if max_tokens is not None:
        payload["max_tokens"] = int(max_tokens)
    if temperature is not None:
        payload["temperature"] = float(temperature)
    if response_format:
        fmt = response_format if isinstance(response_format, dict) else {"type": str(response_format)}
        payload["response_format"] = fmt
    if stream is not None:
        payload["stream"] = bool(stream)

    return payload


def build_responses_payload_variants(
    *,
    model: str,
    messages: Sequence[Mapping[str, Any]],
    max_output_tokens: int | None = None,
    temperature: float | None = None,
    text_format: str | Dict[str, Any] | None = None,
    stream: bool | None = None,
    reasoning_effort: str | None = None,
) -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for output_token_param in _RESPONSES_TOKEN_PARAM_CANDIDATES:
        payload = build_responses_payload(
            model=model,
            messages=messages,
            max_output_tokens=max_output_tokens,
            output_token_param=output_token_param or None,
            temperature=temperature,
            text_format=text_format,
            stream=stream,
            reasoning_effort=reasoning_effort,
        )
        key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        variants.append(payload)
    return variants


def unsupported_responses_parameter(error_text: Any) -> str | None:
    text = str(error_text or "").strip()
    if not text:
        return None
    match = _UNSUPPORTED_PARAMETER_RE.search(text)
    if match:
        return str(match.group(1) or "").strip() or None
    lowered = text.lower()
    for candidate in ("max_output_tokens", "max_completion_tokens", "max_tokens"):
        if candidate in lowered and "unsupported" in lowered:
            return candidate
    return None


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
