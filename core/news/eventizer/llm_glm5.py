"""GLM-5 event extraction with strict JSON validation and rules fallback."""
from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from loguru import logger

from config.settings import settings
from core.news.eventizer.rules import SymbolMapper, extract_events_rules
from core.news.storage.models import EVENT_TYPES, EventSchema
from core.utils.openai_responses import (
    anthropic_messages_endpoint,
    build_anthropic_messages_payload,
    build_openai_headers,
    build_target_headers,
    build_chat_completions_payload,
    build_responses_payload,
    build_responses_payload_variants,
    chat_completions_endpoint,
    clear_openai_target_chat_preference,
    coerce_responses_to_chat_completions,
    extract_response_text,
    openai_endpoint_targets,
    prioritize_openai_targets,
    read_requests_responses_json,
    remember_openai_target_chat_preference,
    remember_openai_target_failure,
    remember_openai_target_success,
    responses_endpoint,
    responses_api_unavailable,
    should_failover_openai_status,
    should_prefer_openai_target_chat_completions,
    target_transport,
    unsupported_responses_parameter,
)


DEFAULT_OPENAI_BASE_URL = "https://sub.a-j.app/v1"
DEFAULT_OPENAI_MODEL = "gpt-5.4"
_OPENAI_FAILOVER_SCOPE = "news"
_LEGACY_PROVIDER_ALIASES = {"glm", "glm5", "zhipu"}
_LEGACY_BASE_URL_HINTS = ("bigmodel.cn", "zhipu")
_RUNTIME_SETTING_NAMES = (
    "OPENAI_API_KEY",
    "OPENAI_BACKUP_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_BACKUP_BASE_URL",
    "OPENAI_MODEL",
    "OPENAI_BACKUP_MODEL",
)
_INITIAL_RUNTIME_SETTINGS = {
    name: str(getattr(settings, name, "") or "").strip()
    for name in _RUNTIME_SETTING_NAMES
}
_SUMMARY_CACHE: Dict[str, Dict[str, Any]] = {}
_SUMMARY_CACHE_MAX = 4000
_POS_SENTIMENT_HINTS = {
    "approved", "approval", "approve", "etf inflow", "net inflow", "listing", "listed", "partnership",
    "launch", "bullish", "surge", "rally", "soar", "soars", "jump", "jumps", "upgrade", "integration", "adoption", "buyback",
    "获批", "批准", "通过", "上架", "上线", "利好", "大涨", "拉升", "增持", "流入", "合作", "升级", "采用",
}
_NEG_SENTIMENT_HINTS = {
    "hack", "hacked", "exploit", "drained", "stolen", "outage", "suspend withdrawals", "withdrawals suspended",
    "lawsuit", "sues", "crackdown", "ban", "delist", "delisting", "liquidation", "liquidations", "bearish", "sell-off", "dump",
    "crash", "crashes", "plunge", "plunges", "slump", "slumps", "drop", "drops", "falls",
    "黑客", "被盗", "攻击", "漏洞", "宕机", "暂停提现", "暂停交易", "诉讼", "起诉", "打击", "封禁", "下架", "爆仓", "清算", "利空", "暴跌",
}


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _merge_csv_values(*values: Any) -> str:
    merged: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_openai_base_urls(value)
        for item in normalized.split(","):
            part = str(item or "").strip().rstrip("/")
            if not part or part in seen:
                continue
            seen.add(part)
            merged.append(part)
    return ",".join(merged)


def _normalize_openai_base_urls(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        parts = [_normalize_openai_base_urls(item) for item in value]
        return ",".join([part for part in parts if part])

    text = str(value or "").strip()
    if not text:
        return ""

    cleaned: List[str] = []
    for raw_part in text.split(","):
        part = str(raw_part or "").strip().rstrip("/")
        if not part:
            continue
        lowered = part.lower()
        if any(hint in lowered for hint in _LEGACY_BASE_URL_HINTS):
            continue
        cleaned.append(part)
    return ",".join(cleaned)


def _normalize_openai_model(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return DEFAULT_OPENAI_MODEL
    if text.lower().startswith("glm"):
        return DEFAULT_OPENAI_MODEL
    return text


def _runtime_setting(name: str) -> str:
    current = str(getattr(settings, name, "") or "").strip()
    if current != _INITIAL_RUNTIME_SETTINGS.get(name, ""):
        return current
    return str(os.getenv(name) or current or "").strip()


def _openai_primary_api_key() -> str:
    return _runtime_setting("OPENAI_API_KEY")


def _openai_backup_api_key() -> str:
    return _runtime_setting("OPENAI_BACKUP_API_KEY")


def _openai_api_key() -> str:
    primary = _openai_primary_api_key()
    if primary:
        return primary
    return _openai_backup_api_key()


def _llm_provider(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    raw = str(os.getenv("NEWS_LLM_PROVIDER") or llm_cfg.get("provider") or "").strip().lower()
    if raw in {"openai", "codex", "responses"} or raw in _LEGACY_PROVIDER_ALIASES:
        return "openai"
    return "openai"


def _openai_endpoint_targets(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    llm_cfg = cfg.get("llm") or {}
    return openai_endpoint_targets(
        primary_base_url=(
            _normalize_openai_base_urls(
                _first_non_empty(
                    _runtime_setting("OPENAI_BASE_URL"),
                    llm_cfg.get("base_url"),
                )
            )
            or DEFAULT_OPENAI_BASE_URL
        ),
        backup_base_urls=_merge_csv_values(
            _runtime_setting("OPENAI_BACKUP_BASE_URL"),
            llm_cfg.get("backup_base_url"),
        ),
        primary_api_key=_openai_primary_api_key(),
        backup_api_key=_openai_backup_api_key(),
        primary_model=_openai_model(cfg),
        backup_model=(
            str(
                _first_non_empty(
                    _runtime_setting("OPENAI_BACKUP_MODEL"),
                    llm_cfg.get("backup_model"),
                )
                or ""
            ).strip()
        ),
    )


def _openai_base_url(cfg: Dict[str, Any]) -> str:
    targets = _openai_endpoint_targets(cfg)
    for target in targets:
        base_url = str(target.get("base_url") or "").rstrip("/")
        if base_url:
            return base_url
    return DEFAULT_OPENAI_BASE_URL.rstrip("/")


def _openai_model(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    return _normalize_openai_model(
        _first_non_empty(
            _runtime_setting("OPENAI_MODEL"),
            llm_cfg.get("model"),
        )
    )


def _llm_api_key(cfg: Dict[str, Any]) -> str:
    return _openai_api_key()


def _llm_base_url(cfg: Dict[str, Any]) -> str:
    return _openai_base_url(cfg)


def _llm_model(cfg: Dict[str, Any]) -> str:
    return _openai_model(cfg)


def _llm_summary_source(cfg: Dict[str, Any]) -> str:
    return "openai_responses"


def _summarize_item_cap(llm_cfg: Dict[str, Any], total_titles: int) -> int:
    raw_limit = llm_cfg.get("summarize_max_llm_items")
    if raw_limit is None:
        raw_limit = llm_cfg.get("summarize_max_glm_items")
    max_llm_items = int(raw_limit or max(12, total_titles))
    return max(0, min(120, max_llm_items))


def _openai_post_with_failover(
    *,
    cfg: Dict[str, Any],
    payload: Optional[Dict[str, Any]] = None,
    payload_variants: Optional[List[Dict[str, Any]]] = None,
    chat_fallback_payload: Optional[Dict[str, Any]] = None,
    timeout_sec: int,
    log_prefix: str,
) -> Dict[str, Any]:
    targets = prioritize_openai_targets(
        _openai_endpoint_targets(cfg),
        scope=_OPENAI_FAILOVER_SCOPE,
    )
    available = [
        dict(target)
        for target in targets
        if str(target.get("base_url") or "").strip() and str(target.get("api_key") or "").strip()
    ]
    if not available:
        raise RuntimeError("OPENAI_API_KEY is missing")

    request_payload_variants = [dict(item) for item in (payload_variants or ([] if payload is None else [payload]))]
    if not request_payload_variants:
        raise RuntimeError("news llm payload is missing")

    last_exc: Optional[BaseException] = None
    total_targets = len(available)
    for idx, target in enumerate(available):
        base_url = str(target.get("base_url") or "").rstrip("/")
        api_key = str(target.get("api_key") or "").strip()
        target_model = str(target.get("model") or "").strip()
        transport = target_transport(target)
        headers = build_target_headers({**dict(target), "api_key": api_key})
        request_chat_payload = None
        if chat_fallback_payload is not None:
            request_chat_payload = dict(chat_fallback_payload)
            if target_model:
                request_chat_payload["model"] = target_model
        try:
            advance_to_next_target = False
            if transport == "anthropic":
                if not request_chat_payload or not isinstance(request_chat_payload.get("messages"), list):
                    raise RuntimeError("news llm anthropic backup requires chat_fallback_payload messages")
                request_anthropic_payload = build_anthropic_messages_payload(
                    model=target_model or str(request_chat_payload.get("model") or ""),
                    messages=request_chat_payload.get("messages") or [],
                    max_tokens=int(request_chat_payload.get("max_tokens") or 0) or None,
                    temperature=request_chat_payload.get("temperature"),
                )
                response = requests.post(
                    anthropic_messages_endpoint(base_url),
                    headers=headers,
                    json=request_anthropic_payload,
                    timeout=timeout_sec,
                )
                if response.status_code >= 400:
                    err = RuntimeError(f"LLM anthropic HTTP {response.status_code}: {response.text[:300]}")
                    if should_failover_openai_status(response.status_code):
                        remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    if idx + 1 < total_targets and should_failover_openai_status(response.status_code):
                        last_exc = err
                        logger.warning(
                            f"{log_prefix}: anthropic-style backup HTTP {response.status_code}; "
                            f"trying backup {idx + 2}/{total_targets}"
                        )
                        continue
                    raise err
                data = read_requests_responses_json(response)
                if not extract_response_text(data):
                    err = RuntimeError("LLM anthropic response missing content")
                    remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    if idx + 1 < total_targets:
                        last_exc = err
                        logger.warning(
                            f"{log_prefix}: anthropic-style backup returned empty content; "
                            f"trying backup {idx + 2}/{total_targets}"
                        )
                        continue
                    raise err
                remember_openai_target_success(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                return data
            if request_chat_payload and should_prefer_openai_target_chat_completions(
                targets,
                base_url,
                scope=_OPENAI_FAILOVER_SCOPE,
            ):
                chat_url = chat_completions_endpoint(base_url)
                chat_response = requests.post(
                    chat_url,
                    headers=headers,
                    json=request_chat_payload,
                    timeout=timeout_sec,
                )
                if chat_response.status_code >= 400:
                    err = RuntimeError(f"LLM chat HTTP {chat_response.status_code}: {chat_response.text[:300]}")
                    if should_failover_openai_status(chat_response.status_code):
                        remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    if idx + 1 < total_targets and should_failover_openai_status(chat_response.status_code):
                        last_exc = err
                        logger.warning(
                            f"{log_prefix}: chat-preferred relay HTTP {chat_response.status_code}; "
                            f"trying backup {idx + 2}/{total_targets}"
                        )
                        continue
                    raise err
                chat_data = read_requests_responses_json(chat_response)
                if not extract_response_text(chat_data):
                    err = RuntimeError("LLM chat response missing content")
                    remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    if idx + 1 < total_targets:
                        last_exc = err
                        logger.warning(
                            f"{log_prefix}: chat-preferred relay returned empty content; "
                            f"trying backup {idx + 2}/{total_targets}"
                        )
                        continue
                    raise err
                remember_openai_target_chat_preference(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                remember_openai_target_success(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                return chat_data
            for payload_index, payload_variant in enumerate(request_payload_variants):
                url = responses_endpoint(base_url)
                request_payload = dict(payload_variant)
                if target_model:
                    request_payload["model"] = target_model
                response = requests.post(
                    url,
                    headers=headers,
                    json=request_payload,
                    timeout=timeout_sec,
                )
                if response.status_code >= 400:
                    if request_chat_payload and responses_api_unavailable(response.status_code, response.text):
                        remember_openai_target_chat_preference(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                        chat_url = chat_completions_endpoint(base_url)
                        logger.warning(
                            f"{log_prefix}: relay does not support Responses API; retrying via chat/completions"
                        )
                        chat_response = requests.post(
                            chat_url,
                            headers=headers,
                            json=request_chat_payload,
                            timeout=timeout_sec,
                        )
                        if chat_response.status_code >= 400:
                            err = RuntimeError(f"LLM chat HTTP {chat_response.status_code}: {chat_response.text[:300]}")
                            if should_failover_openai_status(chat_response.status_code):
                                remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                            if idx + 1 < total_targets and should_failover_openai_status(chat_response.status_code):
                                last_exc = err
                                logger.warning(
                                    f"{log_prefix}: openai chat/completions HTTP {chat_response.status_code}; "
                                    f"trying backup {idx + 2}/{total_targets}"
                                )
                                advance_to_next_target = True
                                break
                            raise err
                        remember_openai_target_chat_preference(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                        remember_openai_target_success(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                        return read_requests_responses_json(chat_response)
                    err = RuntimeError(f"LLM HTTP {response.status_code}: {response.text[:300]}")
                    unsupported_param = unsupported_responses_parameter(response.text)
                    if response.status_code == 400 and unsupported_param in {
                        "max_output_tokens",
                        "max_completion_tokens",
                        "max_tokens",
                    }:
                        if payload_index + 1 < len(request_payload_variants):
                            last_exc = err
                            logger.warning(
                                f"{log_prefix}: relay rejected token parameter {unsupported_param}; "
                                f"retrying payload variant {payload_index + 2}/{len(request_payload_variants)}"
                            )
                            continue
                        if idx + 1 < total_targets:
                            last_exc = err
                            remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                            logger.warning(
                                f"{log_prefix}: relay rejected all token parameter variants; "
                                f"trying backup {idx + 2}/{total_targets}"
                            )
                            advance_to_next_target = True
                            break
                        raise err
                    if should_failover_openai_status(response.status_code):
                        remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    if idx + 1 < total_targets and should_failover_openai_status(response.status_code):
                        last_exc = err
                        logger.warning(
                            f"{log_prefix}: openai relay HTTP {response.status_code}; "
                            f"trying backup {idx + 2}/{total_targets}"
                        )
                        advance_to_next_target = True
                        break
                    raise err
                data = read_requests_responses_json(response)
                if request_chat_payload and not extract_response_text(data):
                    chat_url = chat_completions_endpoint(base_url)
                    logger.warning(
                        f"{log_prefix}: responses relay returned empty content; retrying via chat/completions"
                    )
                    chat_response = requests.post(
                        chat_url,
                        headers=build_openai_headers(api_key),
                        json=request_chat_payload,
                        timeout=timeout_sec,
                    )
                    if chat_response.status_code >= 400:
                        err = RuntimeError(f"LLM chat HTTP {chat_response.status_code}: {chat_response.text[:300]}")
                        if should_failover_openai_status(chat_response.status_code):
                            remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                        if idx + 1 < total_targets and should_failover_openai_status(chat_response.status_code):
                            last_exc = err
                            logger.warning(
                                f"{log_prefix}: empty responses body and chat/completions HTTP "
                                f"{chat_response.status_code}; trying backup {idx + 2}/{total_targets}"
                            )
                            advance_to_next_target = True
                            break
                        raise err
                    chat_data = read_requests_responses_json(chat_response)
                    if not extract_response_text(chat_data):
                        err = RuntimeError("LLM chat response missing content")
                        remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                        if idx + 1 < total_targets:
                            last_exc = err
                            logger.warning(
                                f"{log_prefix}: chat/completions also returned empty content; "
                                f"trying backup {idx + 2}/{total_targets}"
                            )
                            advance_to_next_target = True
                            break
                        raise err
                    remember_openai_target_chat_preference(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    remember_openai_target_success(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                    return chat_data
                clear_openai_target_chat_preference(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                remember_openai_target_success(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
                return data
            if advance_to_next_target:
                continue
        except requests.RequestException as exc:
            remember_openai_target_failure(targets, base_url, scope=_OPENAI_FAILOVER_SCOPE)
            if idx + 1 < total_targets:
                last_exc = exc
                logger.warning(
                    f"{log_prefix}: openai relay transport failure; "
                    f"trying backup {idx + 2}/{total_targets}: {exc}"
                )
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("OPENAI_BASE_URL is missing")


def _thinking_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    llm_cfg = cfg.get("llm") or {}
    raw = llm_cfg.get("thinking")
    if isinstance(raw, dict) and raw.get("type") in {"enabled", "disabled"}:
        return {"type": str(raw.get("type"))}
    disable = llm_cfg.get("disable_thinking")
    if disable is None:
        disable = True
    return {"type": "disabled" if bool(disable) else "enabled"}


def _batched(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    step = max(1, size)
    for i in range(0, len(items), step):
        yield items[i : i + step]


def _safe_json_loads(text: str) -> Any:
    return json.loads(text)


def _extract_json_block(text: str) -> Any:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("empty LLM response")

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
        raw = re.sub(r"```$", "", raw).strip()

    try:
        return _safe_json_loads(raw)
    except Exception:
        pass

    candidates: List[str] = []
    left_bracket = raw.find("[")
    right_bracket = raw.rfind("]")
    if left_bracket >= 0 and right_bracket > left_bracket:
        candidates.append(raw[left_bracket : right_bracket + 1])

    left_brace = raw.find("{")
    right_brace = raw.rfind("}")
    if left_brace >= 0 and right_brace > left_brace:
        candidates.append(raw[left_brace : right_brace + 1])

    for candidate in candidates:
        try:
            return _safe_json_loads(candidate)
        except Exception:
            continue

    raise ValueError("cannot parse JSON from LLM response")


def _normalize_llm_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks: List[str] = []
        for item in content:
            if isinstance(item, dict):
                if "text" in item:
                    chunks.append(str(item["text"]))
                elif "content" in item:
                    chunks.append(str(item["content"]))
            else:
                chunks.append(str(item))
        return "\n".join(chunks)
    return str(content)


def _hash_event_fallback(item: Dict[str, Any]) -> str:
    seed = f"{item.get('symbol')}|{item.get('event_type')}|{item.get('ts')}|{item.get('evidence', {}).get('url', '')}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _semantic_event_key(event: Dict[str, Any]) -> str:
    evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
    title = str(evidence.get("title") or "").strip().lower()
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"[^\w\u4e00-\u9fff ]+", "", title)
    url = str(evidence.get("url") or "").strip().lower()
    anchor = title or url or "no_anchor"
    ts = str(event.get("ts") or "")
    bucket = ts[:19]
    seed = (
        f"{str(event.get('symbol') or '').upper()}|"
        f"{str(event.get('event_type') or '').lower()}|"
        f"{int(event.get('sentiment') or 0)}|"
        f"{bucket}|{anchor}"
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _summary_cache_key(title: str, max_length: int) -> str:
    seed = f"{str(title or '').strip()}|{int(max_length)}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _summary_cache_get(title: str, max_length: int) -> Optional[Dict[str, Any]]:
    return _SUMMARY_CACHE.get(_summary_cache_key(title, max_length))


def _summary_cache_set(title: str, max_length: int, result: Dict[str, Any]) -> None:
    if len(_SUMMARY_CACHE) >= _SUMMARY_CACHE_MAX:
        try:
            _SUMMARY_CACHE.pop(next(iter(_SUMMARY_CACHE)))
        except Exception:
            _SUMMARY_CACHE.clear()
    _SUMMARY_CACHE[_summary_cache_key(title, max_length)] = {
        "summary": str(result.get("summary") or ""),
        "sentiment": str(result.get("sentiment") or "neutral"),
        "source": str(result.get("source") or "unknown"),
    }


def _fallback_cn_summary_text(title: str, max_length: int) -> str:
    text = str(title or "").strip()
    if not text:
        return ""
    repl = {
        "bitcoin": "比特币",
        "ethereum": "以太坊",
        "solana": "索拉纳",
        "binance": "币安",
        "okx": "OKX",
        "gate": "Gate",
        "etf": "ETF",
        "sec": "美国证监会",
        "fed": "美联储",
        "regulation": "监管",
        "regulatory": "监管",
        "hack": "被黑",
        "exploit": "漏洞攻击",
        "listing": "上架",
        "delist": "下架",
        "liquidation": "清算",
        "inflow": "资金流入",
        "outflow": "资金流出",
        "whale": "巨鲸",
        "surge": "大涨",
        "rally": "上涨",
        "drop": "下跌",
        "falls": "下跌",
        "rise": "上涨",
        "macro": "宏观",
        "market": "市场",
        "crypto": "加密",
    }
    out = text
    for k, v in repl.items():
        out = re.sub(rf"(?i)\b{re.escape(k)}\b", v, out)
    if out == text:
        out = f"快讯：{text}"
    if len(out) > max_length + 10:
        out = out[: max_length + 10]
    return out


def _heuristic_sentiment_from_title(title: str) -> str:
    text = str(title or "").strip().lower()
    if not text:
        return "neutral"
    pos_score = 0
    neg_score = 0
    for kw in _POS_SENTIMENT_HINTS:
        if kw in text:
            pos_score += 2 if len(kw) >= 6 else 1
    for kw in _NEG_SENTIMENT_HINTS:
        if kw in text:
            neg_score += 2 if len(kw) >= 6 else 1
    if "etf" in text and any(x in text for x in ("approved", "approval", "inflow")):
        pos_score += 2
    if ("fed" in text or "美联储" in text) and any(x in text for x in ("rate hike", "hawkish", "higher for longer", "加息", "鹰派")):
        neg_score += 2
    if ("fed" in text or "美联储" in text) and any(x in text for x in ("rate cut", "dovish", "降息", "鸽派")):
        pos_score += 2
    if pos_score == 0 and neg_score == 0:
        return "neutral"
    if pos_score > neg_score:
        return "positive"
    if neg_score > pos_score:
        return "negative"
    return "neutral"


def _validate_events(payload: Any, mapper: SymbolMapper) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("events"), list):
            payload = payload["events"]
        else:
            raise ValueError("LLM output must be a JSON array or {events:[...]}")

    if not isinstance(payload, list):
        raise ValueError("LLM output is not a JSON array")

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("array elements must be objects")

        item = dict(raw)
        symbol = mapper.normalize_symbol(item.get("symbol")) or str(item.get("symbol") or "").upper()
        if not symbol:
            raise ValueError(f"invalid symbol: {item.get('symbol')!r}")
        item["symbol"] = symbol

        event_type = str(item.get("event_type") or "other").strip().lower()
        item["event_type"] = event_type
        if event_type not in EVENT_TYPES:
            raise ValueError(f"invalid event_type: {event_type}")

        if not item.get("event_id"):
            item["event_id"] = _hash_event_fallback(item)

        evidence = item.get("evidence")
        if not isinstance(evidence, dict):
            raise ValueError("evidence must be an object")
        item["evidence"] = {
            "title": str(evidence.get("title") or ""),
            "url": str(evidence.get("url") or ""),
            "source": str(evidence.get("source") or ""),
            "matched_reason": str(evidence.get("matched_reason") or "llm_event"),
        }

        validated = EventSchema.model_validate(item).model_dump(mode="json")
        validated["model_source"] = "llm"

        event_id = validated["event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        out.append(validated)

    return out


def _build_prompt(batch: List[Dict[str, Any]], allowed_symbols: List[str], feedback: str = "") -> Tuple[str, str]:
    schema_hint = {
        "event_id": "string",
        "ts": "ISO8601",
        "symbol": "one of allowed symbols",
        "event_type": sorted(EVENT_TYPES),
        "sentiment": [-1, 0, 1],
        "impact_score": "float in [0,1]",
        "half_life_min": "int in [30,1440]",
        "evidence": {"title": "string", "url": "string", "source": "string", "matched_reason": "string"},
    }

    compact_news = []
    for idx, item in enumerate(batch, start=1):
        compact_news.append(
            {
                "id": f"n{idx}",
                "title": str(item.get("title") or "")[:300],
                "content": str(item.get("content") or item.get("summary") or "")[:800],
                "url": str(item.get("url") or ""),
                "source": str(item.get("source") or "gdelt"),
                "published_at": str(item.get("published_at") or item.get("published") or item.get("seendate") or ""),
            }
        )

    system_prompt = (
        "You are an event extractor for crypto market news. "
        "Return ONLY strict JSON. Never return explanations, markdown, or trading instructions."
    )

    user_prompt = {
        "task": "Extract market-moving events from the given news and return a JSON array only.",
        "strict_requirements": [
            "Output MUST be a JSON array.",
            "Each object MUST follow schema exactly.",
            "Do not include any keys outside schema.",
            "If no valid event, return [].",
        ],
        "schema": schema_hint,
        "allowed_symbols": allowed_symbols[:120],
        "news": compact_news,
    }

    if feedback:
        user_prompt["fix_feedback"] = feedback

    return system_prompt, json.dumps(user_prompt, ensure_ascii=False)


def _call_llm_once(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    mapper: SymbolMapper,
    feedback: str = "",
) -> List[Dict[str, Any]]:
    api_key = _llm_api_key(cfg)
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    llm_cfg = cfg.get("llm") or {}
    model = _llm_model(cfg)
    timeout_sec = int(llm_cfg.get("timeout_sec") or 45)

    allowed_symbols = sorted({mapper.normalize_symbol(k) for k in (cfg.get("symbols") or {}).keys()} - {""})
    if not allowed_symbols:
        allowed_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

    system_prompt, user_prompt = _build_prompt(batch, allowed_symbols, feedback)

    payload = build_responses_payload(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        stream=False,
    )
    chat_payload = build_chat_completions_payload(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        stream=False,
    )
    data = _openai_post_with_failover(
        cfg=cfg,
        payload=payload,
        chat_fallback_payload=chat_payload,
        timeout_sec=timeout_sec,
        log_prefix="news_llm.extract",
    )
    data = coerce_responses_to_chat_completions(data)
    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices:
        raise ValueError("LLM response missing choices")

    message = choices[0].get("message") or {}
    content = _normalize_llm_content(message.get("content"))

    parsed = _extract_json_block(content)
    return _validate_events(parsed, mapper)


def extract_events_llm_with_meta(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool, List[str]]:
    """Extract events with LLM; retry once then fallback to rules per batch."""
    if not news_items:
        return [], False, []

    mapper: SymbolMapper = cfg.get("_symbol_mapper") or SymbolMapper({"symbols": cfg.get("symbols") or {}})
    batch_size = int((cfg.get("llm") or {}).get("batch_size") or 8)
    batch_size = max(1, min(20, batch_size))

    all_events: List[Dict[str, Any]] = []
    errors: List[str] = []
    llm_used = False

    api_key = _llm_api_key(cfg)
    if not api_key:
        fallback_events = extract_events_rules(news_items, cfg)
        return fallback_events, False, ["LLM API key is missing; fallback to rules"]

    for idx, batch in enumerate(_batched(news_items, batch_size), start=1):
        try:
            events = _call_llm_once(batch=batch, cfg=cfg, mapper=mapper, feedback="")
            llm_used = True
            # LLM success (including empty events) is authoritative.
            # Rules fallback is only for LLM failures.
            all_events.extend(events)
            continue
        except Exception as first_exc:
            feedback = f"Validation or parsing error: {first_exc}"

        try:
            events = _call_llm_once(batch=batch, cfg=cfg, mapper=mapper, feedback=feedback)
            llm_used = True
            all_events.extend(events)
            continue
        except Exception as second_exc:
            err_msg = f"batch={idx} llm failed after retry: {second_exc}"
            logger.warning(err_msg)
            errors.append(err_msg)
            fallback = extract_events_rules(batch, cfg)
            all_events.extend(fallback)

    # Final de-dup by event_id and semantic identity so duplicated headlines from
    # different aggregator URLs do not inflate structured event counts.
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    seen_semantic: set[str] = set()
    for event in all_events:
        event_id = str(event.get("event_id") or "")
        semantic_key = _semantic_event_key(event)
        if not event_id or event_id in seen or semantic_key in seen_semantic:
            continue
        seen.add(event_id)
        seen_semantic.add(semantic_key)
        deduped.append(event)

    return deduped, llm_used, errors


def extract_events_llm(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Required API: return event list only."""
    events, _, _ = extract_events_llm_with_meta(news_items, cfg)
    return events


def summarize_title_llm(title: str, cfg: Dict[str, Any], max_length: int = 60) -> Dict[str, Any]:
    """Summarize a news title to a single line using the configured LLM.

    Args:
        title: The original news title to summarize
        cfg: Configuration dictionary (unused but kept for consistency)
        max_length: Maximum length of the summary (default 60 characters)

    Returns:
        Dict with keys: summary (str), sentiment (str: positive/negative/neutral)
    """
    default_result = {"summary": title, "sentiment": "neutral", "source": "default"}

    if not title or not str(title).strip():
        return default_result

    title = str(title).strip()
    cached = _summary_cache_get(title, max_length)
    if cached:
        return cached

    api_key = _llm_api_key(cfg)
    if not api_key:
        result = _summarize_fallback(title, max_length)
        _summary_cache_set(title, max_length, result)
        return result

    llm_cfg = cfg.get("llm") or {}
    model = _llm_model(cfg)
    summary_source = _llm_summary_source(cfg)
    timeout_sec = int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 12)

    system_prompt = (
        "你是一个加密货币新闻标题分析助手。"
        "请将新闻标题翻译成中文并精简为一行。"
        "同时判断该新闻对市场的影响：利好(positive)、利空(negative)或中性(neutral)。"
        "必须严格返回JSON格式：{\"summary\":\"中文摘要\",\"sentiment\":\"positive或negative或neutral\"}"
    )

    user_prompt = f"请分析这条加密货币新闻标题，翻译成中文并判断利好利空：\n\n{title}"

    payload_variants = build_responses_payload_variants(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=max_length + 50,
        temperature=None,
        text_format=None,
        stream=False,
    )
    chat_payload = build_chat_completions_payload(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=max_length + 50,
        response_format={"type": "json_object"},
        stream=False,
    )

    try:
        data = _openai_post_with_failover(
            cfg=cfg,
            payload_variants=payload_variants,
            chat_fallback_payload=chat_payload,
            timeout_sec=timeout_sec,
            log_prefix="news_llm.summarize",
        )
        data = coerce_responses_to_chat_completions(data)
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            result = _summarize_fallback(title, max_length)
            _summary_cache_set(title, max_length, result)
            return result

        message = choices[0].get("message") or {}
        content = _normalize_llm_content(message.get("content")).strip()

        # Parse JSON response
        parsed = _extract_json_block(content)
        if isinstance(parsed, dict):
            summary = str(parsed.get("summary") or title)[:max_length + 10]
            sentiment = str(parsed.get("sentiment") or "neutral").lower()
            if sentiment not in ("positive", "negative", "neutral"):
                sentiment = "neutral"
            result = {"summary": summary, "sentiment": sentiment, "source": summary_source}
            _summary_cache_set(title, max_length, result)
            return result

        result = _summarize_fallback(title, max_length)
        _summary_cache_set(title, max_length, result)
        return result

    except Exception as e:
        logger.warning(f"LLM summarize error: {e}")
        result = _summarize_fallback(title, max_length)
        _summary_cache_set(title, max_length, result)
        return result


def _summarize_fallback(title: str, max_length: int) -> Dict[str, Any]:
    text = _fallback_cn_summary_text(title, max_length)
    if not text:
        return {"summary": "", "sentiment": "neutral", "source": "fallback_rule"}
    return {
        "summary": text,
        "sentiment": _heuristic_sentiment_from_title(title),
        "source": "fallback_rule",
    }


def _call_llm_batch_summarize(
    titles: List[str],
    cfg: Dict[str, Any],
    max_length: int = 60,
) -> List[Dict[str, Any]]:
    api_key = _llm_api_key(cfg)
    if not api_key:
        return [_summarize_fallback(t, max_length) for t in titles]

    llm_cfg = cfg.get("llm") or {}
    model = _llm_model(cfg)
    summary_source = _llm_summary_source(cfg)
    timeout_sec = int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 12)

    compact = [{"idx": i, "title": str(t or "")[:300]} for i, t in enumerate(titles)]
    system_prompt = (
        "You summarize crypto news headlines."
        " For each title, return one concise Simplified Chinese summary and a sentiment label."
        " Return strict json only."
    )
    user_prompt = {
        "task": "Return one Simplified Chinese headline summary and sentiment for each title in strict json.",
        "output_schema": {
            "items": [
                {"idx": 0, "summary": "中文摘要", "sentiment": "positive|negative|neutral"}
            ]
        },
        "max_summary_length": int(max_length),
        "titles": compact,
    }

    payload_variants = build_responses_payload_variants(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ],
        temperature=None,
        text_format=None,
        stream=False,
    )
    chat_payload = build_chat_completions_payload(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
        stream=False,
    )

    try:
        data = _openai_post_with_failover(
            cfg=cfg,
            payload_variants=payload_variants,
            chat_fallback_payload=chat_payload,
            timeout_sec=timeout_sec,
            log_prefix="news_llm.batch_summarize",
        )
        data = coerce_responses_to_chat_completions(data)
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            raise ValueError("LLM summarize batch missing choices")
        message = choices[0].get("message") or {}
        content = _normalize_llm_content(message.get("content")).strip()
        parsed = _extract_json_block(content)
        if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
            parsed_items = parsed.get("items")
        elif isinstance(parsed, list):
            parsed_items = parsed
        else:
            raise ValueError("invalid summarize batch output")

        out: List[Dict[str, Any]] = [_summarize_fallback(t, max_length) for t in titles]
        for item in parsed_items:
            if not isinstance(item, dict):
                continue
            try:
                idx = int(item.get("idx"))
            except Exception:
                continue
            if idx < 0 or idx >= len(out):
                continue
            summary = str(item.get("summary") or "").strip()
            if not summary:
                continue
            sentiment = str(item.get("sentiment") or "neutral").strip().lower()
            if sentiment not in {"positive", "negative", "neutral"}:
                sentiment = "neutral"
            if len(summary) > max_length + 10:
                summary = summary[: max_length + 10]
            out[idx] = {"summary": summary, "sentiment": sentiment, "source": summary_source}
        return out
    except Exception as e:
        logger.warning(f"LLM summarize batch error: {e}")
        return [_summarize_fallback(t, max_length) for t in titles]


def batch_summarize_titles(titles: List[str], cfg: Dict[str, Any], max_length: int = 60) -> List[Dict[str, Any]]:
    """Batch summarize multiple titles.

    Args:
        titles: List of titles to summarize
        cfg: Configuration dictionary
        max_length: Maximum length of each summary

    Returns:
        List of dicts with summary and sentiment (same order as input)
    """
    if not titles:
        return []

    llm_cfg = cfg.get("llm") or {}
    batch_size = int(llm_cfg.get("summarize_batch_size") or 20)
    batch_size = max(1, min(40, batch_size))
    max_llm_items = _summarize_item_cap(llm_cfg, len(titles))

    results: List[Optional[Dict[str, Any]]] = [None] * len(titles)
    uncached_idx: List[int] = []
    for idx, title in enumerate(titles):
        cached = _summary_cache_get(title, max_length)
        if cached:
            results[idx] = cached
        else:
            uncached_idx.append(idx)

    llm_targets = uncached_idx[:max_llm_items]
    fallback_targets = uncached_idx[max_llm_items:]
    for idx in fallback_targets:
        fallback = _summarize_fallback(titles[idx], max_length)
        results[idx] = fallback
        _summary_cache_set(titles[idx], max_length, fallback)

    for i in range(0, len(llm_targets), batch_size):
        chunk_idx = llm_targets[i : i + batch_size]
        chunk_titles = [titles[idx] for idx in chunk_idx]
        chunk_res = _call_llm_batch_summarize(chunk_titles, cfg, max_length)
        for idx, res in zip(chunk_idx, chunk_res):
            results[idx] = res
            _summary_cache_set(titles[idx], max_length, res)

    final: List[Dict[str, Any]] = []
    for idx, item in enumerate(results):
        if item is None:
            item = _summarize_fallback(titles[idx], max_length)
            _summary_cache_set(titles[idx], max_length, item)
        final.append(item)
    return final


def extract_events_glm5_with_meta(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool, List[str]]:
    return extract_events_llm_with_meta(news_items, cfg)


def extract_events_glm5(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    return extract_events_llm(news_items, cfg)


def summarize_title_glm5(title: str, cfg: Dict[str, Any], max_length: int = 60) -> Dict[str, Any]:
    return summarize_title_llm(title, cfg, max_length)


def batch_summarize_titles_llm(titles: List[str], cfg: Dict[str, Any], max_length: int = 60) -> List[Dict[str, Any]]:
    return batch_summarize_titles(titles, cfg, max_length)
