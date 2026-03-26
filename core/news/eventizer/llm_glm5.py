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
    build_openai_headers,
    build_responses_payload,
    coerce_responses_to_chat_completions,
    read_requests_responses_json,
    responses_endpoint,
)


DEFAULT_OPENAI_BASE_URL = "https://vpsairobot.com/v1"
DEFAULT_OPENAI_MODEL = "gpt-5.1-codex-mini"
DEFAULT_ZHIPU_BASE_URL = "https://open.bigmodel.cn/api/coding/paas/v4"
DEFAULT_ZHIPU_MODEL = "GLM-4.5-Air"
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


def _zhipu_api_key() -> str:
    return str(os.getenv("ZHIPU_API_KEY") or getattr(settings, "ZHIPU_API_KEY", "") or "").strip()


def _openai_api_key() -> str:
    return str(os.getenv("OPENAI_API_KEY") or getattr(settings, "OPENAI_API_KEY", "") or "").strip()


def _llm_provider(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    raw = str(os.getenv("NEWS_LLM_PROVIDER") or llm_cfg.get("provider") or "").strip().lower()
    if raw in {"openai", "codex", "responses"}:
        return "openai"
    if raw == "glm":
        return "glm"
    if _openai_api_key():
        return "openai"
    return "glm"


def _zhipu_base_url(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    return str(
        os.getenv("ZHIPU_BASE_URL")
        or llm_cfg.get("base_url")
        or getattr(settings, "ZHIPU_BASE_URL", "")
        or DEFAULT_ZHIPU_BASE_URL
    ).rstrip("/")


def _zhipu_model(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    return str(
        os.getenv("ZHIPU_MODEL")
        or llm_cfg.get("model")
        or getattr(settings, "ZHIPU_MODEL", "")
        or DEFAULT_ZHIPU_MODEL
    )


def _openai_base_url(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    return str(
        os.getenv("OPENAI_BASE_URL")
        or llm_cfg.get("base_url")
        or getattr(settings, "OPENAI_BASE_URL", "")
        or DEFAULT_OPENAI_BASE_URL
    ).rstrip("/")


def _openai_model(cfg: Dict[str, Any]) -> str:
    llm_cfg = cfg.get("llm") or {}
    return str(
        os.getenv("OPENAI_MODEL")
        or llm_cfg.get("model")
        or getattr(settings, "OPENAI_MODEL", "")
        or DEFAULT_OPENAI_MODEL
    )


def _llm_api_key(cfg: Dict[str, Any]) -> str:
    if _llm_provider(cfg) == "openai":
        return _openai_api_key()
    return _zhipu_api_key()


def _llm_base_url(cfg: Dict[str, Any]) -> str:
    if _llm_provider(cfg) == "openai":
        return _openai_base_url(cfg)
    return _zhipu_base_url(cfg)


def _llm_model(cfg: Dict[str, Any]) -> str:
    if _llm_provider(cfg) == "openai":
        return _openai_model(cfg)
    return _zhipu_model(cfg)


def _llm_summary_source(cfg: Dict[str, Any]) -> str:
    return "openai_responses" if _llm_provider(cfg) == "openai" else "glm5"


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


def _call_glm5_once(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    mapper: SymbolMapper,
    feedback: str = "",
) -> List[Dict[str, Any]]:
    provider = _llm_provider(cfg)
    api_key = _llm_api_key(cfg)
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing" if provider == "openai" else "ZHIPU_API_KEY is missing")

    llm_cfg = cfg.get("llm") or {}
    base_url = _llm_base_url(cfg)
    model = _llm_model(cfg)
    timeout_sec = int(llm_cfg.get("timeout_sec") or 45)

    allowed_symbols = sorted({mapper.normalize_symbol(k) for k in (cfg.get("symbols") or {}).keys()} - {""})
    if not allowed_symbols:
        allowed_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

    system_prompt, user_prompt = _build_prompt(batch, allowed_symbols, feedback)

    if provider == "openai":
        payload = build_responses_payload(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            stream=False,
        )
        url = responses_endpoint(base_url)
    else:
        payload = {
            "model": model,
            "temperature": 0,
            "top_p": 0.1,
            "response_format": {"type": "json_object"},
            "thinking": _thinking_cfg(cfg),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        url = f"{base_url}/chat/completions"

    headers = build_openai_headers(api_key)
    response = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
    if response.status_code >= 400:
        raise RuntimeError(f"LLM HTTP {response.status_code}: {response.text[:300]}")

    data = read_requests_responses_json(response) if provider == "openai" else response.json()
    if provider == "openai":
        data = coerce_responses_to_chat_completions(data)
    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices:
        raise ValueError("LLM response missing choices")

    message = choices[0].get("message") or {}
    content = _normalize_llm_content(message.get("content"))

    parsed = _extract_json_block(content)
    return _validate_events(parsed, mapper)


def extract_events_glm5_with_meta(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool, List[str]]:
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
            events = _call_glm5_once(batch=batch, cfg=cfg, mapper=mapper, feedback="")
            llm_used = True
            # LLM success (including empty events) is authoritative.
            # Rules fallback is only for LLM failures.
            all_events.extend(events)
            continue
        except Exception as first_exc:
            feedback = f"Validation or parsing error: {first_exc}"

        try:
            events = _call_glm5_once(batch=batch, cfg=cfg, mapper=mapper, feedback=feedback)
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


def extract_events_glm5(news_items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Required API: return event list only."""
    events, _, _ = extract_events_glm5_with_meta(news_items, cfg)
    return events


def summarize_title_glm5(title: str, cfg: Dict[str, Any], max_length: int = 60) -> Dict[str, Any]:
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

    provider = _llm_provider(cfg)
    api_key = _llm_api_key(cfg)
    if not api_key:
        result = _summarize_fallback(title, max_length)
        _summary_cache_set(title, max_length, result)
        return result

    llm_cfg = cfg.get("llm") or {}
    base_url = _llm_base_url(cfg)
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

    if provider == "openai":
        payload = build_responses_payload(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_output_tokens=max_length + 50,
            temperature=0.3,
            text_format="json_object",
            stream=False,
        )
        url = responses_endpoint(base_url)
    else:
        payload = {
            "model": model,
            "temperature": 0.3,
            "top_p": 0.9,
            "max_tokens": max_length + 50,
            "response_format": {"type": "json_object"},
            "thinking": _thinking_cfg(cfg),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        url = f"{base_url}/chat/completions"

    headers = build_openai_headers(api_key)

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        if response.status_code >= 400:
            logger.warning(f"LLM summarize failed: HTTP {response.status_code}")
            result = _summarize_fallback(title, max_length)
            _summary_cache_set(title, max_length, result)
            return result

        data = read_requests_responses_json(response) if provider == "openai" else response.json()
        if provider == "openai":
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


def _call_glm5_batch_summarize(
    titles: List[str],
    cfg: Dict[str, Any],
    max_length: int = 60,
) -> List[Dict[str, Any]]:
    provider = _llm_provider(cfg)
    api_key = _llm_api_key(cfg)
    if not api_key:
        return [_summarize_fallback(t, max_length) for t in titles]

    llm_cfg = cfg.get("llm") or {}
    base_url = _llm_base_url(cfg)
    model = _llm_model(cfg)
    summary_source = _llm_summary_source(cfg)
    timeout_sec = int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 12)

    compact = [{"idx": i, "title": str(t or "")[:300]} for i, t in enumerate(titles)]
    system_prompt = (
        "你是加密新闻标题处理助手。"
        "将每条标题翻译为简洁中文一行，并标注情绪。"
        "只返回严格 JSON。"
    )
    user_prompt = {
        "task": "逐条输出中文一行摘要和情绪",
        "output_schema": {
            "items": [
                {"idx": 0, "summary": "中文摘要", "sentiment": "positive|negative|neutral"}
            ]
        },
        "max_summary_length": int(max_length),
        "titles": compact,
    }

    if provider == "openai":
        payload = build_responses_payload(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
            ],
            temperature=0.2,
            text_format="json_object",
            stream=False,
        )
        url = responses_endpoint(base_url)
    else:
        payload = {
            "model": model,
            "temperature": 0.2,
            "top_p": 0.8,
            "response_format": {"type": "json_object"},
            "thinking": _thinking_cfg(cfg),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
            ],
        }
        url = f"{base_url}/chat/completions"
    headers = build_openai_headers(api_key)

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        if response.status_code >= 400:
            body_preview = (response.text or "")[:200].replace("\n", " ")
            raise RuntimeError(f"LLM summarize batch HTTP {response.status_code}: {body_preview}")
        data = read_requests_responses_json(response) if provider == "openai" else response.json()
        if provider == "openai":
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
    max_glm_items = int(llm_cfg.get("summarize_max_glm_items") or max(12, len(titles)))
    max_glm_items = max(0, min(120, max_glm_items))

    results: List[Optional[Dict[str, Any]]] = [None] * len(titles)
    uncached_idx: List[int] = []
    for idx, title in enumerate(titles):
        cached = _summary_cache_get(title, max_length)
        if cached:
            results[idx] = cached
        else:
            uncached_idx.append(idx)

    glm_targets = uncached_idx[:max_glm_items]
    fallback_targets = uncached_idx[max_glm_items:]
    for idx in fallback_targets:
        fallback = _summarize_fallback(titles[idx], max_length)
        results[idx] = fallback
        _summary_cache_set(titles[idx], max_length, fallback)

    for i in range(0, len(glm_targets), batch_size):
        chunk_idx = glm_targets[i : i + batch_size]
        chunk_titles = [titles[idx] for idx in chunk_idx]
        chunk_res = _call_glm5_batch_summarize(chunk_titles, cfg, max_length)
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
