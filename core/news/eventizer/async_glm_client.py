"""Async GLM-5 client for event extraction and summarization."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp
from loguru import logger

from config.settings import settings
from core.news.eventizer.rate_limiter import rate_limiter
from core.news.eventizer.rules import SymbolMapper, extract_events_rules
from core.news.storage.models import EVENT_TYPES, EventSchema

DEFAULT_ZHIPU_BASE_URL = "https://open.bigmodel.cn/api/coding/paas/v4"
DEFAULT_ZHIPU_MODEL = "GLM-4.5-Air"

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


def _summarize_fallback(title: str, max_length: int) -> Dict[str, Any]:
    text = _fallback_cn_summary_text(title, max_length)
    if not text:
        return {"summary": "", "sentiment": "neutral", "source": "fallback_rule"}
    return {
        "summary": text,
        "sentiment": _heuristic_sentiment_from_title(title),
        "source": "fallback_rule",
    }


class AsyncGLMClient:
    """Async GLM-5 client with timeout, rate limiting, and error handling."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        """Initialize the async GLM client.

        Args:
            cfg: Configuration dictionary. If None, uses environment defaults.
        """
        self._cfg = cfg or {}
        self._api_key = _zhipu_api_key()
        self._base_url = _zhipu_base_url(self._cfg)
        self._model = _zhipu_model(self._cfg)

        llm_cfg = self._cfg.get("llm") or {}
        self._timeout = aiohttp.ClientTimeout(
            total=int(llm_cfg.get("timeout_sec") or 45),
            connect=int(llm_cfg.get("connect_timeout_sec") or 10),
        )
        self._summarize_timeout = aiohttp.ClientTimeout(
            total=int(llm_cfg.get("summarize_timeout_sec") or 12),
            connect=int(llm_cfg.get("connect_timeout_sec") or 10),
        )

        # Statistics for monitoring
        self._requests_total = 0
        self._requests_success = 0
        self._requests_failed = 0
        self._requests_rate_limited = 0

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers for GLM API."""
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        endpoint: str,
        payload: Dict[str, Any],
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> Tuple[Dict[str, Any], str]:
        """Make an async HTTP request to GLM API with rate limiting.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            payload: Request payload
            timeout: Request timeout override

        Returns:
            Tuple of (response_data, error_type)
            error_type: "none", "rate_limit", "timeout", "other"

        Raises:
            RuntimeError: If API key is missing
        """
        if not self._api_key:
            raise RuntimeError("ZHIPU_API_KEY is missing")

        self._requests_total += 1
        url = f"{self._base_url}{endpoint}"
        timeout = timeout or self._timeout

        # Wait for rate limiter
        await rate_limiter.wait_for_token(timeout=30.0)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    json=payload,
                ) as response:
                    # Handle rate limiting (429)
                    if response.status == 429:
                        self._requests_rate_limited += 1

                        retry_after = None
                        retry_after_header = response.headers.get("Retry-After")
                        if retry_after_header:
                            try:
                                retry_after = int(retry_after_header)
                            except ValueError:
                                pass

                        error_text = await response.text()
                        logger.warning(f"GLM rate limited (429): {error_text[:200]}")
                        rate_limiter.on_rate_limit(retry_after=retry_after)
                        return {}, "rate_limit"

                    if response.status >= 400:
                        self._requests_failed += 1
                        error_text = await response.text()
                        logger.warning(f"GLM API HTTP {response.status}: {error_text[:300]}")

                        if response.status in (408, 504):
                            return {}, "timeout"
                        return {}, "other"

                    data = await response.json()
                    self._requests_success += 1
                    rate_limiter.reset_backoff()
                    return data, "none"
            except asyncio.TimeoutError:
                self._requests_failed += 1
                logger.warning("GLM request timed out")
                return {}, "timeout"
            except aiohttp.ClientError as exc:
                self._requests_failed += 1
                logger.warning(f"GLM client error: {exc!r}")
                return {}, "other"

    async def chat_completions(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0,
        top_p: float = 0.1,
        max_tokens: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> Tuple[Dict[str, Any], str]:
        """Call GLM chat completions API asynchronously with rate limiting.

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature (0-1)
            top_p: Nucleus sampling threshold
            max_tokens: Maximum tokens in response
            timeout: Request timeout in seconds (overrides default)

        Returns:
            Tuple of (response_data, error_type)
            error_type: "none", "rate_limit", "timeout", "other"

        Raises:
            RuntimeError: If API key is missing
        """
        payload = {
            "model": self._model,
            "temperature": temperature,
            "top_p": top_p,
            "response_format": {"type": "json_object"},
            "messages": messages,
        }

        if max_tokens is not None:
            payload["max_tokens"] = max_tokens

        request_timeout = self._timeout if timeout is None else aiohttp.ClientTimeout(total=timeout)

        return await self._request("POST", "/chat/completions", payload, timeout=request_timeout)

    async def chat_completions_stream(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0,
        top_p: float = 0.1,
        max_tokens: Optional[int] = None,
        timeout: Optional[int] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Call GLM chat completions API with streaming response.

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature (0-1)
            top_p: Nucleus sampling threshold
            max_tokens: Maximum tokens in response
            timeout: Request timeout in seconds (overrides default)

        Yields:
            Streaming response chunks from the API

        Raises:
            RuntimeError: If API call fails or times out

        Note:
            Uses SSE (Server-Sent Events) format. Each chunk contains
            partial response data that can be processed incrementally.
        """
        if not self._api_key:
            raise RuntimeError("ZHIPU_API_KEY is missing")

        payload = {
            "model": self._model,
            "temperature": temperature,
            "top_p": top_p,
            "stream": True,
            "messages": messages,
        }

        if max_tokens is not None:
            payload["max_tokens"] = max_tokens

        request_timeout = self._timeout if timeout is None else aiohttp.ClientTimeout(total=timeout)
        url = f"{self._base_url}/chat/completions"

        try:
            async with aiohttp.ClientSession(timeout=request_timeout) as session:
                async with session.post(
                    url=url,
                    headers=self._get_headers(),
                    json=payload,
                ) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise RuntimeError(f"GLM API HTTP {response.status}: {error_text[:300]}")

                    async for line in response.content:
                        line_text = line.decode("utf-8").strip()
                        if not line_text or line_text.startswith(":"):
                            continue
                        if line_text.startswith("data: "):
                            data = line_text[6:].strip()
                            if data == "[DONE]":
                                break
                            try:
                                yield json.loads(data)
                            except json.JSONDecodeError:
                                continue
        except asyncio.CancelledError:
            logger.debug("Stream request cancelled by caller")
            raise
        except Exception as e:
            raise RuntimeError(f"Stream request failed: {e}") from e

    def _build_prompt(
        self,
        batch: List[Dict[str, Any]],
        allowed_symbols: List[str],
        feedback: str = "",
    ) -> tuple[str, str]:
        """Build system and user prompts for event extraction.

        Args:
            batch: List of news items
            allowed_symbols: List of allowed trading symbols
            feedback: Feedback message for retry attempts

        Returns:
            Tuple of (system_prompt, user_prompt_json)
        """
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

    def _hash_event_fallback(self, item: Dict[str, Any]) -> str:
        """Generate a fallback event ID hash."""
        seed = f"{item.get('symbol')}|{item.get('event_type')}|{item.get('ts')}|{item.get('evidence', {}).get('url', '')}"
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]

    def _validate_events(self, payload: Any, mapper: SymbolMapper) -> List[Dict[str, Any]]:
        """Validate and normalize events from LLM response.

        Args:
            payload: Parsed JSON response from LLM
            mapper: Symbol mapper instance

        Returns:
            List of validated event dictionaries
        """
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
                item["event_id"] = self._hash_event_fallback(item)

            evidence = item.get("evidence")
            if not isinstance(evidence, dict):
                raise ValueError("evidence must be an object")
            item["evidence"] = {
                "title": str(evidence.get("title") or ""),
                "url": str(evidence.get("url") or ""),
                "source": str(evidence.get("source") or ""),
                "matched_reason": str(evidence.get("matched_reason") or "glm5_event"),
            }

            validated = EventSchema.model_validate(item).model_dump(mode="json")
            validated["model_source"] = "llm"

            event_id = validated["event_id"]
            if event_id in seen:
                continue
            seen.add(event_id)
            out.append(validated)

        return out

    async def _call_extract_once(
        self,
        batch: List[Dict[str, Any]],
        mapper: SymbolMapper,
        feedback: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Make a single async call for event extraction.

        Args:
            batch: Batch of news items to extract events from
            mapper: Symbol mapper instance
            feedback: Feedback message for retry attempts

        Returns:
            Tuple of (events, error_type)
            error_type: "none", "rate_limit", "timeout", "other"
        """
        allowed_symbols = sorted(
            {mapper.normalize_symbol(k) for k in (self._cfg.get("symbols") or {}).keys()} - {""}
        )
        if not allowed_symbols:
            allowed_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

        system_prompt, user_prompt = self._build_prompt(batch, allowed_symbols, feedback)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        response, error_type = await self.chat_completions(messages, temperature=0, top_p=0.1)

        if error_type != "none" or not isinstance(response, dict):
            return [], error_type

        choices = response.get("choices") if isinstance(response, dict) else None
        if not choices:
            return [], "other"

        message = choices[0].get("message") or {}
        content = _normalize_llm_content(message.get("content"))

        try:
            parsed = _extract_json_block(content)
            return self._validate_events(parsed, mapper), "none"
        except Exception:
            return [], "other"

    async def extract_events(
        self,
        news_batch: List[Dict[str, Any]],
        cfg: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """Extract events from a batch of news items asynchronously.

        Args:
            news_batch: List of news items with title, content, url, etc.
            cfg: Configuration dictionary (overrides instance config)

        Returns:
            Tuple of (events, llm_used, error_type)
            llm_used: Whether LLM was successfully used
            error_type: "none", "rate_limit", "timeout", "other"
        """
        if not news_batch:
            return [], False, "none"

        effective_cfg = cfg or self._cfg
        mapper = effective_cfg.get("_symbol_mapper") or SymbolMapper({"symbols": effective_cfg.get("symbols") or {}})

        llm_cfg = effective_cfg.get("llm") or {}
        batch_size = int(llm_cfg.get("batch_size") or 8)
        batch_size = min(20, max(5, batch_size))

        all_events: List[Dict[str, Any]] = []
        llm_used = False

        if not self._api_key:
            fallback_events = extract_events_rules(news_batch, effective_cfg)
            logger.warning("ZHIPU_API_KEY is missing; fallback to rules")
            return fallback_events, False, "none"

        async def _process_batch(batch: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], str]:
            """Process a single batch and return (events, error_type)."""
            try:
                events, error_type = await self._call_extract_once(batch, mapper, feedback="")
                if error_type == "none":
                    # A valid empty array is still a successful extraction result.
                    # Keep optional rule supplementation, but do not mark the task as failed
                    # just because there were no market-moving events in this batch.
                    llm_events = list(events)
                    rule_events = extract_events_rules(batch, effective_cfg)
                    return llm_events + rule_events, "none"
                elif error_type in ("rate_limit", "timeout"):
                    # Transient error, return empty to trigger backoff
                    return [], error_type
                else:
                    # Other error, fallback to rules
                    return extract_events_rules(batch, effective_cfg), "other"
            except Exception as first_exc:
                try:
                    feedback = f"Validation or parsing error: {first_exc}"
                    events, error_type = await self._call_extract_once(batch, mapper, feedback=feedback)
                    if error_type == "none":
                        llm_events = list(events)
                        rule_events = extract_events_rules(batch, effective_cfg)
                        return llm_events + rule_events, "none"
                    return extract_events_rules(batch, effective_cfg), "other"
                except Exception as second_exc:
                    logger.warning(f"LLM extract failed after retry: {second_exc!r}")
                    return extract_events_rules(batch, effective_cfg), "other"

        last_error_type = "none"
        for i in range(0, len(news_batch), batch_size):
            batch = news_batch[i : i + batch_size]
            batch_events, error_type = await _process_batch(batch)
            all_events.extend(batch_events)
            last_error_type = error_type
            if error_type == "none":
                llm_used = True

        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        seen_semantic: set[str] = set()

        for event in all_events:
            event_id = str(event.get("event_id") or "")
            evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
            title = str(evidence.get("title") or "").strip().lower()
            title = re.sub(r"\s+", " ", title)
            url = str(evidence.get("url") or "").strip().lower()
            anchor = title or url or "no_anchor"
            ts = str(event.get("ts") or "")
            bucket = ts[:19]  # second-level precision to avoid same-minute dedup
            seed = (
                f"{str(event.get('symbol') or '').upper()}|"
                f"{str(event.get('event_type') or '').lower()}|"
                f"{int(event.get('sentiment') or 0)}|"
                f"{bucket}|{anchor}"
            )
            semantic_key = hashlib.sha1(seed.encode("utf-8")).hexdigest()

            if not event_id or event_id in seen or semantic_key in seen_semantic:
                continue
            seen.add(event_id)
            seen_semantic.add(semantic_key)
            deduped.append(event)

        return deduped, llm_used, last_error_type

    async def extract_events_stream(
        self,
        news_stream: AsyncIterator[Dict[str, Any]],
        batch_size: int = 8,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Extract events from a stream of news items.

        Args:
            news_stream: Async iterator of news items
            batch_size: Number of items to process per batch

        Yields:
            Extracted events one at a time
        """
        batch: List[Dict[str, Any]] = []

        async for item in news_stream:
            batch.append(item)
            if len(batch) >= batch_size:
                events, _, _ = await self.extract_events(batch)
                batch.clear()
                for event in events:
                    yield event

        if batch:
            events, _, _ = await self.extract_events(batch)
            for event in events:
                yield event

    async def _call_summarize_batch(
        self,
        titles: List[str],
        max_length: int = 60,
    ) -> List[Dict[str, Any]]:
        """Call GLM API to summarize a batch of titles.

        Args:
            titles: List of titles to summarize
            max_length: Maximum length of each summary

        Returns:
            List of summary results with 'summary', 'sentiment', 'source' keys
        """
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

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
        ]

        try:
            response = await self.chat_completions(
                messages,
                temperature=0.2,
                top_p=0.8,
                timeout=self._summarize_timeout.total,
            )

            choices = response.get("choices") if isinstance(response, dict) else None
            if not choices:
                raise ValueError("GLM5 summarize batch missing choices")

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
                out[idx] = {"summary": summary, "sentiment": sentiment, "source": "glm5"}
            return out

        except Exception as e:
            logger.warning(f"GLM5 summarize batch error: {e}")
            return [_summarize_fallback(t, max_length) for t in titles]

    async def summarize_stream(
        self,
        title: str,
        max_length: int = 60,
    ) -> AsyncIterator[Dict[str, str]]:
        """Summarize a single title with streaming response.

        Args:
            title: The title to summarize
            max_length: Maximum length of the summary

        Yields:
            Dict with 'delta' (incremental text), 'done' (bool), 'summary' (if done)

        Example:
            >>> async for chunk in client.summarize_stream(title):
            ...     print(chunk['delta'], end='')
        """
        system_prompt = (
            "你是一个加密货币新闻标题分析助手。"
            "请将新闻标题翻译成中文并精简为一行。"
            "同时判断该新闻对市场的影响：利好(positive)、利空(negative)或中性(neutral)。"
            f"请控制在{max_length}个字符以内。"
        )

        user_prompt = f"请分析这条加密货币新闻标题，翻译成中文并判断利好利空：\n\n{title}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        accumulated_content = ""

        try:
            async for chunk in self.chat_completions_stream(
                messages,
                temperature=0.3,
                timeout=self._summarize_timeout.total,
            ):
                choices = chunk.get("choices") if isinstance(chunk, dict) else None
                if not choices:
                    continue

                delta = choices[0].get("delta") or {}
                content = delta.get("content", "")

                if content:
                    accumulated_content += content
                    yield {"delta": content, "done": False}

                finish_reason = choices[0].get("finish_reason")
                if finish_reason:
                    yield {
                        "delta": "",
                        "done": True,
                        "summary": accumulated_content.strip()[:max_length + 10],
                    }
                    break

        except Exception as e:
            logger.warning(f"Stream summarize error for '{title[:50]}...': {e}")
            yield {"delta": "", "done": True, "summary": _fallback_cn_summary_text(title, max_length)}

    async def summarize_batch(
        self,
        titles: List[str],
        cfg: Optional[Dict[str, Any]] = None,
        max_length: int = 60,
    ) -> List[Dict[str, Any]]:
        """Summarize a batch of titles asynchronously.

        Args:
            titles: List of titles to summarize
            cfg: Configuration dictionary
            max_length: Maximum length of each summary

        Returns:
            List of dicts with summary and sentiment (same order as input)
        """
        if not titles:
            return []

        effective_cfg = cfg or self._cfg
        llm_cfg = effective_cfg.get("llm") or {}
        batch_size = int(llm_cfg.get("summarize_batch_size") or 20)
        batch_size = max(1, min(40, batch_size))
        max_glm_items = int(llm_cfg.get("summarize_max_glm_items") or max(12, len(titles)))
        max_glm_items = max(0, min(120, max_glm_items))

        results: List[Optional[Dict[str, Any]]] = [None] * len(titles)
        uncached_idx: List[int] = []

        for idx, title in enumerate(titles):
            cached = self._summary_cache_get(title, max_length)
            if cached:
                results[idx] = cached
            else:
                uncached_idx.append(idx)

        glm_targets = uncached_idx[:max_glm_items]
        fallback_targets = uncached_idx[max_glm_items:]

        for idx in fallback_targets:
            fallback = _summarize_fallback(titles[idx], max_length)
            results[idx] = fallback
            self._summary_cache_set(titles[idx], max_length, fallback)

        for i in range(0, len(glm_targets), batch_size):
            chunk_idx = glm_targets[i : i + batch_size]
            chunk_titles = [titles[idx] for idx in chunk_idx]
            chunk_res = await self._call_summarize_batch(chunk_titles, max_length)
            for idx, res in zip(chunk_idx, chunk_res):
                results[idx] = res
                self._summary_cache_set(titles[idx], max_length, res)

        final: List[Dict[str, Any]] = []
        for idx, item in enumerate(results):
            if item is None:
                item = _summarize_fallback(titles[idx], max_length)
                self._summary_cache_set(titles[idx], max_length, item)
            final.append(item)

        return final

    def _summary_cache_key(self, title: str, max_length: int) -> str:
        """Generate cache key for summary results."""
        seed = f"{str(title or '').strip()}|{int(max_length)}"
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    def _summary_cache_get(self, title: str, max_length: int) -> Optional[Dict[str, Any]]:
        """Get cached summary result."""
        return self._summary_cache.get(self._summary_cache_key(title, max_length))

    def _summary_cache_set(self, title: str, max_length: int, result: Dict[str, Any]) -> None:
        """Cache summary result."""
        if len(self._summary_cache) >= self._summary_cache_max:
            try:
                self._summary_cache.pop(next(iter(self._summary_cache)))
            except Exception:
                self._summary_cache.clear()
        self._summary_cache[self._summary_cache_key(title, max_length)] = {
            "summary": str(result.get("summary") or ""),
            "sentiment": str(result.get("sentiment") or "neutral"),
            "source": str(result.get("source") or "unknown"),
        }

    _summary_cache: Dict[str, Dict[str, Any]] = {}
    _summary_cache_max: int = 4000


async def extract_events_async_with_meta(
    news_items: List[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """Convenience function to extract events using AsyncGLMClient with metadata.

    Args:
        news_items: List of news items to process
        cfg: Configuration dictionary

    Returns:
        Tuple of (events, llm_used, error_type)
        llm_used: Whether LLM was successfully used
        error_type: "none", "rate_limit", "timeout", "other"
    """
    client = AsyncGLMClient(cfg)
    return await client.extract_events(news_items, cfg)


async def summarize_batch_async(
    titles: List[str],
    cfg: Dict[str, Any],
    max_length: int = 60,
) -> List[Dict[str, Any]]:
    """Convenience function to summarize a batch of titles.

    Args:
        titles: List of titles to summarize
        cfg: Configuration dictionary
        max_length: Maximum length of each summary

    Returns:
        List of summary results
    """
    client = AsyncGLMClient(cfg)
    return await client.summarize_batch(titles, cfg, max_length)
