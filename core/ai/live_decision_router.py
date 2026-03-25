from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

from config.settings import settings
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.utils.openai_responses import (
    build_openai_headers,
    build_responses_payload,
    extract_response_text,
    responses_endpoint,
)

# Persistent overlay for runtime config — survives service restarts
_OVERLAY_PATH = Path(os.environ.get("AI_RUNTIME_CONFIG_PATH", "data/ai_runtime_config.json"))
# Keys that may be persisted (excludes secrets like API keys)
_PERSISTABLE_KEYS = frozenset({
    "AI_LIVE_DECISION_ENABLED",
    "AI_LIVE_DECISION_MODE",
    "AI_LIVE_DECISION_PROVIDER",
    "AI_LIVE_DECISION_MODEL",
    "AI_LIVE_DECISION_TIMEOUT_MS",
    "AI_LIVE_DECISION_MAX_TOKENS",
    "AI_LIVE_DECISION_CONFIDENCE_THRESHOLD",
    "AI_LIVE_DECISION_FAIL_OPEN",
    "AI_LIVE_DECISION_APPLY_IN_PAPER",
})


_DEFAULT_OPENAI_BASE_URL = "https://vpsairobot.com/v1"
_DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com"
_DEFAULT_GLM_BASE_URL = "https://open.bigmodel.cn/api/coding/paas/v4"
_SUPPORTED_PROVIDERS = {"glm", "codex", "claude"}
_SUPPORTED_MODES = {"shadow", "enforce"}
_SUPPORTED_ACTIONS = {"allow", "block", "reduce_only"}


def _normalize_provider(value: Any) -> str:
    text = str(value or "codex").strip().lower()
    aliases = {"openai": "codex"}
    text = aliases.get(text, text)
    if text not in _SUPPORTED_PROVIDERS:
        raise ValueError("provider must be one of: glm/codex(openai)/claude")
    return text


def _normalize_mode(value: Any) -> str:
    text = str(value or "shadow").strip().lower()
    if text not in _SUPPORTED_MODES:
        raise ValueError("mode must be one of: shadow/enforce")
    return text


def _coerce_float(value: Any, default: float, *, low: float, high: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(low, min(high, parsed))


def _coerce_int(value: Any, default: int, *, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(low, min(high, parsed))


def _extract_json_obj(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty response")

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
        raw = re.sub(r"```$", "", raw).strip()

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    left = raw.find("{")
    right = raw.rfind("}")
    if left >= 0 and right > left:
        data = json.loads(raw[left : right + 1])
        if isinstance(data, dict):
            return data
    raise ValueError("invalid json object")


@dataclass
class LiveDecisionOutcome:
    enabled: bool
    applied: bool
    mode: str
    provider: str
    model: str
    action: str
    allowed: bool
    reason: str
    confidence: float
    latency_ms: int
    error: Optional[str] = None
    research_context: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "applied": bool(self.applied),
            "mode": str(self.mode),
            "provider": str(self.provider),
            "model": str(self.model),
            "action": str(self.action),
            "allowed": bool(self.allowed),
            "reason": str(self.reason),
            "confidence": float(self.confidence),
            "latency_ms": int(self.latency_ms),
            "error": self.error,
            "research_context": self.research_context or {},
        }


class LiveAIDecisionRouter:
    def __init__(self) -> None:
        self._override: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._load_overlay()

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _load_overlay(self) -> None:
        """Load persisted runtime config from JSON overlay on startup."""
        try:
            if _OVERLAY_PATH.exists():
                raw = _OVERLAY_PATH.read_text(encoding="utf-8")
                data = json.loads(raw)
                if isinstance(data, dict):
                    safe = {k: v for k, v in data.items() if k in _PERSISTABLE_KEYS}
                    self._override.update(safe)
                    logger.info(f"live_decision_router: loaded {len(safe)} persisted config keys from {_OVERLAY_PATH}")
        except Exception as exc:
            logger.warning(f"live_decision_router: failed to load overlay (using defaults): {exc}")

    def _save_overlay(self) -> None:
        """Atomically persist current _override to JSON overlay."""
        try:
            _OVERLAY_PATH.parent.mkdir(parents=True, exist_ok=True)
            safe = {k: v for k, v in self._override.items() if k in _PERSISTABLE_KEYS}
            tmp = _OVERLAY_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(safe, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(_OVERLAY_PATH)
        except Exception as exc:
            logger.warning(f"live_decision_router: failed to save overlay: {exc}")

    def _provider_base_url(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL).rstrip("/")
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_BASE_URL", "") or _DEFAULT_ANTHROPIC_BASE_URL).rstrip("/")
        return str(getattr(settings, "ZHIPU_BASE_URL", "") or _DEFAULT_GLM_BASE_URL).rstrip("/")

    def _provider_model(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return str(getattr(settings, "OPENAI_MODEL", "") or "gpt-5.4")
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_MODEL", "") or "claude-3-5-sonnet-latest")
        return str(getattr(settings, "ZHIPU_MODEL", "") or "GLM-4.5-Air")

    def _provider_api_key(self, provider: str) -> str:
        provider = _normalize_provider(provider)
        if provider == "codex":
            return str(getattr(settings, "OPENAI_API_KEY", "") or "").strip()
        if provider == "claude":
            return str(getattr(settings, "ANTHROPIC_API_KEY", "") or "").strip()
        return str(getattr(settings, "ZHIPU_API_KEY", "") or "").strip()

    def _get(self, name: str, fallback: Any = None) -> Any:
        if name in self._override:
            return self._override[name]
        return getattr(settings, name, fallback)

    def get_runtime_config(self) -> Dict[str, Any]:
        provider = _normalize_provider(self._get("AI_LIVE_DECISION_PROVIDER", "codex"))
        model_override = str(self._get("AI_LIVE_DECISION_MODEL", "") or "").strip()
        model = model_override or self._provider_model(provider)
        mode = _normalize_mode(self._get("AI_LIVE_DECISION_MODE", "shadow"))
        timeout_ms = _coerce_int(self._get("AI_LIVE_DECISION_TIMEOUT_MS", 6000), 6000, low=1000, high=60000)
        max_tokens = _coerce_int(self._get("AI_LIVE_DECISION_MAX_TOKENS", 220), 220, low=32, high=4096)
        temperature = _coerce_float(
            self._get("AI_LIVE_DECISION_TEMPERATURE", 0.0),
            0.0,
            low=0.0,
            high=1.5,
        )
        enabled = bool(self._get("AI_LIVE_DECISION_ENABLED", False))
        fail_open = bool(self._get("AI_LIVE_DECISION_FAIL_OPEN", True))
        apply_in_paper = bool(self._get("AI_LIVE_DECISION_APPLY_IN_PAPER", False))

        providers = {}
        for item in sorted(_SUPPORTED_PROVIDERS):
            providers[item] = {
                "available": bool(self._provider_api_key(item)),
                "default_model": self._provider_model(item),
                "base_url": self._provider_base_url(item),
            }

        return {
            "enabled": enabled,
            "mode": mode,
            "provider": provider,
            "model": model,
            "timeout_ms": timeout_ms,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "fail_open": fail_open,
            "apply_in_paper": apply_in_paper,
            "providers": providers,
        }

    async def update_runtime_config(self, **kwargs: Any) -> Dict[str, Any]:
        updates: Dict[str, Any] = {}
        if "enabled" in kwargs and kwargs["enabled"] is not None:
            updates["AI_LIVE_DECISION_ENABLED"] = bool(kwargs["enabled"])
        if "mode" in kwargs and kwargs["mode"] is not None:
            updates["AI_LIVE_DECISION_MODE"] = _normalize_mode(kwargs["mode"])
        if "provider" in kwargs and kwargs["provider"] is not None:
            updates["AI_LIVE_DECISION_PROVIDER"] = _normalize_provider(kwargs["provider"])
        if "model" in kwargs and kwargs["model"] is not None:
            updates["AI_LIVE_DECISION_MODEL"] = str(kwargs["model"]).strip()
        if "timeout_ms" in kwargs and kwargs["timeout_ms"] is not None:
            updates["AI_LIVE_DECISION_TIMEOUT_MS"] = _coerce_int(kwargs["timeout_ms"], 6000, low=1000, high=60000)
        if "max_tokens" in kwargs and kwargs["max_tokens"] is not None:
            updates["AI_LIVE_DECISION_MAX_TOKENS"] = _coerce_int(kwargs["max_tokens"], 220, low=32, high=4096)
        if "temperature" in kwargs and kwargs["temperature"] is not None:
            updates["AI_LIVE_DECISION_TEMPERATURE"] = _coerce_float(kwargs["temperature"], 0.0, low=0.0, high=1.5)
        if "fail_open" in kwargs and kwargs["fail_open"] is not None:
            updates["AI_LIVE_DECISION_FAIL_OPEN"] = bool(kwargs["fail_open"])
        if "apply_in_paper" in kwargs and kwargs["apply_in_paper"] is not None:
            updates["AI_LIVE_DECISION_APPLY_IN_PAPER"] = bool(kwargs["apply_in_paper"])

        if not updates:
            return self.get_runtime_config()

        async with self._lock:
            self._override.update(updates)

        self._save_overlay()
        return self.get_runtime_config()

    async def _call_provider(
        self,
        *,
        provider: str,
        model: str,
        timeout_ms: int,
        max_tokens: int,
        temperature: float,
        system_prompt: str,
        user_prompt: str,
    ) -> Dict[str, Any]:
        provider = _normalize_provider(provider)
        api_key = self._provider_api_key(provider)
        if not api_key:
            raise RuntimeError(f"{provider}_api_key_missing")

        timeout = aiohttp.ClientTimeout(total=max(1, int(timeout_ms)) / 1000.0)
        base_url = self._provider_base_url(provider)

        if provider == "claude":
            url = f"{base_url}/v1/messages" if not base_url.endswith("/v1") else f"{base_url}/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            payload = {
                "model": model,
                "max_tokens": int(max_tokens),
                "temperature": float(temperature),
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status >= 400:
                        body = (await resp.text())[:300]
                        raise RuntimeError(f"claude_http_{resp.status}:{body}")
                    data = await resp.json()
            content = data.get("content")
            text = ""
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and str(item.get("type") or "") == "text":
                        text = str(item.get("text") or "")
                        if text:
                            break
            if not text:
                raise RuntimeError("claude_empty_content")
            return _extract_json_obj(text)

        if provider == "codex":
            url = responses_endpoint(base_url)
            headers = build_openai_headers(api_key)
            payload = build_responses_payload(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_output_tokens=int(max_tokens),
                temperature=float(temperature),
                text_format="json_object",
            )
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status >= 400:
                        body = (await resp.text())[:300]
                        raise RuntimeError(f"{provider}_http_{resp.status}:{body}")
                    data = await resp.json()
            text = extract_response_text(data)
            if not text:
                raise RuntimeError(f"{provider}_empty_content")
            return _extract_json_obj(text)

        url = f"{base_url}/chat/completions"
        headers = build_openai_headers(api_key)
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
            "response_format": {"type": "json_object"},
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    body = (await resp.text())[:300]
                    raise RuntimeError(f"{provider}_http_{resp.status}:{body}")
                data = await resp.json()
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            raise RuntimeError(f"{provider}_empty_choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = message.get("content")
        if isinstance(content, list):
            text = "\n".join(str(x.get("text") or x.get("content") or "") for x in content if isinstance(x, dict)).strip()
        else:
            text = str(content or "").strip()
        if not text:
            raise RuntimeError(f"{provider}_empty_content")
        return _extract_json_obj(text)

    def _build_prompt(self, payload: Dict[str, Any]) -> tuple[str, str]:
        system_prompt = (
            "You are a crypto trading live risk-and-decision gate. "
            "Return strict JSON only. Never output markdown."
        )
        user_prompt = {
            "task": "Given current strategy signal/context, decide if live execution should proceed.",
            "output_schema": {
                "action": "allow|block|reduce_only",
                "reason": "short reason <= 140 chars",
                "confidence": "float in [0,1]",
            },
            "policy": [
                "Prefer block when confidence is low with elevated uncertainty.",
                "Block if signal conflicts with obvious risk context in payload.",
                "Use research_context as an advisory source of the current champion candidate and active runtime candidate, not as an execution authority.",
                "If uncertain, output allow with lower confidence, not fabricated certainty.",
            ],
            "input": payload,
        }
        return system_prompt, json.dumps(user_prompt, ensure_ascii=False)

    async def evaluate_signal(
        self,
        *,
        trading_mode: str,
        strategy: str,
        symbol: str,
        signal_type: str,
        signal_strength: float,
        price: float,
        account_equity: float,
        order_value: float,
        leverage: float,
        timeframe: str = "",
        existing_position: Optional[Dict[str, Any]] = None,
        trade_policy: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = self.get_runtime_config()
        enabled = bool(cfg.get("enabled", False))
        apply_in_paper = bool(cfg.get("apply_in_paper", False))
        mode = str(cfg.get("mode") or "shadow")
        provider = str(cfg.get("provider") or "codex")
        model = str(cfg.get("model") or "")
        timeout_ms = int(cfg.get("timeout_ms") or 6000)
        max_tokens = int(cfg.get("max_tokens") or 220)
        temperature = float(cfg.get("temperature") or 0.0)
        fail_open = bool(cfg.get("fail_open", True))

        effective_mode = str(trading_mode or "paper").strip().lower()
        if (not enabled) or (effective_mode != "live" and not apply_in_paper):
            return LiveDecisionOutcome(
                enabled=enabled,
                applied=False,
                mode=mode,
                provider=provider,
                model=model,
                action="allow",
                allowed=True,
                reason="ai_live_decision_disabled",
                confidence=1.0,
                latency_ms=0,
                research_context={},
            ).to_dict()

        research_context = resolve_runtime_research_context(
            exchange=str((metadata or {}).get("exchange") or ""),
            symbol=str(symbol or ""),
            timeframe=str(timeframe or ""),
            strategy_name=str(strategy or ""),
        )

        prompt_payload = {
            "trading_mode": effective_mode,
            "strategy": str(strategy or ""),
            "symbol": str(symbol or ""),
            "signal_type": str(signal_type or "").lower(),
            "signal_strength": float(signal_strength or 0.0),
            "price": float(price or 0.0),
            "account_equity": float(account_equity or 0.0),
            "order_value": float(order_value or 0.0),
            "leverage": float(leverage or 1.0),
            "timeframe": str(timeframe or ""),
            "existing_position": existing_position or {},
            "trade_policy": trade_policy or {},
            "metadata": metadata or {},
            "research_context": research_context,
        }
        system_prompt, user_prompt = self._build_prompt(prompt_payload)
        started = time.perf_counter()

        try:
            payload = await self._call_provider(
                provider=provider,
                model=model,
                timeout_ms=timeout_ms,
                max_tokens=max_tokens,
                temperature=temperature,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            action = str(payload.get("action") or "allow").strip().lower()
            if action not in _SUPPORTED_ACTIONS:
                action = "allow"
            confidence = _coerce_float(payload.get("confidence", 0.5), 0.5, low=0.0, high=1.0)
            reason = str(payload.get("reason") or "model_decision").strip()[:140] or "model_decision"
            blocked = action == "block"
            reduce_only = action == "reduce_only"
            applied = bool(mode == "enforce" and action in {"block", "reduce_only"})
            allowed = not blocked if mode == "enforce" else True
            latency_ms = int((time.perf_counter() - started) * 1000)
            return LiveDecisionOutcome(
                enabled=True,
                applied=applied,
                mode=mode,
                provider=provider,
                model=model,
                action=action,
                allowed=allowed,
                reason=reason,
                confidence=confidence,
                latency_ms=latency_ms,
                research_context=research_context,
            ).to_dict()
        except Exception as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            err = str(exc)
            logger.warning(f"live decision router failed ({provider}/{model}): {err}")
            if fail_open:
                return LiveDecisionOutcome(
                    enabled=True,
                    applied=False,
                    mode=mode,
                    provider=provider,
                    model=model,
                    action="allow",
                    allowed=True,
                    reason="ai_error_fail_open",
                    confidence=0.0,
                    latency_ms=latency_ms,
                    error=err,
                    research_context=research_context,
                ).to_dict()
            blocked = mode == "enforce"
            return LiveDecisionOutcome(
                enabled=True,
                applied=blocked,
                mode=mode,
                provider=provider,
                model=model,
                action="block" if blocked else "allow",
                allowed=not blocked,
                reason="ai_error_fail_closed" if blocked else "ai_error_shadow",
                confidence=0.0,
                latency_ms=latency_ms,
                error=err,
                research_context=research_context,
            ).to_dict()


live_decision_router = LiveAIDecisionRouter()
