from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from loguru import logger

from config.settings import settings
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.ai.signal_aggregator import signal_aggregator
from core.data import data_storage
from core.exchanges import exchange_manager
from core.runtime import runtime_state
from core.strategies import Signal, SignalType
from core.trading import execution_engine, position_manager
from core.utils.openai_responses import (
    build_openai_headers,
    build_responses_payload,
    extract_response_text,
    responses_endpoint,
)


_DEFAULT_OPENAI_BASE_URL = "https://vpsairobot.com/v1"
_DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com"
_DEFAULT_GLM_BASE_URL = "https://open.bigmodel.cn/api/coding/paas/v4"

_SUPPORTED_PROVIDERS = {"glm", "codex", "claude"}
_SUPPORTED_MODES = {"shadow", "execute"}
_SUPPORTED_ACTIONS = {"buy", "sell", "hold", "close_long", "close_short"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
        raise ValueError("mode must be one of: shadow/execute")
    return text


def _normalize_action(value: Any) -> str:
    text = str(value or "hold").strip().lower()
    aliases = {
        "long": "buy",
        "short": "sell",
        "flat": "hold",
        "close": "hold",
        "exit": "hold",
    }
    text = aliases.get(text, text)
    if text not in _SUPPORTED_ACTIONS:
        return "hold"
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


def _timeframe_to_seconds(timeframe: str) -> int:
    text = str(timeframe or "15m").strip().lower()
    m = re.fullmatch(r"(\d+)([smhdw])", text)
    if not m:
        if text == "1m":
            return 60
        if text == "1h":
            return 3600
        if text == "1d":
            return 86400
        return 900
    value = int(m.group(1))
    unit = m.group(2)
    mul = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 86400 * 7}[unit]
    return max(1, value * mul)


def _default_profile() -> Dict[str, Any]:
    return {
        "updated_at": None,
        "decision_count": 0,
        "executed_count": 0,
        "action_counts": {},
        "avg_confidence": 0.0,
        "avg_strength": 0.0,
        "avg_leverage": 1.0,
        "avg_stop_loss_pct": 0.02,
        "avg_take_profit_pct": 0.04,
    }


_AGENT_PERSISTABLE_KEYS = frozenset({
    "AI_AUTONOMOUS_AGENT_ENABLED",
    "AI_AUTONOMOUS_AGENT_AUTO_START",
    "AI_AUTONOMOUS_AGENT_PROVIDER",
    "AI_AUTONOMOUS_AGENT_MODEL",
    "AI_AUTONOMOUS_AGENT_INTERVAL_SEC",
    "AI_AUTONOMOUS_AGENT_COOLDOWN_SEC",
    "AI_AUTONOMOUS_AGENT_MODE",
    "AI_AUTONOMOUS_AGENT_ALLOW_LIVE",
    "AI_AUTONOMOUS_AGENT_ACCOUNT_ID",
    "AI_AUTONOMOUS_AGENT_STRATEGY_NAME",
})


class AutonomousTradingAgent:
    """Independent AI trading agent that generates and executes signals."""

    def __init__(self, cache_root: Optional[Path] = None) -> None:
        self._override: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

        root = cache_root or (Path(settings.CACHE_PATH) / "ai")
        self._cache_root = root
        self._journal_path = self._cache_root / "autonomous_agent_journal.jsonl"
        self._profile_path = self._cache_root / "autonomous_agent_profile.json"
        self._overlay_path = Path(
            os.environ.get("AI_AGENT_CONFIG_PATH", str(self._cache_root / "agent_runtime_config.json"))
        )

        self._load_overlay()
        self._profile = self._load_profile()
        self._last_error: Optional[str] = None
        self._last_run_at: Optional[str] = None
        self._last_decision: Optional[Dict[str, Any]] = None
        self._last_execution: Optional[Dict[str, Any]] = None
        self._last_research_context: Optional[Dict[str, Any]] = None
        self._tick_count: int = 0
        self._submitted_count: int = 0
        self._last_submit_at: Optional[float] = None

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
        provider = _normalize_provider(self._get("AI_AUTONOMOUS_AGENT_PROVIDER", "codex"))
        model_override = str(self._get("AI_AUTONOMOUS_AGENT_MODEL", "") or "").strip()
        model = model_override or self._provider_model(provider)

        providers: Dict[str, Dict[str, Any]] = {}
        for item in sorted(_SUPPORTED_PROVIDERS):
            providers[item] = {
                "available": bool(self._provider_api_key(item)),
                "default_model": self._provider_model(item),
                "base_url": self._provider_base_url(item),
            }

        return {
            "enabled": bool(self._get("AI_AUTONOMOUS_AGENT_ENABLED", False)),
            "auto_start": bool(self._get("AI_AUTONOMOUS_AGENT_AUTO_START", False)),
            "mode": _normalize_mode(self._get("AI_AUTONOMOUS_AGENT_MODE", "shadow")),
            "provider": provider,
            "model": model,
            "exchange": str(self._get("AI_AUTONOMOUS_AGENT_EXCHANGE", "binance") or "binance").strip().lower(),
            "symbol": str(self._get("AI_AUTONOMOUS_AGENT_SYMBOL", "BTC/USDT") or "BTC/USDT").strip().upper(),
            "timeframe": str(self._get("AI_AUTONOMOUS_AGENT_TIMEFRAME", "15m") or "15m").strip(),
            "interval_sec": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_INTERVAL_SEC", 120), 120, low=15, high=7200),
            "lookback_bars": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_LOOKBACK_BARS", 240), 240, low=30, high=4000),
            "min_confidence": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE", 0.58), 0.58, low=0.0, high=1.0),
            "default_leverage": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_DEFAULT_LEVERAGE", 3.0), 3.0, low=1.0, high=125.0),
            "max_leverage": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_MAX_LEVERAGE", 20.0), 20.0, low=1.0, high=125.0),
            "default_stop_loss_pct": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_STOP_LOSS_PCT", 0.02), 0.02, low=0.001, high=0.5),
            "default_take_profit_pct": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_TAKE_PROFIT_PCT", 0.04), 0.04, low=0.001, high=2.0),
            "timeout_ms": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_TIMEOUT_MS", 12000), 12000, low=1000, high=120000),
            "max_tokens": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_MAX_TOKENS", 420), 420, low=32, high=4096),
            "temperature": _coerce_float(self._get("AI_AUTONOMOUS_AGENT_TEMPERATURE", 0.15), 0.15, low=0.0, high=1.5),
            "cooldown_sec": _coerce_int(self._get("AI_AUTONOMOUS_AGENT_COOLDOWN_SEC", 180), 180, low=0, high=86400),
            "allow_live": bool(self._get("AI_AUTONOMOUS_AGENT_ALLOW_LIVE", False)),
            "account_id": str(self._get("AI_AUTONOMOUS_AGENT_ACCOUNT_ID", "main") or "main").strip() or "main",
            "strategy_name": str(self._get("AI_AUTONOMOUS_AGENT_STRATEGY_NAME", "AI_AutonomousAgent") or "AI_AutonomousAgent").strip() or "AI_AutonomousAgent",
            "providers": providers,
        }

    async def update_runtime_config(self, **kwargs: Any) -> Dict[str, Any]:
        updates: Dict[str, Any] = {}
        if "enabled" in kwargs and kwargs["enabled"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ENABLED"] = bool(kwargs["enabled"])
        if "auto_start" in kwargs and kwargs["auto_start"] is not None:
            updates["AI_AUTONOMOUS_AGENT_AUTO_START"] = bool(kwargs["auto_start"])
        if "mode" in kwargs and kwargs["mode"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MODE"] = _normalize_mode(kwargs["mode"])
        if "provider" in kwargs and kwargs["provider"] is not None:
            updates["AI_AUTONOMOUS_AGENT_PROVIDER"] = _normalize_provider(kwargs["provider"])
        if "model" in kwargs and kwargs["model"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MODEL"] = str(kwargs["model"]).strip()
        if "exchange" in kwargs and kwargs["exchange"] is not None:
            updates["AI_AUTONOMOUS_AGENT_EXCHANGE"] = str(kwargs["exchange"]).strip().lower() or "binance"
        if "symbol" in kwargs and kwargs["symbol"] is not None:
            updates["AI_AUTONOMOUS_AGENT_SYMBOL"] = str(kwargs["symbol"]).strip().upper() or "BTC/USDT"
        if "timeframe" in kwargs and kwargs["timeframe"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TIMEFRAME"] = str(kwargs["timeframe"]).strip() or "15m"
        if "interval_sec" in kwargs and kwargs["interval_sec"] is not None:
            updates["AI_AUTONOMOUS_AGENT_INTERVAL_SEC"] = _coerce_int(kwargs["interval_sec"], 120, low=15, high=7200)
        if "lookback_bars" in kwargs and kwargs["lookback_bars"] is not None:
            updates["AI_AUTONOMOUS_AGENT_LOOKBACK_BARS"] = _coerce_int(kwargs["lookback_bars"], 240, low=30, high=4000)
        if "min_confidence" in kwargs and kwargs["min_confidence"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE"] = _coerce_float(kwargs["min_confidence"], 0.58, low=0.0, high=1.0)
        if "default_leverage" in kwargs and kwargs["default_leverage"] is not None:
            updates["AI_AUTONOMOUS_AGENT_DEFAULT_LEVERAGE"] = _coerce_float(kwargs["default_leverage"], 3.0, low=1.0, high=125.0)
        if "max_leverage" in kwargs and kwargs["max_leverage"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MAX_LEVERAGE"] = _coerce_float(kwargs["max_leverage"], 20.0, low=1.0, high=125.0)
        if "default_stop_loss_pct" in kwargs and kwargs["default_stop_loss_pct"] is not None:
            updates["AI_AUTONOMOUS_AGENT_STOP_LOSS_PCT"] = _coerce_float(kwargs["default_stop_loss_pct"], 0.02, low=0.001, high=0.5)
        if "default_take_profit_pct" in kwargs and kwargs["default_take_profit_pct"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TAKE_PROFIT_PCT"] = _coerce_float(kwargs["default_take_profit_pct"], 0.04, low=0.001, high=2.0)
        if "timeout_ms" in kwargs and kwargs["timeout_ms"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TIMEOUT_MS"] = _coerce_int(kwargs["timeout_ms"], 12000, low=1000, high=120000)
        if "max_tokens" in kwargs and kwargs["max_tokens"] is not None:
            updates["AI_AUTONOMOUS_AGENT_MAX_TOKENS"] = _coerce_int(kwargs["max_tokens"], 420, low=32, high=4096)
        if "temperature" in kwargs and kwargs["temperature"] is not None:
            updates["AI_AUTONOMOUS_AGENT_TEMPERATURE"] = _coerce_float(kwargs["temperature"], 0.15, low=0.0, high=1.5)
        if "cooldown_sec" in kwargs and kwargs["cooldown_sec"] is not None:
            updates["AI_AUTONOMOUS_AGENT_COOLDOWN_SEC"] = _coerce_int(kwargs["cooldown_sec"], 180, low=0, high=86400)
        if "allow_live" in kwargs and kwargs["allow_live"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ALLOW_LIVE"] = bool(kwargs["allow_live"])
        if "account_id" in kwargs and kwargs["account_id"] is not None:
            updates["AI_AUTONOMOUS_AGENT_ACCOUNT_ID"] = str(kwargs["account_id"]).strip() or "main"
        if "strategy_name" in kwargs and kwargs["strategy_name"] is not None:
            updates["AI_AUTONOMOUS_AGENT_STRATEGY_NAME"] = str(kwargs["strategy_name"]).strip() or "AI_AutonomousAgent"

        if not updates:
            return self.get_runtime_config()
        async with self._lock:
            self._override.update(updates)
        self._save_overlay()
        return self.get_runtime_config()

    def is_running(self) -> bool:
        return bool(self._task and not self._task.done())

    def get_status(self) -> Dict[str, Any]:
        return {
            "running": self.is_running(),
            "last_run_at": self._last_run_at,
            "last_error": self._last_error,
            "tick_count": int(self._tick_count),
            "submitted_count": int(self._submitted_count),
            "last_decision": self._last_decision,
            "last_execution": self._last_execution,
            "last_research_context": self._last_research_context,
            "profile": dict(self._profile or _default_profile()),
            "journal_path": str(self._journal_path),
        }

    async def start(self) -> Dict[str, Any]:
        if self.is_running():
            return self.get_status()
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="ai_autonomous_agent")
        runtime_state.register_task("ai_autonomous_agent", restart_on_failure=False)
        runtime_state.mark_task_started("ai_autonomous_agent")
        logger.info("AutonomousTradingAgent started")
        return self.get_status()

    async def stop(self) -> Dict[str, Any]:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        self._stop_event = None
        runtime_state.mark_task_stopped("ai_autonomous_agent")
        logger.info("AutonomousTradingAgent stopped")
        return self.get_status()

    async def _loop(self) -> None:
        await asyncio.sleep(2)
        while self._stop_event is not None and not self._stop_event.is_set():
            try:
                await self.run_once(trigger="loop")
                runtime_state.touch_task("ai_autonomous_agent", success=True)
            except Exception as exc:
                self._last_error = str(exc)
                runtime_state.mark_task_failed("ai_autonomous_agent", str(exc), will_restart=False)
                logger.warning(f"autonomous agent tick failed: {exc}")
            interval = int(self.get_runtime_config().get("interval_sec") or 120)
            for _ in range(max(1, interval)):
                if self._stop_event is None or self._stop_event.is_set():
                    break
                await asyncio.sleep(1)

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
            text = ""
            content = data.get("content") if isinstance(data, dict) else None
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
            text = "\n".join(
                str(x.get("text") or x.get("content") or "")
                for x in content
                if isinstance(x, dict)
            ).strip()
        else:
            text = str(content or "").strip()
        if not text:
            raise RuntimeError(f"{provider}_empty_content")
        return _extract_json_obj(text)

    async def _load_market_data(self, cfg: Dict[str, Any]) -> pd.DataFrame:
        now = _utc_now()
        timeframe_sec = _timeframe_to_seconds(str(cfg.get("timeframe") or "15m"))
        lookback = int(cfg.get("lookback_bars") or 240)
        span_sec = max(timeframe_sec * lookback, 3600 * 8)
        start_time = now - timedelta(seconds=span_sec + timeframe_sec * 2)
        df = await data_storage.load_klines_from_parquet(
            exchange=str(cfg.get("exchange") or "binance"),
            symbol=str(cfg.get("symbol") or "BTC/USDT"),
            timeframe=str(cfg.get("timeframe") or "15m"),
            start_time=start_time,
            end_time=now,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.tail(max(40, lookback)).copy()
        return df

    async def _resolve_last_price(self, cfg: Dict[str, Any], market_data: pd.DataFrame) -> float:
        if market_data is not None and not market_data.empty and "close" in market_data.columns:
            try:
                value = float(pd.to_numeric(market_data["close"], errors="coerce").dropna().iloc[-1])
                if value > 0:
                    return value
            except Exception:
                pass
        exchange = str(cfg.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        connector = exchange_manager.get_exchange(exchange)
        if connector is None:
            return 0.0
        try:
            ticker = await connector.get_ticker(symbol)
            return float(getattr(ticker, "last", 0.0) or 0.0)
        except Exception:
            return 0.0

    async def _build_context(self, cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], pd.DataFrame]:
        market_data = await self._load_market_data(cfg)
        last_price = await self._resolve_last_price(cfg, market_data)
        timeframe = str(cfg.get("timeframe") or "15m")
        timeframe_sec = _timeframe_to_seconds(timeframe)

        close_series = pd.Series(dtype=float)
        if market_data is not None and not market_data.empty and "close" in market_data.columns:
            close_series = pd.to_numeric(market_data["close"], errors="coerce").dropna()

        def _pct_change(steps: int) -> float:
            if close_series.empty or len(close_series) <= steps:
                return 0.0
            denom = float(close_series.iloc[-steps - 1] or 0.0)
            if denom <= 0:
                return 0.0
            return float(close_series.iloc[-1] / denom - 1.0)

        steps_1h = max(1, int(round(3600 / timeframe_sec)))
        steps_24h = max(1, int(round(86400 / timeframe_sec)))

        returns = close_series.pct_change().dropna()
        annual_factor = max(1.0, (86400.0 * 365.0) / max(1.0, float(timeframe_sec)))
        realized_vol = 0.0
        if not returns.empty:
            realized_vol = float(returns.tail(120).std() * (annual_factor ** 0.5))

        agg_signal: Dict[str, Any] = {}
        try:
            agg = await signal_aggregator.aggregate(symbol=str(cfg["symbol"]), market_data=market_data)
            agg_signal = agg.to_dict() if hasattr(agg, "to_dict") else {}
        except Exception as exc:
            logger.debug(f"autonomous agent aggregate signal failed: {exc}")

        account_id = str(cfg.get("account_id") or "main")
        exchange = str(cfg.get("exchange") or "binance")
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        position = position_manager.get_position(exchange, symbol, account_id=account_id)
        position_payload: Dict[str, Any] = {}
        if position is not None:
            with contextlib.suppress(Exception):
                position_payload = {
                    "side": str(getattr(position.side, "value", "") or ""),
                    "quantity": float(getattr(position, "quantity", 0.0) or 0.0),
                    "entry_price": float(getattr(position, "entry_price", 0.0) or 0.0),
                    "current_price": float(getattr(position, "current_price", 0.0) or 0.0),
                    "unrealized_pnl": float(getattr(position, "unrealized_pnl", 0.0) or 0.0),
                }

        research_context = resolve_runtime_research_context(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "price": float(last_price or 0.0),
            "returns": {
                "r_1h": _pct_change(steps_1h),
                "r_24h": _pct_change(steps_24h),
            },
            "realized_vol_annualized": float(realized_vol),
            "bars": int(len(market_data) if market_data is not None else 0),
            "aggregated_signal": agg_signal,
            "position": position_payload,
            "research_context": research_context,
            "profile": dict(self._profile or _default_profile()),
            "trading_mode": execution_engine.get_trading_mode(),
        }, market_data

    def _build_prompt(self, cfg: Dict[str, Any], context_payload: Dict[str, Any]) -> Tuple[str, str]:
        system_prompt = (
            "You are an autonomous crypto trading agent. "
            "Return strict JSON only. Never output markdown."
        )
        user_payload = {
            "task": "Decide the next single trading action for current market context.",
            "output_schema": {
                "action": "buy|sell|hold|close_long|close_short",
                "confidence": "float in [0,1]",
                "strength": "float in [0.1,1.0]",
                "leverage": "float in [1,max_leverage]",
                "stop_loss_pct": "float > 0, fraction not percent",
                "take_profit_pct": "float > 0, fraction not percent",
                "reason": "short reason <= 180 chars",
            },
            "hard_rules": [
                "If uncertain or data quality is low, choose hold.",
                "Never fabricate certainty.",
                "Use tighter risk when volatility is high.",
                "If research_context is available, treat its selected_candidate as the current research champion hypothesis unless real-time risk clearly invalidates it.",
            ],
            "runtime_constraints": {
                "min_confidence": cfg.get("min_confidence"),
                "max_leverage": cfg.get("max_leverage"),
                "default_stop_loss_pct": cfg.get("default_stop_loss_pct"),
                "default_take_profit_pct": cfg.get("default_take_profit_pct"),
            },
            "input": context_payload,
        }
        return system_prompt, json.dumps(user_payload, ensure_ascii=False)

    def _normalize_decision(self, raw: Dict[str, Any], cfg: Dict[str, Any], context_payload: Dict[str, Any]) -> Dict[str, Any]:
        action = _normalize_action(raw.get("action"))
        confidence = _coerce_float(raw.get("confidence", 0.0), 0.0, low=0.0, high=1.0)
        strength = _coerce_float(raw.get("strength", max(0.2, confidence)), max(0.2, confidence), low=0.1, high=1.0)
        leverage = _coerce_float(
            raw.get("leverage", cfg.get("default_leverage", 3.0)),
            float(cfg.get("default_leverage") or 3.0),
            low=1.0,
            high=float(cfg.get("max_leverage") or 20.0),
        )
        stop_loss_pct = _coerce_float(
            raw.get("stop_loss_pct", cfg.get("default_stop_loss_pct", 0.02)),
            float(cfg.get("default_stop_loss_pct") or 0.02),
            low=0.001,
            high=0.5,
        )
        take_profit_pct = _coerce_float(
            raw.get("take_profit_pct", cfg.get("default_take_profit_pct", 0.04)),
            float(cfg.get("default_take_profit_pct") or 0.04),
            low=0.001,
            high=2.0,
        )
        if take_profit_pct <= stop_loss_pct:
            take_profit_pct = min(2.0, max(stop_loss_pct * 1.6, float(cfg.get("default_take_profit_pct") or 0.04)))
        reason = str(raw.get("reason") or "model_decision").strip()[:180] or "model_decision"

        min_conf = float(cfg.get("min_confidence") or 0.0)
        if action in {"buy", "sell"} and confidence < min_conf:
            action = "hold"
            reason = f"below_min_confidence({confidence:.3f}<{min_conf:.3f})"

        position = context_payload.get("position") if isinstance(context_payload, dict) else {}
        if action == "close_long" and str((position or {}).get("side") or "").lower() != "long":
            action = "hold"
            reason = "no_long_position"
        if action == "close_short" and str((position or {}).get("side") or "").lower() != "short":
            action = "hold"
            reason = "no_short_position"

        return {
            "action": action,
            "confidence": confidence,
            "strength": strength,
            "leverage": leverage,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "reason": reason,
        }

    @staticmethod
    def _signal_type_from_action(action: str) -> Optional[SignalType]:
        if action == "buy":
            return SignalType.BUY
        if action == "sell":
            return SignalType.SELL
        if action == "close_long":
            return SignalType.CLOSE_LONG
        if action == "close_short":
            return SignalType.CLOSE_SHORT
        return None

    def _build_signal(
        self,
        *,
        decision: Dict[str, Any],
        cfg: Dict[str, Any],
        context_payload: Dict[str, Any],
    ) -> Optional[Signal]:
        action = str(decision.get("action") or "hold")
        signal_type = self._signal_type_from_action(action)
        if signal_type is None:
            return None

        price = float(context_payload.get("price") or 0.0)
        stop_loss = None
        take_profit = None
        if action in {"buy", "sell"} and price > 0:
            sl_pct = float(decision.get("stop_loss_pct") or cfg.get("default_stop_loss_pct") or 0.02)
            tp_pct = float(decision.get("take_profit_pct") or cfg.get("default_take_profit_pct") or 0.04)
            if action == "buy":
                stop_loss = max(0.0, price * (1.0 - sl_pct))
                take_profit = max(0.0, price * (1.0 + tp_pct))
            else:
                stop_loss = max(0.0, price * (1.0 + sl_pct))
                take_profit = max(0.0, price * (1.0 - tp_pct))

        metadata = {
            "exchange": str(cfg.get("exchange") or "binance"),
            "account_id": str(cfg.get("account_id") or "main"),
            "leverage": float(decision.get("leverage") or cfg.get("default_leverage") or 1.0),
            "timeframe": str(cfg.get("timeframe") or ""),
            "source": "ai_autonomous_agent",
            "agent_provider": str(cfg.get("provider") or ""),
            "agent_model": str(cfg.get("model") or ""),
            "agent_confidence": float(decision.get("confidence") or 0.0),
            "agent_reason": str(decision.get("reason") or ""),
        }
        research_context = context_payload.get("research_context") if isinstance(context_payload, dict) else {}
        if isinstance(research_context, dict) and research_context.get("available"):
            selected_candidate = dict(research_context.get("selected_candidate") or {})
            champion_candidate = dict(research_context.get("research_champion") or {})
            metadata.update(
                {
                    "research_context_available": True,
                    "research_selection_reason": str(research_context.get("selection_reason") or ""),
                    "research_candidate_id": str(selected_candidate.get("candidate_id") or ""),
                    "research_proposal_id": str(selected_candidate.get("proposal_id") or ""),
                    "research_strategy": str(selected_candidate.get("strategy") or ""),
                    "research_score": _coerce_float(selected_candidate.get("score", 0.0), 0.0, low=0.0, high=1000000.0),
                    "research_status": str(selected_candidate.get("status") or ""),
                    "research_role": str(selected_candidate.get("search_role") or ""),
                    "research_champion_candidate_id": str(champion_candidate.get("candidate_id") or ""),
                    "research_champion_strategy": str(champion_candidate.get("strategy") or ""),
                }
            )
        return Signal(
            symbol=str(cfg.get("symbol") or "BTC/USDT"),
            signal_type=signal_type,
            price=float(price or 0.0),
            timestamp=_utc_now(),
            strategy_name=str(cfg.get("strategy_name") or "AI_AutonomousAgent"),
            strength=float(decision.get("strength") or 0.5),
            quantity=None,
            stop_loss=stop_loss if stop_loss and stop_loss > 0 else None,
            take_profit=take_profit if take_profit and take_profit > 0 else None,
            metadata=metadata,
        )

    def _load_overlay(self) -> None:
        """Load persisted agent config from JSON overlay on startup."""
        try:
            if self._overlay_path.exists():
                raw = self._overlay_path.read_text(encoding="utf-8")
                data = json.loads(raw)
                if isinstance(data, dict):
                    safe = {k: v for k, v in data.items() if k in _AGENT_PERSISTABLE_KEYS}
                    self._override.update(safe)
                    logger.info(f"autonomous_agent: loaded {len(safe)} persisted config keys")
        except Exception as exc:
            logger.warning(f"autonomous_agent: failed to load overlay (using defaults): {exc}")

    def _save_overlay(self) -> None:
        """Atomically persist current _override to JSON overlay."""
        try:
            self._overlay_path.parent.mkdir(parents=True, exist_ok=True)
            safe = {k: v for k, v in self._override.items() if k in _AGENT_PERSISTABLE_KEYS}
            tmp = self._overlay_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(safe, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._overlay_path)
        except Exception as exc:
            logger.warning(f"autonomous_agent: failed to save overlay: {exc}")

    def _load_profile(self) -> Dict[str, Any]:
        try:
            if not self._profile_path.exists():
                return _default_profile()
            raw = json.loads(self._profile_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return _default_profile()
            merged = _default_profile()
            merged.update(raw)
            if not isinstance(merged.get("action_counts"), dict):
                merged["action_counts"] = {}
            return merged
        except Exception as exc:
            logger.debug(f"load autonomous profile failed: {exc}")
            return _default_profile()

    def _save_profile(self) -> None:
        try:
            self._cache_root.mkdir(parents=True, exist_ok=True)
            tmp = self._profile_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._profile, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._profile_path)
        except Exception as exc:
            logger.warning(f"save autonomous profile failed: {exc}")

    def _update_profile(self, decision: Dict[str, Any], *, submitted: bool) -> None:
        profile = dict(self._profile or _default_profile())
        action = str(decision.get("action") or "hold")
        profile["decision_count"] = int(profile.get("decision_count") or 0) + 1
        if submitted:
            profile["executed_count"] = int(profile.get("executed_count") or 0) + 1
        actions = dict(profile.get("action_counts") or {})
        actions[action] = int(actions.get(action) or 0) + 1
        profile["action_counts"] = actions

        n = max(1, int(profile.get("decision_count") or 1))
        for key, source_key in (
            ("avg_confidence", "confidence"),
            ("avg_strength", "strength"),
            ("avg_leverage", "leverage"),
            ("avg_stop_loss_pct", "stop_loss_pct"),
            ("avg_take_profit_pct", "take_profit_pct"),
        ):
            old_v = float(profile.get(key) or 0.0)
            new_v = float(decision.get(source_key) or 0.0)
            profile[key] = ((old_v * (n - 1)) + new_v) / n
        profile["updated_at"] = _utc_now().isoformat()
        self._profile = profile
        self._save_profile()

    def _append_journal(self, payload: Dict[str, Any]) -> None:
        try:
            self._cache_root.mkdir(parents=True, exist_ok=True)
            with self._journal_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.warning(f"append autonomous journal failed: {exc}")

    def read_journal(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        size = max(1, min(int(limit or 50), 500))
        if not self._journal_path.exists():
            return rows
        try:
            lines = self._journal_path.read_text(encoding="utf-8").splitlines()
            for line in lines[-size:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    if isinstance(item, dict):
                        rows.append(item)
                except Exception:
                    continue
        except Exception as exc:
            logger.debug(f"read autonomous journal failed: {exc}")
        return rows

    async def run_once(self, *, trigger: str = "manual", force: bool = False) -> Dict[str, Any]:
        cfg = self.get_runtime_config()
        if not bool(cfg.get("enabled")) and not force:
            result = {
                "request_id": str(uuid.uuid4())[:8],
                "skipped": True,
                "reason": "agent_disabled",
                "rejection_reason": "agent_disabled",
                "execution_allowed": False,
                "trigger": trigger,
                "timestamp": _utc_now().isoformat(),
            }
            self._last_run_at = result["timestamp"]
            self._last_decision = {"action": "hold", "reason": "agent_disabled"}
            self._last_execution = {"submitted": False, "reason": "agent_disabled"}
            self._last_research_context = None
            return result

        started = time.perf_counter()
        context_payload, market_data = await self._build_context(cfg)
        research_context = context_payload.get("research_context") if isinstance(context_payload, dict) else {}
        if float(context_payload.get("price") or 0.0) <= 0:
            decision = {
                "action": "hold",
                "confidence": 0.0,
                "strength": 0.1,
                "leverage": float(cfg.get("default_leverage") or 1.0),
                "stop_loss_pct": float(cfg.get("default_stop_loss_pct") or 0.02),
                "take_profit_pct": float(cfg.get("default_take_profit_pct") or 0.04),
                "reason": "no_price",
            }
        else:
            provider = str(cfg.get("provider") or "codex")
            model = str(cfg.get("model") or self._provider_model(provider))
            system_prompt, user_prompt = self._build_prompt(cfg, context_payload)
            raw_decision: Dict[str, Any]
            try:
                raw_decision = await self._call_provider(
                    provider=provider,
                    model=model,
                    timeout_ms=int(cfg.get("timeout_ms") or 12000),
                    max_tokens=int(cfg.get("max_tokens") or 420),
                    temperature=float(cfg.get("temperature") or 0.15),
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                )
            except Exception as exc:
                raw_decision = {
                    "action": "hold",
                    "confidence": 0.0,
                    "strength": 0.1,
                    "leverage": cfg.get("default_leverage"),
                    "stop_loss_pct": cfg.get("default_stop_loss_pct"),
                    "take_profit_pct": cfg.get("default_take_profit_pct"),
                    "reason": f"model_error:{exc}",
                }
            decision = self._normalize_decision(raw_decision, cfg, context_payload)

        cooldown_sec = int(cfg.get("cooldown_sec") or 0)
        if (
            decision["action"] in {"buy", "sell", "close_long", "close_short"}
            and cooldown_sec > 0
            and self._last_submit_at is not None
        ):
            elapsed = time.time() - float(self._last_submit_at)
            if elapsed < cooldown_sec:
                decision["action"] = "hold"
                decision["reason"] = f"cooldown({elapsed:.1f}s<{cooldown_sec}s)"

        signal = self._build_signal(decision=decision, cfg=cfg, context_payload=context_payload)
        execution = {
            "mode": str(cfg.get("mode") or "shadow"),
            "submitted": False,
            "reason": "hold",
            "signal": signal.to_dict() if signal is not None else None,
        }
        if signal is not None:
            if str(cfg.get("mode") or "shadow") != "execute":
                execution["reason"] = "shadow_mode"
            else:
                trading_mode = execution_engine.get_trading_mode()
                if trading_mode == "live" and not bool(cfg.get("allow_live")):
                    execution["reason"] = "live_mode_blocked"
                else:
                    accepted = await execution_engine.submit_signal(signal)
                    execution["submitted"] = bool(accepted)
                    execution["reason"] = "submitted" if accepted else "submit_rejected"
                    if accepted:
                        self._submitted_count += 1
                        self._last_submit_at = time.time()

        latency_ms = int((time.perf_counter() - started) * 1000)
        now_iso = _utc_now().isoformat()
        # Derive rejection_reason for easy observability:
        #   "none" if submitted, otherwise the execution.reason string
        exec_reason = str(execution.get("reason") or "hold")
        rejection_reason = None if execution.get("submitted") else exec_reason
        journal_row = {
            "request_id": str(uuid.uuid4())[:8],
            "timestamp": now_iso,
            "trigger": str(trigger or "manual"),
            "latency_ms": latency_ms,
            "execution_allowed": bool(execution.get("submitted")),
            "rejection_reason": rejection_reason,
            "config": {
                "provider": cfg.get("provider"),
                "model": cfg.get("model"),
                "mode": cfg.get("mode"),
                "exchange": cfg.get("exchange"),
                "symbol": cfg.get("symbol"),
                "timeframe": cfg.get("timeframe"),
                "allow_live": cfg.get("allow_live"),
            },
            "context": {
                "price": context_payload.get("price"),
                "bars": context_payload.get("bars"),
                "returns": context_payload.get("returns"),
                "vol": context_payload.get("realized_vol_annualized"),
                "aggregated_signal": context_payload.get("aggregated_signal"),
                "position": context_payload.get("position"),
                "research_context": research_context,
            },
            "decision": decision,
            "execution": execution,
        }
        self._append_journal(journal_row)
        self._update_profile(decision, submitted=bool(execution.get("submitted")))

        self._last_run_at = now_iso
        self._last_error = None
        self._last_decision = decision
        self._last_execution = execution
        self._last_research_context = research_context if isinstance(research_context, dict) else None
        self._tick_count += 1

        return {
            "timestamp": now_iso,
            "trigger": str(trigger or "manual"),
            "latency_ms": latency_ms,
            "market_bars": int(len(market_data) if market_data is not None else 0),
            "decision": decision,
            "execution": execution,
            "status": self.get_status(),
        }


autonomous_trading_agent = AutonomousTradingAgent()
