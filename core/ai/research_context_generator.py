"""OpenAI Responses-based research context generator for the AI workbench.

Produces an ``LLMResearchOutput``-compatible payload and, when possible,
returns executable open-ended strategy drafts under
``proposed_strategy_changes`` so the planner can enter hybrid or
autonomous-draft research instead of staying template-only.
"""
from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

from config.settings import settings
from core.governance.schemas import LLMResearchOutput
from core.utils.openai_responses import (
    anthropic_messages_endpoint,
    build_anthropic_messages_payload,
    build_target_headers,
    build_chat_completions_payload,
    build_responses_payload,
    chat_completions_endpoint,
    extract_response_text,
    openai_endpoint_targets,
    prioritize_openai_targets,
    read_aiohttp_responses_json,
    remember_openai_target_failure,
    remember_openai_target_success,
    responses_endpoint,
    responses_api_unavailable,
    should_failover_openai_status,
    target_transport,
)


_DEFAULT_OPENAI_BASE_URL = "https://nowcoding.ai/v1"
_DEFAULT_OPENAI_MODEL = "gpt-5.6-sol"
_OPENAI_FAILOVER_SCOPE = "ai_research"
_last_generation_error = ContextVar('research_generation_error', default=None)
_generation_errors = ContextVar('research_generation_errors', default=())


class ResearchGenerationError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _set_generation_error(error):
    _last_generation_error.set(error)
    _generation_errors.set((*_generation_errors.get(), error))


def _record_provider_error(status, body):
    """Classify upstream failures without persisting balances, IDs or raw bodies."""
    text = str(body or '').lower()
    if any(token in text for token in ('insufficient_quota', 'insufficient_user_quota', 'quota_exceeded', '余额不足', '额度不足')):
        error = ('provider_quota_exhausted', '研究模型通道额度不足，请在对应服务商补充额度或配置可用备用通道')
    elif status == 401:
        error = ('provider_authentication', '研究模型通道认证失败，请检查凭据配置')
    elif status == 429:
        error = ('provider_rate_limited', '研究模型通道限流，将按间隔重试')
    else:
        error = (f'provider_http_{status}', f'研究模型接口返回 HTTP {status}')
    _set_generation_error(error)
    logger.debug(f'research_context_generator: {error[0]} (HTTP {status})')

_CONTEXT_SYSTEM_PROMPT = """You are a quantitative research planner.
Your goal is not to emit direct trading instructions. Your goal is to produce
testable hypotheses, an experiment plan, and executable strategy research drafts.

Constraints:
1. Return JSON only, without markdown or extra commentary.
2. In `proposed_strategy_changes`, provide 1-3 compact drafts; provide at least 1 draft when context is limited.
3. `program` should be executable and only use indicator kinds:
   price / sma / ema / rsi / zscore / returns
4. `program.entry_conditions` and `program.exit_conditions` can only use:
   gt / gte / lt / lte / cross_over / cross_under
5. Do not include direct order placement instructions, leverage commands, or explicit long/short execution wording.
6. If a draft is close to an existing template, set `strategy` to the template name.
   If it is open-ended, leave `strategy` empty and rely on `program`.
"""

_CONTEXT_PROMPT_TEMPLATE = """Based on the market summary and goals below, produce a structured quant research plan.

Market summary:
{market_summary}

Research goals:
{goals}

Return JSON with these required fields:
{{
  "hypothesis": "single-sentence research hypothesis",
  "experiment_plan": ["step 1", "step 2", "step 3"],
  "metrics_to_check": ["metric 1", "metric 2", "metric 3"],
  "expected_failure_modes": ["failure mode 1", "failure mode 2"],
  "proposed_strategy_changes": [
    {{
      "draft_id": "draft-01",
      "name": "draft name",
      "strategy": "optional template name, or empty string",
      "thesis": "core idea",
      "rationale": "why this is worth testing",
      "features": ["ema_fast", "ema_slow", "rsi"],
      "entry_logic": ["cross_over(ema_fast, ema_slow)", "rsi <= 35"],
      "exit_logic": ["cross_under(ema_fast, ema_slow)", "rsi >= 60"],
      "risk_logic": ["reduce priority under volatility expansion"],
      "params": {{"fast_period": 8, "slow_period": 21, "rsi_period": 14}},
      "program": {{
        "name": "draft name",
        "indicators": [
          {{"name": "ema_fast", "kind": "ema", "period": 8}},
          {{"name": "ema_slow", "kind": "ema", "period": 21}},
          {{"name": "rsi", "kind": "rsi", "period": 14}}
        ],
        "entry_conditions": [
          {{"left": "ema_fast", "op": "cross_over", "right": "ema_slow"}},
          {{"left": "rsi", "op": "lte", "right": 35}}
        ],
        "exit_conditions": [
          {{"left": "ema_fast", "op": "cross_under", "right": "ema_slow"}},
          {{"left": "rsi", "op": "gte", "right": 60}}
        ],
        "parameter_space": {{
          "fast_period": [5, 8, 12],
          "slow_period": [21, 34, 55]
        }}
      }},
      "confidence": 0.62,
      "source": "openai_context"
    }}
  ],
  "uncertainty": "low|medium|high",
  "evidence_refs": ["evidence 1", "evidence 2"]
}}

Requirements:
1. Keep `experiment_plan` to 3-5 items.
2. Keep `metrics_to_check` to 3-5 items.
3. Keep `expected_failure_modes` to 1-3 items.
4. At least one item in `proposed_strategy_changes` must include `program`.
5. If open-ended exploration is more suitable than a fixed template, prioritize open-ended drafts in `proposed_strategy_changes`.
"""

_REQUIRED_KEYS = {
    "hypothesis",
    "experiment_plan",
    "metrics_to_check",
    "expected_failure_modes",
    "proposed_strategy_changes",
    "uncertainty",
    "evidence_refs",
}

_DEFAULTS: Dict[str, Any] = {
    "hypothesis": "",
    "experiment_plan": [],
    "metrics_to_check": [],
    "expected_failure_modes": [],
    "proposed_strategy_changes": [],
    "uncertainty": "medium",
    "evidence_refs": [],
}


def _format_market_summary(market_summary: Dict[str, Any]) -> str:
    if not market_summary:
        return "(no market data)"
    lines = []
    for key, val in market_summary.items():
        if isinstance(val, dict):
            inner = ", ".join(f"{k}={v}" for k, v in val.items())
            lines.append(f"  {key}: {inner}")
        elif isinstance(val, list):
            preview = ", ".join(str(x) for x in val[:5])
            lines.append(f"  {key}: {preview}")
        else:
            lines.append(f"  {key}: {val}")
    return "\n".join(lines) if lines else "(no market data)"


def _strip_code_fences(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
    return text.strip()


def _parse_json_payload(raw: str) -> Dict[str, Any]:
    text = _strip_code_fences(raw)
    if not text:
        raise ValueError("empty_response")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("json_not_object")
    return parsed


def _fill_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    for key, default in _DEFAULTS.items():
        if key not in out:
            out[key] = default
    for key in ("experiment_plan", "metrics_to_check", "expected_failure_modes", "evidence_refs"):
        if not isinstance(out.get(key), list):
            out[key] = [str(out.get(key) or "").strip()] if str(out.get(key) or "").strip() else []
    if not isinstance(out.get("proposed_strategy_changes"), list):
        out["proposed_strategy_changes"] = []
    out["hypothesis"] = str(out.get("hypothesis") or "").strip()
    out["uncertainty"] = str(out.get("uncertainty") or "medium").strip() or "medium"
    return out


async def _call_openai_responses_json(prompt: str, *, timeout: int) -> Optional[Dict[str, Any]]:
    _last_generation_error.set(None)
    _generation_errors.set(())
    targets = prioritize_openai_targets(
        openai_endpoint_targets(
            primary_base_url=str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL),
            backup_base_urls=getattr(settings, "OPENAI_BACKUP_BASE_URL", "") or "",
            primary_api_key=str(getattr(settings, "OPENAI_API_KEY", "") or "").strip(),
            backup_api_key=str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip(),
            primary_model=str(settings.AI_RESEARCH_MODEL or _DEFAULT_OPENAI_MODEL),
            backup_model=str(settings.AI_RESEARCH_BACKUP_MODEL or settings.AI_RESEARCH_MODEL),
        ),
        scope=_OPENAI_FAILOVER_SCOPE,
    )
    if not any(bool(str(target.get("api_key") or "").strip()) for target in targets):
        _set_generation_error(('credentials_missing', '研究模型未配置凭据'))
        logger.debug("research_context_generator: OPENAI_API_KEY missing")
        return None

    model = str(settings.AI_RESEARCH_MODEL or _DEFAULT_OPENAI_MODEL)
    messages = [
        {"role": "system", "content": _CONTEXT_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    payload = build_responses_payload(
        model=model,
        messages=messages,
        max_output_tokens=6000,
        reasoning_effort="low",
        temperature=0.2,
        text_format="json_object",
        stream=True,
    )
    chat_payload = build_chat_completions_payload(
        model=model,
        messages=messages,
        max_tokens=6000,
        temperature=0.2,
        response_format={"type": "json_object"},
        stream=False,
    )
    anthropic_payload = build_anthropic_messages_payload(
        model=model,
        messages=messages,
        max_tokens=2400,
        temperature=0.2,
    )
    timeout_cfg = aiohttp.ClientTimeout(total=max(5, int(timeout)))

    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        total_targets = len(targets)
        for idx, target in enumerate(targets):
            base_url = str(target.get("base_url") or "").rstrip("/")
            api_key = str(target.get("api_key") or "").strip()
            target_model = str(target.get("model") or model or "").strip() or model
            if not base_url or not api_key:
                continue
            transport = target_transport(target)
            request_payload = dict(payload, model=target_model)
            request_chat_payload = dict(chat_payload, model=target_model)
            request_anthropic_payload = dict(anthropic_payload, model=target_model)
            headers = build_target_headers({**dict(target), "api_key": api_key})
            try:
                if transport == "anthropic":
                    url = anthropic_messages_endpoint(base_url)
                    async with session.post(url, headers=headers, json=request_anthropic_payload) as resp:
                        if resp.status >= 400:
                            body = (await resp.text())[:400]
                            _record_provider_error(resp.status, body)
                            if should_failover_openai_status(resp.status):
                                remember_openai_target_failure(
                                    targets,
                                    base_url,
                                    scope=_OPENAI_FAILOVER_SCOPE,
                                )
                            if idx + 1 < total_targets and should_failover_openai_status(resp.status):
                                logger.warning(
                                    "research_context_generator: anthropic-style backup failed with "
                                    f"{resp.status}; trying backup {idx + 2}/{total_targets}"
                                )
                                continue
                            return None
                        data = await read_aiohttp_responses_json(resp)
                        remember_openai_target_success(
                            targets,
                            base_url,
                            scope=_OPENAI_FAILOVER_SCOPE,
                        )
                else:
                    url = responses_endpoint(base_url)
                    async with session.post(url, headers=headers, json=request_payload) as resp:
                        if resp.status >= 400:
                            body = (await resp.text())[:400]
                            _record_provider_error(resp.status, body)
                            if responses_api_unavailable(resp.status, body):
                                logger.warning(
                                    "research_context_generator: relay does not support Responses API; "
                                    "retrying via chat/completions"
                                )
                                chat_url = chat_completions_endpoint(base_url)
                                async with session.post(
                                    chat_url,
                                    headers=headers,
                                    json=request_chat_payload,
                                ) as chat_resp:
                                    if chat_resp.status >= 400:
                                        chat_body = (await chat_resp.text())[:400]
                                        _record_provider_error(chat_resp.status, chat_body)
                                        if should_failover_openai_status(chat_resp.status):
                                            remember_openai_target_failure(
                                                targets,
                                                base_url,
                                                scope=_OPENAI_FAILOVER_SCOPE,
                                            )
                                        if idx + 1 < total_targets and should_failover_openai_status(chat_resp.status):
                                            logger.warning(
                                                "research_context_generator: chat/completions relay failed with "
                                                f"{chat_resp.status}; trying backup {idx + 2}/{total_targets}"
                                            )
                                            continue
                                        return None
                                    data = await read_aiohttp_responses_json(chat_resp)
                                raw = extract_response_text(data)
                                if not raw:
                                    _set_generation_error(('empty_model_output', '研究模型返回空内容'))
                                    logger.debug("research_context_generator: empty OpenAI chat/completions content")
                                    remember_openai_target_failure(
                                        targets,
                                        base_url,
                                        scope=_OPENAI_FAILOVER_SCOPE,
                                    )
                                    if idx + 1 < total_targets:
                                        continue
                                    return None
                                remember_openai_target_success(
                                    targets,
                                    base_url,
                                    scope=_OPENAI_FAILOVER_SCOPE,
                                )
                                try:
                                    return {**_parse_json_payload(raw), "_generation": {"requested_model": target_model, "response_model": data.get("model"), "usage": data.get("usage", {})}}
                                except Exception as exc:
                                    _set_generation_error(('invalid_model_json', '研究模型输出不是有效 JSON'))
                                    logger.debug(f"research_context_generator: failed to parse chat payload: {exc}")
                                    return None
                            if should_failover_openai_status(resp.status):
                                remember_openai_target_failure(
                                    targets,
                                    base_url,
                                    scope=_OPENAI_FAILOVER_SCOPE,
                                )
                            if idx + 1 < total_targets and should_failover_openai_status(resp.status):
                                logger.warning(
                                    f"research_context_generator: primary relay failed with {resp.status}; "
                                    f"trying backup {idx + 2}/{total_targets}"
                                )
                                continue
                            return None
                        data = await read_aiohttp_responses_json(resp)
                        remember_openai_target_success(
                            targets,
                            base_url,
                            scope=_OPENAI_FAILOVER_SCOPE,
                        )
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                _set_generation_error(('provider_timeout' if isinstance(exc, asyncio.TimeoutError) else 'provider_connection', '研究模型请求超时或连接失败'))
                logger.debug(f"research_context_generator: openai transport error: {exc}")
                remember_openai_target_failure(
                    targets,
                    base_url,
                    scope=_OPENAI_FAILOVER_SCOPE,
                )
                if idx + 1 < total_targets:
                    logger.warning(
                        f"research_context_generator: primary relay transport failure; "
                        f"trying backup {idx + 2}/{total_targets}"
                    )
                    continue
                return None

            raw = extract_response_text(data)
            if not raw:
                _set_generation_error(('empty_model_output', '研究模型返回空内容'))
                logger.debug("research_context_generator: empty OpenAI content")
                remember_openai_target_failure(
                    targets,
                    base_url,
                    scope=_OPENAI_FAILOVER_SCOPE,
                )
                if idx + 1 < total_targets:
                    continue
                return None
            try:
                return {**_parse_json_payload(raw), "_generation": {"requested_model": target_model, "response_model": data.get("model"), "usage": data.get("usage", {})}}
            except Exception as exc:  # noqa: BLE001
                _set_generation_error(('invalid_model_json', '研究模型输出不是有效 JSON'))
                logger.debug(f"research_context_generator: JSON parse error: {exc}")
                remember_openai_target_failure(
                    targets,
                    base_url,
                    scope=_OPENAI_FAILOVER_SCOPE,
                )
                if idx + 1 < total_targets:
                    continue
                return None
    return None


async def generate_research_context(
    market_summary: Dict[str, Any],
    goals: str = "",
    timeout: int = 180,
    *,
    raise_on_error: bool = False,
) -> Optional[Dict[str, Any]]:
    """Generate a structured research context for the AI research workbench."""

    try:
        market_str = _format_market_summary(market_summary)
        goals_str = str(goals).strip() or "Improve risk-adjusted returns while reducing max drawdown."
        prompt = _CONTEXT_PROMPT_TEMPLATE.format(
            market_summary=market_str,
            goals=goals_str,
        )
        parsed = await _call_openai_responses_json(prompt, timeout=timeout)
        if not isinstance(parsed, dict):
            if raise_on_error:
                code, message = _last_generation_error.get() or ('provider_unavailable', '研究模型未返回可用内容')
                failures = list(dict.fromkeys(message for _, message in _generation_errors.get()))
                if failures:
                    message = '；'.join(failures[-3:])
                raise ResearchGenerationError(code, message)
            return None
        payload = _fill_defaults(parsed)
        missing = _REQUIRED_KEYS - set(payload.keys())
        if missing:
            logger.debug(f"research_context_generator: missing keys after fill: {missing}")
        validated = LLMResearchOutput.model_validate(payload).model_dump(mode="json")
        if parsed.get("_generation"):
            validated["_generation"] = parsed["_generation"]
        return validated
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"research_context_generator: unexpected error: {exc}")
        if raise_on_error:
            if isinstance(exc, ResearchGenerationError):
                raise
            code = 'invalid_model_schema' if isinstance(exc, ValueError) else 'implementation_error'
            raise ResearchGenerationError(code, f'研究输出校验或调用失败（{type(exc).__name__}）') from exc
        return None
