"""OpenAI Responses-based research context generator for the AI workbench.

Produces an ``LLMResearchOutput``-compatible payload and, when possible,
returns executable open-ended strategy drafts under
``proposed_strategy_changes`` so the planner can enter hybrid or
autonomous-draft research instead of staying template-only.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

from config.settings import settings
from core.utils.openai_responses import (
    build_openai_headers,
    build_responses_payload,
    extract_response_text,
    openai_endpoint_targets,
    prioritize_openai_targets,
    read_aiohttp_responses_json,
    remember_openai_target_failure,
    remember_openai_target_success,
    responses_endpoint,
    should_failover_openai_status,
)


_DEFAULT_OPENAI_BASE_URL = "https://sub.a-j.app/v1"
_DEFAULT_OPENAI_MODEL = "gpt-5.4"

_CONTEXT_SYSTEM_PROMPT = """You are a quantitative research planner.
Your goal is not to emit direct trading instructions. Your goal is to produce
testable hypotheses, an experiment plan, and executable strategy research drafts.

Constraints:
1. Return JSON only, without markdown or extra commentary.
2. In `proposed_strategy_changes`, provide 2-4 drafts when possible; provide at least 1 draft when context is limited.
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
    targets = prioritize_openai_targets(
        openai_endpoint_targets(
            primary_base_url=str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL),
            backup_base_urls=getattr(settings, "OPENAI_BACKUP_BASE_URL", "") or "",
            primary_api_key=str(getattr(settings, "OPENAI_API_KEY", "") or "").strip(),
            backup_api_key=str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip(),
        )
    )
    if not any(bool(str(target.get("api_key") or "").strip()) for target in targets):
        logger.debug("research_context_generator: OPENAI_API_KEY missing")
        return None

    model = str(getattr(settings, "OPENAI_MODEL", "") or _DEFAULT_OPENAI_MODEL).strip() or _DEFAULT_OPENAI_MODEL
    payload = build_responses_payload(
        model=model,
        messages=[
            {"role": "system", "content": _CONTEXT_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        max_output_tokens=2400,
        temperature=0.2,
        text_format="json_object",
        stream=False,
    )
    timeout_cfg = aiohttp.ClientTimeout(total=max(5, int(timeout)))

    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        total_targets = len(targets)
        for idx, target in enumerate(targets):
            base_url = str(target.get("base_url") or "").rstrip("/")
            api_key = str(target.get("api_key") or "").strip()
            if not base_url or not api_key:
                continue
            url = responses_endpoint(base_url)
            try:
                async with session.post(url, headers=build_openai_headers(api_key), json=payload) as resp:
                    if resp.status >= 400:
                        body = (await resp.text())[:400]
                        logger.debug(f"research_context_generator: openai_http_{resp.status}:{body}")
                        if should_failover_openai_status(resp.status):
                            remember_openai_target_failure(targets, base_url)
                        if idx + 1 < total_targets and should_failover_openai_status(resp.status):
                            logger.warning(
                                f"research_context_generator: primary relay failed with {resp.status}; "
                                f"trying backup {idx + 2}/{total_targets}"
                            )
                            continue
                        return None
                    data = await read_aiohttp_responses_json(resp)
                    remember_openai_target_success(targets, base_url)
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                logger.debug(f"research_context_generator: openai transport error: {exc}")
                remember_openai_target_failure(targets, base_url)
                if idx + 1 < total_targets:
                    logger.warning(
                        f"research_context_generator: primary relay transport failure; "
                        f"trying backup {idx + 2}/{total_targets}"
                    )
                    continue
                return None

            raw = extract_response_text(data)
            if not raw:
                logger.debug("research_context_generator: empty OpenAI content")
                remember_openai_target_failure(targets, base_url)
                if idx + 1 < total_targets:
                    continue
                return None
            try:
                return _parse_json_payload(raw)
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"research_context_generator: JSON parse error: {exc}")
                remember_openai_target_failure(targets, base_url)
                if idx + 1 < total_targets:
                    continue
                return None
    return None


async def generate_research_context(
    market_summary: Dict[str, Any],
    goals: str = "",
    timeout: int = 180,
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
            return None
        payload = _fill_defaults(parsed)
        missing = _REQUIRED_KEYS - set(payload.keys())
        if missing:
            logger.debug(f"research_context_generator: missing keys after fill: {missing}")
        return payload
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"research_context_generator: unexpected error: {exc}")
        return None
