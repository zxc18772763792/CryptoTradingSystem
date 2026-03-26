"""OpenAI Responses-based research context generator for the AI workbench.

Produces an ``LLMResearchOutput``-compatible payload and, when possible,
returns executable open-ended strategy drafts under
``proposed_strategy_changes`` so the planner can enter hybrid or
autonomous-draft research instead of staying template-only.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

from config.settings import settings
from core.utils.openai_responses import (
    build_openai_headers,
    build_responses_payload,
    extract_response_text,
    read_aiohttp_responses_json,
    responses_endpoint,
)


_DEFAULT_OPENAI_BASE_URL = "https://vpsairobot.com/v1"
_DEFAULT_OPENAI_MODEL = "gpt-5.4"

_CONTEXT_SYSTEM_PROMPT = """你是专业的量化研究规划器。

你的目标不是给出直接交易指令，而是生成可验证的研究假设、实验计划，以及可执行的策略研究草案。

约束：
1. 只返回 JSON，不要返回任何额外说明。
2. proposed_strategy_changes 尽量给出 2-4 个候选草案；如果信息不足，至少给出 1 个。
3. 草案里的 program 必须尽量可执行，并只使用以下指标类型：
   price / sma / ema / rsi / zscore / returns
4. program.entry_conditions / exit_conditions 只使用以下操作：
   gt / gte / lt / lte / cross_over / cross_under
5. 不要包含直接下单、杠杆、市价、限价、long、short 等交易指令词。
6. 如果某个草案接近现有模板，可在 strategy 字段填写模板名；如果是开放式草案，可留空并仅给 program。
"""

_CONTEXT_PROMPT_TEMPLATE = """根据以下市场摘要和研究目标，生成一个结构化量化研究方案。

市场摘要：
{market_summary}

研究目标：
{goals}

请返回 JSON，字段必须完整：
{{
  "hypothesis": "一句话研究假设",
  "experiment_plan": ["步骤1", "步骤2", "步骤3"],
  "metrics_to_check": ["指标1", "指标2", "指标3"],
  "expected_failure_modes": ["失效场景1", "失效场景2"],
  "proposed_strategy_changes": [
    {{
      "draft_id": "draft-01",
      "name": "草案名称",
      "strategy": "可选，接近的模板名；否则留空",
      "thesis": "该草案的核心假设",
      "rationale": "为什么值得研究",
      "features": ["ema_fast", "ema_slow", "rsi"],
      "entry_logic": ["cross_over(ema_fast, ema_slow)", "rsi <= 35"],
      "exit_logic": ["cross_under(ema_fast, ema_slow)", "rsi >= 60"],
      "risk_logic": ["波动放大时收紧研究优先级"],
      "params": {{"fast_period": 8, "slow_period": 21, "rsi_period": 14}},
      "program": {{
        "name": "草案名称",
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
  "uncertainty": "高|中|低",
  "evidence_refs": ["证据1", "证据2"]
}}

要求：
1. experiment_plan 保持 3-5 条。
2. metrics_to_check 保持 3-5 项。
3. expected_failure_modes 保持 1-3 条。
4. proposed_strategy_changes 中至少 1 个草案必须带 program。
5. 如果目标更适合开放式草案研究，请优先让 proposed_strategy_changes 不完全依赖固定模板。
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
    "uncertainty": "中",
    "evidence_refs": [],
}


def _format_market_summary(market_summary: Dict[str, Any]) -> str:
    if not market_summary:
        return "（无市场数据）"
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
    return "\n".join(lines) if lines else "（无市场数据）"


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
    out["uncertainty"] = str(out.get("uncertainty") or "中").strip() or "中"
    return out


async def _call_openai_responses_json(prompt: str, *, timeout: int) -> Optional[Dict[str, Any]]:
    api_key = str(getattr(settings, "OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        logger.debug("research_context_generator: OPENAI_API_KEY missing")
        return None

    base_url = str(getattr(settings, "OPENAI_BASE_URL", "") or _DEFAULT_OPENAI_BASE_URL).rstrip("/")
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
    url = responses_endpoint(base_url)

    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        async with session.post(url, headers=build_openai_headers(api_key), json=payload) as resp:
            if resp.status >= 400:
                body = (await resp.text())[:400]
                logger.debug(f"research_context_generator: openai_http_{resp.status}:{body}")
                return None
            data = await read_aiohttp_responses_json(resp)

    raw = extract_response_text(data)
    if not raw:
        logger.debug("research_context_generator: empty OpenAI content")
        return None
    try:
        return _parse_json_payload(raw)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"research_context_generator: JSON parse error: {exc}")
        return None


async def generate_research_context(
    market_summary: Dict[str, Any],
    goals: str = "",
    timeout: int = 180,
) -> Optional[Dict[str, Any]]:
    """Generate a structured research context for the AI research workbench."""

    try:
        market_str = _format_market_summary(market_summary)
        goals_str = str(goals).strip() or "提升策略收益风险比，降低最大回撤"
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
