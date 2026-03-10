"""LLM-assisted research context generator for AI research workbench.

Calls GLM to produce a structured research hypothesis + experiment plan
based on a market summary snapshot and a user-defined goal string.
The returned dict is compatible with the LLMResearchOutput schema used
in ResearchProposal.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from loguru import logger

_CONTEXT_PROMPT_TEMPLATE = """\
你是专业量化研究员。根据以下市场摘要，生成一个量化策略研究方向。

市场摘要：
{market_summary}

研究目标：{goals}

要求：
1. 明确提出可验证的量化假设
2. 给出具体回测/研究步骤（3-5条）
3. 列出需要检查的关键指标（3-5项）
4. 指出潜在失效场景（1-3条）
5. 评估研究不确定性（高/中/低）

返回JSON（只返回JSON，不要额外文字）:
{{
  "hypothesis": "...",
  "experiment_plan": ["步骤1", "步骤2", ...],
  "metrics_to_check": ["指标1", "指标2", ...],
  "expected_failure_modes": ["场景1", "场景2", ...],
  "proposed_strategy_changes": [],
  "uncertainty": "高|中|低",
  "evidence_refs": []
}}"""

_REQUIRED_KEYS = {
    "hypothesis",
    "experiment_plan",
    "metrics_to_check",
    "expected_failure_modes",
    "proposed_strategy_changes",
    "uncertainty",
    "evidence_refs",
}


def _format_market_summary(market_summary: Dict[str, Any]) -> str:
    """Convert a market summary dict to a readable Chinese string for the prompt."""
    if not market_summary:
        return "（无市场数据）"
    lines = []
    for key, val in market_summary.items():
        if isinstance(val, dict):
            inner = ", ".join(f"{k}={v}" for k, v in val.items())
            lines.append(f"  {key}: {inner}")
        elif isinstance(val, list):
            lines.append(f"  {key}: {', '.join(str(x) for x in val[:5])}")
        else:
            lines.append(f"  {key}: {val}")
    return "\n".join(lines) if lines else "（无市场数据）"


async def generate_research_context(
    market_summary: Dict[str, Any],
    goals: str = "",
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:
    """Call GLM to generate an LLMResearchOutput-compatible research context.

    Parameters
    ----------
    market_summary:
        Snapshot of current market signals/state (e.g., from signal_aggregator).
    goals:
        Free-text research goal provided by the user.
    timeout:
        Request timeout in seconds (5–90).

    Returns
    -------
    Dict matching LLMResearchOutput fields, or None on any failure.
    """
    try:
        from core.news.eventizer.async_glm_client import AsyncGLMClient
    except Exception as exc:
        logger.debug(f"research_context_generator: GLM client unavailable: {exc}")
        return None

    try:
        market_str = _format_market_summary(market_summary)
        goals_str = str(goals).strip() or "提升策略收益风险比，降低最大回撤"
        prompt = _CONTEXT_PROMPT_TEMPLATE.format(
            market_summary=market_str,
            goals=goals_str,
        )

        client = AsyncGLMClient()
        raw: str = await client.chat_completions(
            messages=[{"role": "user", "content": prompt}],
            timeout=timeout,
        )

        # Strip markdown code fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[: raw.rfind("```")]

        parsed: Dict[str, Any] = json.loads(raw.strip())

        # Validate required keys
        missing = _REQUIRED_KEYS - set(parsed.keys())
        if missing:
            logger.debug(f"research_context_generator: LLM response missing keys: {missing}")
            # Fill defaults for missing keys
            defaults: Dict[str, Any] = {
                "hypothesis": "",
                "experiment_plan": [],
                "metrics_to_check": [],
                "expected_failure_modes": [],
                "proposed_strategy_changes": [],
                "uncertainty": "中",
                "evidence_refs": [],
            }
            for k in missing:
                parsed[k] = defaults.get(k, "")

        return parsed

    except json.JSONDecodeError as exc:
        logger.debug(f"research_context_generator: JSON parse error: {exc}")
        return None
    except Exception as exc:
        logger.debug(f"research_context_generator: unexpected error: {exc}")
        return None
