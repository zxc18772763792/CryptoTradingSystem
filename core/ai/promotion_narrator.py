"""GLM-based promotion rationale generator for strategy candidates.

Generates a 2-3 sentence Chinese rationale explaining why a strategy
is or isn't recommended for promotion, based on its validation metrics.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from loguru import logger

_PROMPT_TEMPLATE = """\
你是专业量化研究员。请根据以下策略候选的回测验证结果，用2-3句简洁中文写出"促进推荐说明"。

策略：{strategy}（{symbol} {timeframe}，优化方式：{opt_method}）

核心指标：
- 综合评分 {score:.0f}/100  |  OOS夏普 {oos_sharpe}  |  最大回撤 {max_drawdown:.1f}%
- 胜率 {win_rate:.0f}%  |  交易次数 {total_trades}  |  成本拖累 {cost_drag:.2f}%

统计验证：
- DSR {dsr_pct}（多重检验修正）  |  WF稳定性 {wf_stability}  |  WF一致性 {wf_consistency}

促进决策：{decision}  |  原因：{reasons}

要求：①说明核心优势或主要风险 ②若被过滤解释原因 ③给出行动建议（≤80字）

返回JSON：{{"rationale": "..."}}"""


async def generate_promotion_rationale(
    candidate_dict: Dict[str, Any],
    validation_summary_dict: Dict[str, Any],
    timeout: int = 25,
) -> Optional[str]:
    """Call GLM to generate a Chinese promotion rationale.

    Returns the rationale string, or None on any failure (network, auth, parse).
    This function is intentionally best-effort — callers should never raise on None.
    """
    try:
        from core.news.eventizer.async_glm_client import AsyncGLMClient
    except Exception as e:
        logger.debug(f"promotion_narrator: GLM client unavailable: {e}")
        return None

    try:
        best = dict((candidate_dict.get("metadata") or {}).get("best") or {})
        vs = validation_summary_dict

        def _pct(v: Any) -> str:
            return f"{float(v) * 100:.0f}%" if v is not None else "—"

        def _f(v: Any, d: int = 2) -> str:
            return f"{float(v):.{d}f}" if v is not None else "—"

        prompt = _PROMPT_TEMPLATE.format(
            strategy=candidate_dict.get("strategy", "—"),
            symbol=candidate_dict.get("symbol", "—"),
            timeframe=candidate_dict.get("timeframe", "—"),
            opt_method=best.get("opt_method") or "none",
            score=float(candidate_dict.get("score", 0.0) or 0.0),
            oos_sharpe=_f(vs.get("oos_score")),
            max_drawdown=float(best.get("max_drawdown", 0.0) or 0.0),
            win_rate=float(best.get("win_rate", 0.0) or 0.0),
            total_trades=int(best.get("total_trades", 0) or 0),
            cost_drag=float(best.get("cost_drag_return_pct", 0.0) or 0.0),
            dsr_pct=_pct(vs.get("dsr_score")),
            wf_stability=_f(vs.get("wf_stability")),
            wf_consistency=_pct(vs.get("wf_consistency")),
            decision=vs.get("decision", "—"),
            reasons="; ".join((vs.get("reasons") or [])[:3]) or "无",
        )

        client = AsyncGLMClient()
        resp_data, err_type = await client.chat_completions(
            messages=[
                {"role": "system", "content": "你是专业量化研究员，严格按JSON格式返回结果。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            top_p=0.8,
            max_tokens=200,
            timeout=timeout,
        )

        if err_type != "none":
            logger.debug(f"promotion_narrator: GLM error={err_type}")
            return None

        content = ""
        try:
            choices = resp_data.get("choices") or []
            if choices:
                content = str(choices[0].get("message", {}).get("content", "") or "")
        except Exception:
            pass

        if not content:
            return None

        try:
            parsed = json.loads(content)
            rationale = str(parsed.get("rationale") or "").strip()
            return rationale or None
        except Exception:
            # fallback: return raw text truncated
            stripped = content.strip()
            return stripped[:200] if stripped else None

    except Exception as e:
        logger.debug(f"promotion_narrator: unexpected error: {e}")
        return None
