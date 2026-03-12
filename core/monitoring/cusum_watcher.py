"""CUSUM watcher: periodic scan of all running candidates for decay signals.

Runs every 5 minutes as a background asyncio task (started from web/main.py lifespan).
Detected decays trigger notifications and automatic demotion:
  paper_running  → shadow_running
  shadow_running → retired
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from loguru import logger

from core.utils import utc_now


async def run_cusum_checks_for_all_candidates(app: FastAPI) -> List[Dict[str, Any]]:
    """Scan all paper_running/shadow_running candidates for CUSUM decay.

    Returns a list of triggered-candidate dicts (empty if none triggered).
    """
    from core.monitoring.strategy_monitor import detect_strategy_decay
    from core.research.orchestrator import list_candidates
    from web.api.ai_research import _trades_to_returns

    triggered_reports: List[Dict[str, Any]] = []

    try:
        all_candidates = list_candidates(app, limit=200)
    except Exception as exc:
        logger.warning(f"cusum_watcher: could not list candidates: {exc}")
        return triggered_reports

    active = [c for c in all_candidates if str(c.status) in {"paper_running", "shadow_running"}]
    if not active:
        return triggered_reports

    # Pull trade history once
    all_trades: List[Dict[str, Any]] = []
    try:
        from core.risk.risk_manager import risk_manager
        all_trades = list(getattr(risk_manager, "_trade_history", []) or [])
    except Exception as exc:
        logger.debug(f"cusum_watcher: could not read risk_manager trade history: {exc}")

    for cand in active:
        try:
            strat_name: Optional[str] = (
                cand.metadata.get("registered_strategy_name")
                or cand.metadata.get("display_name")
                or cand.strategy
            )

            if strat_name and all_trades:
                filtered = [
                    t for t in all_trades
                    if t.get("strategy") == strat_name or t.get("strategy_name") == strat_name
                ]
            else:
                filtered = []

            returns = _trades_to_returns(filtered)
            result = detect_strategy_decay(returns)

            status_summary: Dict[str, Any] = {
                "triggered": result["triggered"],
                "n_bars": result["n_bars"],
                "decay_pct": result["decay_pct"],
                "threshold": result["threshold"],
                "message": result["message"],
                "checked_at": utc_now().isoformat(),
                "strategy_name_used": strat_name,
            }
            cand.metadata["cusum_status"] = status_summary

            if result["triggered"]:
                logger.warning(
                    f"cusum_watcher: decay detected for {cand.candidate_id} "
                    f"({strat_name}, status={cand.status}): {result['message']}"
                )
                # Send notification (best-effort)
                _send_cusum_notification(cand, result)

                # Capture status BEFORE demotion (transition_candidate modifies in-place)
                previous_status = str(cand.status)
                new_status = await _demote_on_decay(app, cand)
                # Auto-draft replacement proposal (best-effort, non-blocking)
                _auto_draft_replacement(app, cand, result)
                report = {
                    "candidate_id": cand.candidate_id,
                    "strategy": strat_name or cand.strategy,
                    "previous_status": previous_status,
                    "new_status": new_status,
                    "decay_pct": result["decay_pct"],
                    "message": result["message"],
                    "triggered_at": utc_now().isoformat(),
                }
                triggered_reports.append(report)
            # Single save — captures cusum_status + any status change from demotion
            app.state.ai_candidate_registry.save(cand)

        except Exception as exc:
            logger.debug(f"cusum_watcher: error checking candidate {cand.candidate_id}: {exc}")

    return triggered_reports


def _send_cusum_notification(cand: Any, decay_result: Dict[str, Any]) -> None:
    """Fire-and-forget notification for CUSUM decay (best-effort, non-blocking)."""
    try:
        from core.notifications import notification_manager
        title = f"⚠ 策略衰减告警: {cand.strategy}"
        message = (
            f"候选策略 {cand.candidate_id[:8]} ({cand.strategy}) "
            f"在 {cand.status} 状态下检测到 CUSUM 衰减。\n"
            f"衰减幅度: {decay_result.get('decay_pct', 0):.1f}%\n"
            f"{decay_result.get('message', '')}"
        )
        asyncio.create_task(
            notification_manager.send_message(
                title=title, message=message, channels=["feishu", "telegram"]
            )
        )
    except Exception as exc:
        logger.debug(f"cusum_watcher: notification failed (non-fatal): {exc}")


async def _demote_on_decay(app: FastAPI, candidate: Any) -> str:
    """Demote a triggered candidate one lifecycle step down.

    paper_running  → shadow_running
    shadow_running → retired

    Returns the new status string.
    """
    from core.deployment.promotion_engine import transition_candidate

    current = str(candidate.status)
    lifecycle_reg = app.state.ai_lifecycle_registry

    if current == "paper_running":
        # Stop the running strategy instance first (best-effort)
        strat_name = (
            candidate.metadata.get("promotion_runtime", {}).get("registered_strategy_name")
            or candidate.metadata.get("registered_strategy_name")
        )
        if strat_name:
            try:
                from core.strategies import strategy_manager as sm
                await sm.stop_strategy(strat_name)
                logger.info(f"cusum_watcher: stopped strategy {strat_name!r} for demotion")
            except Exception as exc:
                logger.debug(f"cusum_watcher: could not stop strategy {strat_name!r}: {exc}")

        target = "shadow_running"
        try:
            transition_candidate(
                candidate,
                to_state=target,
                lifecycle_registry=lifecycle_reg,
                actor="cusum_watcher",
                reason="CUSUM decay triggered → demoted paper→shadow",
            )
        except ValueError as exc:
            logger.warning(f"cusum_watcher: transition paper→shadow failed: {exc}; forcing retired")
            candidate.status = "retired"
            return "retired"
        return target

    elif current == "shadow_running":
        target = "retired"
        try:
            transition_candidate(
                candidate,
                to_state=target,
                lifecycle_registry=lifecycle_reg,
                actor="cusum_watcher",
                reason="CUSUM decay triggered again → retired",
            )
        except ValueError as exc:
            logger.warning(f"cusum_watcher: transition shadow→retired failed: {exc}; forcing retired")
            candidate.status = "retired"
        return target

    # Already retired or unknown — no further action
    return current


def _auto_draft_replacement(app: Any, candidate: Any, decay_result: Dict[str, Any]) -> None:
    """Auto-create a draft replacement proposal after decay demotion (best-effort, non-fatal)."""
    try:
        from core.research.orchestrator import create_manual_proposal  # noqa: PLC0415
        symbol = (getattr(candidate, "symbols", None) or ["BTC/USDT"])[0]
        timeframes = getattr(candidate, "timeframes", None) or ["15m", "1h"]
        decay_pct = decay_result.get("decay_pct", 0)
        thesis = (
            f"替代策略研究（自动生成）：{candidate.strategy} 在 {symbol} 上触发 CUSUM 衰减"
            f"（衰减幅度 {decay_pct:.1f}%），寻找替代方向。"
        )
        new_proposal = create_manual_proposal(
            app,
            actor="cusum_auto",
            thesis=thesis,
            symbols=[symbol],
            timeframes=timeframes,
            market_regime="mixed",
            strategy_templates=[],
            source="cusum_auto",
            expected_holding_period="1d",
            risk_hypothesis="",
            invalidation_rules=[],
            required_features=[],
            parameter_space={},
            notes=[f"由 CUSUM 衰减自动生成，原候选: {candidate.candidate_id}"],
            metadata={"parent_candidate_id": candidate.candidate_id, "auto_generated": True},
        )
        logger.info(
            f"cusum_watcher: auto-drafted replacement proposal {new_proposal.proposal_id} "
            f"for {candidate.candidate_id}"
        )
    except Exception as exc:
        logger.debug(f"cusum_watcher: auto-draft failed (non-fatal): {exc}")
