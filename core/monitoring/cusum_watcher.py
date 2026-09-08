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


_NOTIFICATION_TASKS: set[asyncio.Task[Any]] = set()


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

    active = [c for c in all_candidates if str(c.status) in {"paper_running", "shadow_running", "live_candidate", "live_running"}]
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
                # Close the feedback loop: divergence report (decayed) +
                # counterfactual audit + advisory prior update (best-effort).
                _record_decay_feedback(app, cand, result, new_status)
                # Prefer a validated reserve candidate before cold-starting a
                # replacement research loop.
                reserve_activation = _activate_reserve_replacement(app, cand, result, all_candidates)
                if not reserve_activation.get("activated"):
                    _auto_draft_replacement(app, cand, result)
                report = {
                    "candidate_id": cand.candidate_id,
                    "strategy": strat_name or cand.strategy,
                    "previous_status": previous_status,
                    "new_status": new_status,
                    "reserve_activation": reserve_activation,
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
        loop = asyncio.get_running_loop()
        task = loop.create_task(
            notification_manager.send_message(
                title=title, message=message, channels=["feishu", "telegram"]
            )
        )
        _NOTIFICATION_TASKS.add(task)
        task.add_done_callback(_NOTIFICATION_TASKS.discard)
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


def _record_decay_feedback(
    app: FastAPI,
    candidate: Any,
    decay_result: Dict[str, Any],
    new_status: str,
) -> None:
    """Close the feedback loop on a CUSUM-detected decay (best-effort).

    1. Build a `decayed` PerformanceDivergenceReport.
    2. Append a gate counterfactual row (observed=demote, counterfactual=hold).
    3. Fold the divergence into the advisory family/regime prior file.

    Every step is independently guarded; this must never break the watcher.
    """
    try:
        from core.observability.gate_codes import GateCode
        from core.observability.score_calibration import update_family_regime_priors
        from core.research.performance_feedback import (
            build_performance_divergence_report,
            build_prior_update_from_divergence,
        )

        metadata = dict(getattr(candidate, "metadata", {}) or {})
        strategy_family = str(
            metadata.get("strategy_family") or metadata.get("decision_engine") or getattr(candidate, "strategy", "") or "unknown"
        ).strip() or "unknown"
        regime = str(
            metadata.get("research_mode") or (metadata.get("market_state") or {}).get("regime") or "mixed"
        ).strip().lower() or "mixed"

        report = None
        try:
            report = build_performance_divergence_report(
                candidate=candidate,
                snapshots=[],
                decay_state=decay_result,
            )
        except Exception as exc:
            logger.debug(f"cusum_watcher: divergence report failed: {exc}")

        try:
            from core.audit.gate_counterfactuals import record_gate_counterfactual

            trace = {
                "trace_id": f"cusum-{getattr(candidate, 'candidate_id', '')}-{utc_now().timestamp():.0f}",
                "subject_type": "candidate",
                "subject_id": str(getattr(candidate, "candidate_id", "") or ""),
                "root_blocker_code": GateCode.CUSUM_DECAY.value,
                "created_at": utc_now().isoformat(),
                "input_summary": {
                    "decay_pct": decay_result.get("decay_pct"),
                    "threshold": decay_result.get("threshold"),
                    "strategy_family": strategy_family,
                    "regime": regime,
                },
                "gates": [
                    {
                        "code": GateCode.CUSUM_DECAY.value,
                        "status": "demoted",
                        "counterfactual_decision": "hold",
                    }
                ],
            }
            record_gate_counterfactual(
                trace=trace,
                observed_decision=str(new_status or "demoted"),
                mode="paper_or_shadow",
            )
        except Exception as exc:
            logger.debug(f"cusum_watcher: counterfactual record failed: {exc}")

        try:
            if report is not None:
                update = build_prior_update_from_divergence(candidate=candidate, report=report)
            else:
                update = {
                    "strategy_family": strategy_family,
                    "regime": regime,
                    "symbol_scope": "global",
                    "divergence_score": 0.0,
                    "edge_delta": 0.0,
                    "status": "decayed",
                }
            update_family_regime_priors(
                [update]
            )
        except Exception as exc:
            logger.debug(f"cusum_watcher: prior update failed: {exc}")
    except Exception as exc:  # pragma: no cover - defensive outer guard
        logger.debug(f"cusum_watcher: decay feedback failed: {exc}")


def _candidate_symbol(candidate: Any) -> str:
    symbols = getattr(candidate, "symbols", None)
    if isinstance(symbols, list) and symbols:
        return str(symbols[0] or "").strip().upper()
    return str(getattr(candidate, "symbol", "") or "").strip().upper()


def _candidate_timeframe(candidate: Any) -> str:
    timeframes = getattr(candidate, "timeframes", None)
    if isinstance(timeframes, list) and timeframes:
        return str(timeframes[0] or "").strip()
    return str(getattr(candidate, "timeframe", "") or "").strip()


def _candidate_family(candidate: Any) -> str:
    meta = dict(getattr(candidate, "metadata", {}) or {})
    return str(
        meta.get("strategy_family")
        or meta.get("decision_engine")
        or getattr(candidate, "strategy", "")
        or ""
    ).strip()


def _is_reserve_candidate(candidate: Any) -> bool:
    meta = dict(getattr(candidate, "metadata", {}) or {})
    validation = getattr(candidate, "validation_summary", None)
    outcome_type = str(meta.get("outcome_type") or getattr(validation, "outcome_type", "") or "")
    return bool(meta.get("reserve_eligible") or getattr(validation, "reserve_eligible", False)) or outcome_type in {
        "redundant_correlated",
        "borderline",
    }


def _reserve_score(candidate: Any) -> float:
    validation = getattr(candidate, "validation_summary", None)
    meta = dict(getattr(candidate, "metadata", {}) or {})
    try:
        risk_adjusted = getattr(validation, "risk_adjusted_edge", None)
        if risk_adjusted is not None:
            return float(risk_adjusted or 0.0)
    except Exception:
        pass
    try:
        return float(getattr(validation, "deployment_score", None) or 0.0) / 100.0
    except Exception:
        return float(meta.get("radar_score") or 0.0)


def _activate_reserve_replacement(
    app: FastAPI,
    decayed_candidate: Any,
    decay_result: Dict[str, Any],
    all_candidates: List[Any],
) -> Dict[str, Any]:
    """Promote a previously rejected reserve into shadow observation."""
    try:
        from core.deployment.promotion_engine import transition_candidate

        decayed_id = str(getattr(decayed_candidate, "candidate_id", "") or "")
        decayed_symbol = _candidate_symbol(decayed_candidate)
        decayed_timeframe = _candidate_timeframe(decayed_candidate)
        decayed_family = _candidate_family(decayed_candidate)

        eligible: List[Any] = []
        for candidate in all_candidates or []:
            if str(getattr(candidate, "candidate_id", "") or "") == decayed_id:
                continue
            if str(getattr(candidate, "status", "") or "") not in {"new", "retired"}:
                continue
            if not _is_reserve_candidate(candidate):
                continue
            meta = dict(getattr(candidate, "metadata", {}) or {})
            correlated_with = str(meta.get("correlated_with") or "")
            same_symbol = bool(decayed_symbol and _candidate_symbol(candidate) == decayed_symbol)
            same_timeframe = bool(decayed_timeframe and _candidate_timeframe(candidate) == decayed_timeframe)
            same_family = bool(decayed_family and _candidate_family(candidate) == decayed_family)
            if not (correlated_with == decayed_id or same_symbol or (same_family and same_timeframe)):
                continue
            eligible.append(candidate)
        if not eligible:
            return {"activated": False, "reason": "no_matching_reserve_candidate"}

        reserve = sorted(eligible, key=_reserve_score, reverse=True)[0]
        meta = dict(getattr(reserve, "metadata", {}) or {})
        meta["reserve_activation_requested"] = True
        meta["reserve_activation_reason"] = "champion_cusum_decay"
        meta["replaces_candidate_id"] = decayed_id
        meta["activated_at"] = utc_now().isoformat()
        meta["decay_result"] = dict(decay_result or {})
        reserve.metadata = meta
        lifecycle_registry = getattr(getattr(app, "state", None), "ai_lifecycle_registry", None)
        if lifecycle_registry is not None:
            transition_candidate(
                reserve,
                to_state="shadow_running",
                lifecycle_registry=lifecycle_registry,
                actor="cusum_watcher",
                reason=f"reserve activated after CUSUM decay of {decayed_id}",
            )
        else:
            reserve.status = "shadow_running"
        registry = getattr(getattr(app, "state", None), "ai_candidate_registry", None)
        if registry is not None and hasattr(registry, "save"):
            registry.save(reserve)
        logger.info(
            "cusum_watcher: activated reserve candidate {} for decayed {}",
            getattr(reserve, "candidate_id", ""),
            decayed_id,
        )
        return {
            "activated": True,
            "candidate_id": str(getattr(reserve, "candidate_id", "") or ""),
            "status": str(getattr(reserve, "status", "") or ""),
            "score": _reserve_score(reserve),
        }
    except Exception as exc:
        logger.debug(f"cusum_watcher: reserve activation failed (non-fatal): {exc}")
        return {"activated": False, "reason": str(exc)}


def _auto_draft_replacement(app: Any, candidate: Any, decay_result: Dict[str, Any]) -> None:
    """Queue one bounded template research job per decayed candidate, without deployment."""
    try:
        from core.research.orchestrator import create_manual_proposal, ensure_ai_research_runtime_state
        from core.deployment.promotion_engine import transition_proposal

        ensure_ai_research_runtime_state(app)
        # This function contains no await; repeated watcher ticks cannot create
        # another proposal for the same candidate, even after a failed research run.
        for proposal in app.state.ai_proposal_registry.list(limit=None):
            meta = proposal.metadata or {}
            if meta.get("created_by") == "cusum_auto" and meta.get("parent_candidate_id") == candidate.candidate_id:
                return
        symbol = getattr(candidate, "symbol", None) or "BTC/USDT"
        timeframes = [getattr(candidate, "timeframe", None) or "15m"]
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
            source="rule",
            expected_holding_period="1d",
            risk_hypothesis="",
            invalidation_rules=[],
            required_features=[],
            parameter_space={},
            notes=[f"由 CUSUM 衰减自动生成，原候选: {candidate.candidate_id}"],
            metadata={
                "parent_candidate_id": candidate.candidate_id,
                "auto_generated": True,
                "last_research_request": {"symbol": symbol, "timeframes": timeframes, "days": 30},
            },
        )
        transition_proposal(
            new_proposal,
            to_state="research_queued",
            lifecycle_registry=app.state.ai_lifecycle_registry,
            actor="cusum_auto",
            reason="decay replacement queued for template backtesting",
        )
        app.state.ai_proposal_registry.save(new_proposal)
        logger.info(
            f"cusum_watcher: queued replacement proposal {new_proposal.proposal_id} "
            f"for {candidate.candidate_id}"
        )
    except Exception as exc:
        logger.debug(f"cusum_watcher: auto-draft failed (non-fatal): {exc}")
