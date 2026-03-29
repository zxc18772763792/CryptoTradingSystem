"""Shared AI research orchestration service for Ops API and Web API."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

from config.settings import settings
from config.strategy_registry import get_strategy_registry_entry
from core.ai.proposal_schemas import ResearchProposal
from core.backtest.common_pnl import build_common_pnl_summary
from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
from core.deployment.promotion_engine import (
    promote_candidate,
    record_lifecycle,
    transition_proposal,
)
from core.governance.rbac import GovernanceIdentity
from core.governance.service import propose_strategy as governance_propose_strategy
from core.research.experiment_registry import (
    CandidateRegistry,
    ExperimentRegistry,
    ExperimentRunRegistry,
    LifecycleRegistry,
    ProposalRegistry,
)
from core.research.experiment_schemas import ExperimentRun, ExperimentSpec, StrategyCandidate
from core.research.strategy_program import program_strategy_name
from core.research.strategy_research import ResearchConfig, run_strategy_research
from core.research.validation_gate import (
    build_promotion_decision,
    build_validation_summary_from_research_result,
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in values or []:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        return raw
    if "_" in raw:
        left, right = raw.split("_", 1)
        return f"{left}/{right}"
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def normalize_timeframes(values: List[str]) -> List[str]:
    cleaned = _dedupe_keep_order([str(item or "").strip() for item in values or []])
    return cleaned or ["5m", "15m", "1h"]


def _load_research_jobs(path: Path) -> Dict[str, Dict[str, Any]]:
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        jobs: Dict[str, Dict[str, Any]] = {}
        for key, value in payload.items():
            if isinstance(value, dict):
                jobs[str(key)] = dict(value)
        return jobs
    except Exception:
        return {}


def _persist_research_jobs(app: FastAPI) -> None:
    try:
        path = Path(getattr(app.state, "ai_research_jobs_path"))
        path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(getattr(app.state, "research_jobs", {}) or {})
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # best-effort: do not break the main research flow
        pass


def _compact_thesis(text: str, max_len: int = 24) -> str:
    normalized = " ".join(str(text or "").strip().split())
    if len(normalized) <= max_len:
        return normalized
    return f"{normalized[:max_len].rstrip()}..."


def _next_proposal_identity(app: FastAPI, thesis: str) -> tuple[str, str, int, str]:
    now = _now_utc()
    day_key = now.strftime("%Y%m%d")
    pattern = re.compile(rf"^proposal-{day_key}-(\d{{3,}})$")
    max_seq = 0
    for item in app.state.ai_proposal_registry.list(limit=None):
        matched = pattern.match(str(item.proposal_id or ""))
        if not matched:
            continue
        try:
            seq = int(matched.group(1))
            max_seq = max(max_seq, seq)
        except Exception:
            continue
    next_seq = max_seq + 1
    proposal_id = f"proposal-{day_key}-{next_seq:03d}"
    title = _compact_thesis(thesis)
    display_name = f"{now.strftime('%m-%d %H:%M')} #{next_seq:03d} {title}".strip()
    return proposal_id, display_name, next_seq, day_key


def _filter_supported_research_strategies(strategies: List[str]) -> tuple[List[str], List[str]]:
    try:
        from core.research.strategy_research import get_supported_research_strategies

        supported = set(get_supported_research_strategies())
    except Exception:
        supported = set()
    if not supported:
        return _dedupe_keep_order(strategies), []

    selected: List[str] = []
    dropped: List[str] = []
    for raw in strategies or []:
        item = str(raw or "").strip()
        if not item:
            continue
        if item in supported:
            selected.append(item)
        else:
            dropped.append(item)
    return _dedupe_keep_order(selected), _dedupe_keep_order(dropped)


def _recover_stale_jobs_on_startup(app: FastAPI) -> None:
    """D: On startup, fix proposals stuck in research_running/research_queued."""
    stale_states = {"research_running", "research_queued"}
    try:
        proposals = app.state.ai_proposal_registry.list(limit=None)
        for proposal in proposals:
            if str(proposal.status) not in stale_states:
                continue
            old_status = str(proposal.status)
            proposal.status = "rejected"  # type: ignore[assignment]
            proposal.metadata["last_research_error"] = "service restart — job not completed"
            proposal.updated_at = _now_utc()
            app.state.ai_proposal_registry.save(proposal)
            record_lifecycle(
                app.state.ai_lifecycle_registry,
                object_type="proposal",
                object_id=proposal.proposal_id,
                from_state=old_status,
                to_state="rejected",
                actor="system",
                reason="service restart — stale job recovered",
            )
        # Also mark stale experiment runs as failed
        runs = app.state.ai_experiment_run_registry.list(limit=None)
        for run in runs:
            if str(run.status) in {"running", "queued"}:
                run.status = "failed"  # type: ignore[assignment]
                run.finished_at = _now_utc()
                run.error = "service restart — run not completed"
                app.state.ai_experiment_run_registry.save(run)
        # Mark stale in-flight jobs as failed in persisted job store
        for job_id, job in dict(getattr(app.state, "research_jobs", {}) or {}).items():
            status = str(job.get("status") or "")
            if status in {"pending", "running"}:
                job["status"] = "failed"
                job["finished_at"] = _now_utc().isoformat()
                job["error"] = "service restart — job not completed"
                app.state.research_jobs[str(job_id)] = job
        _persist_research_jobs(app)
    except Exception:
        pass  # Recovery is best-effort; don't crash startup


def ensure_ai_research_runtime_state(app: FastAPI) -> None:
    base_dir = (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "ai").resolve()
    if not hasattr(app.state, "ai_research_dir"):
        app.state.ai_research_dir = base_dir
    if not hasattr(app.state, "ai_proposal_registry_path"):
        app.state.ai_proposal_registry_path = base_dir / "proposals.json"
    if not hasattr(app.state, "ai_experiment_registry_path"):
        app.state.ai_experiment_registry_path = base_dir / "experiments.json"
    if not hasattr(app.state, "ai_experiment_run_registry_path"):
        app.state.ai_experiment_run_registry_path = base_dir / "experiment_runs.json"
    if not hasattr(app.state, "ai_candidate_registry_path"):
        app.state.ai_candidate_registry_path = base_dir / "candidates.json"
    if not hasattr(app.state, "ai_lifecycle_registry_path"):
        app.state.ai_lifecycle_registry_path = base_dir / "lifecycle.json"
    if not hasattr(app.state, "ai_research_jobs_path"):
        app.state.ai_research_jobs_path = base_dir / "research_jobs.json"

    first_init = not isinstance(getattr(app.state, "ai_proposal_registry", None), ProposalRegistry)

    if not isinstance(getattr(app.state, "ai_proposal_registry", None), ProposalRegistry):
        app.state.ai_proposal_registry = ProposalRegistry(Path(app.state.ai_proposal_registry_path))
    if not isinstance(getattr(app.state, "ai_experiment_registry", None), ExperimentRegistry):
        app.state.ai_experiment_registry = ExperimentRegistry(Path(app.state.ai_experiment_registry_path))
    if not isinstance(getattr(app.state, "ai_experiment_run_registry", None), ExperimentRunRegistry):
        app.state.ai_experiment_run_registry = ExperimentRunRegistry(Path(app.state.ai_experiment_run_registry_path))
    if not isinstance(getattr(app.state, "ai_candidate_registry", None), CandidateRegistry):
        app.state.ai_candidate_registry = CandidateRegistry(Path(app.state.ai_candidate_registry_path))
    if not isinstance(getattr(app.state, "ai_lifecycle_registry", None), LifecycleRegistry):
        app.state.ai_lifecycle_registry = LifecycleRegistry(Path(app.state.ai_lifecycle_registry_path))
    if not isinstance(getattr(app.state, "research_jobs", None), dict):
        app.state.research_jobs = _load_research_jobs(Path(app.state.ai_research_jobs_path))
    if not isinstance(getattr(app.state, "research_job_tasks", None), dict):
        app.state.research_job_tasks = {}

    # D: Job recovery — on first init, fix any stale running/queued proposals
    if first_init:
        _recover_stale_jobs_on_startup(app)


def save_proposal(app: FastAPI, proposal: ResearchProposal) -> ResearchProposal:
    ensure_ai_research_runtime_state(app)
    proposal.updated_at = _now_utc()
    return app.state.ai_proposal_registry.save(proposal)


def get_proposal(app: FastAPI, proposal_id: str) -> ResearchProposal:
    ensure_ai_research_runtime_state(app)
    proposal = app.state.ai_proposal_registry.get(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal not found")
    return proposal


def list_proposals(app: FastAPI, limit: int = 20) -> List[ResearchProposal]:
    ensure_ai_research_runtime_state(app)
    return app.state.ai_proposal_registry.list(limit=max(1, min(int(limit), 200)))


def list_candidates(app: FastAPI, limit: int = 50) -> List[StrategyCandidate]:
    ensure_ai_research_runtime_state(app)
    return app.state.ai_candidate_registry.list(limit=max(1, min(int(limit), 200)))


def get_candidate(app: FastAPI, candidate_id: str) -> StrategyCandidate:
    ensure_ai_research_runtime_state(app)
    item = app.state.ai_candidate_registry.get(candidate_id)
    if item is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return item


def list_experiments(app: FastAPI, limit: int = 50) -> List[ExperimentSpec]:
    ensure_ai_research_runtime_state(app)
    return app.state.ai_experiment_registry.list(limit=max(1, min(int(limit), 200)))


def get_experiment(app: FastAPI, experiment_id: str) -> ExperimentSpec:
    ensure_ai_research_runtime_state(app)
    item = app.state.ai_experiment_registry.get(experiment_id)
    if item is None:
        raise HTTPException(status_code=404, detail="experiment not found")
    return item


def list_experiment_runs(app: FastAPI, experiment_id: str, limit: int = 100) -> List[ExperimentRun]:
    ensure_ai_research_runtime_state(app)
    return app.state.ai_experiment_run_registry.list_for_experiment(experiment_id, limit=max(1, min(int(limit), 200)))


def list_lifecycle(app: FastAPI, object_type: str, object_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    ensure_ai_research_runtime_state(app)
    rows = app.state.ai_lifecycle_registry.list_for_object(object_type, object_id, limit=max(1, min(int(limit), 500)))
    return [row.model_dump(mode="json") for row in rows]


def list_promotions(app: FastAPI, limit: int = 50) -> List[Dict[str, Any]]:
    items = []
    for candidate in list_candidates(app, limit=limit):
        if candidate.promotion is None:
            continue
        items.append(
            {
                "candidate_id": candidate.candidate_id,
                "proposal_id": candidate.proposal_id,
                "strategy": candidate.strategy,
                "status": candidate.status,
                "promotion": candidate.promotion.model_dump(mode="json"),
            }
        )
    return items


def get_deployment_status(app: FastAPI) -> Dict[str, Any]:
    rows = list_candidates(app, limit=200)
    counts = {
        "new": 0,
        "paper_running": 0,
        "shadow_running": 0,
        "live_candidate": 0,
        "live_running": 0,
        "retired": 0,
    }
    for row in rows:
        counts[str(row.status)] = counts.get(str(row.status), 0) + 1
    return {"counts": counts, "total_candidates": len(rows)}


def create_manual_proposal(
    app: FastAPI,
    *,
    actor: str,
    thesis: str,
    symbols: List[str],
    timeframes: List[str],
    market_regime: str,
    strategy_templates: List[str],
    source: str,
    expected_holding_period: str,
    risk_hypothesis: str,
    invalidation_rules: List[str],
    required_features: List[str],
    parameter_space: Dict[str, Dict[str, Any]],
    notes: List[str],
    metadata: Dict[str, Any],
) -> ResearchProposal:
    ensure_ai_research_runtime_state(app)
    now = _now_utc()
    normalized_symbols = _dedupe_keep_order([normalize_symbol(item) for item in symbols])
    if not normalized_symbols:
        normalized_symbols = ["BTC/USDT"]
    normalized_timeframes = normalize_timeframes(timeframes)
    planner_seed = generate_research_proposal(
        PlannerGenerateRequest(
            goal=str(thesis).strip(),
            market_regime=market_regime,
            symbols=normalized_symbols,
            timeframes=normalized_timeframes,
            constraints={},
            metadata={},
            origin_context={},
        ),
        actor=actor,
    ).proposal
    source_value = str(source or "ai").strip().lower()
    if source_value not in {"ai", "human", "hybrid"}:
        source_value = "ai"
    proposal = ResearchProposal(
        proposal_id=f"proposal-{int(now.timestamp())}-{secrets.token_hex(4)}",
        created_at=now,
        updated_at=now,
        status="draft",
        source=source_value,
        thesis=str(thesis).strip(),
        market_regime=str(market_regime or "mixed").strip() or "mixed",
        target_symbols=normalized_symbols,
        target_timeframes=normalized_timeframes,
        strategy_templates=_dedupe_keep_order(strategy_templates) or list(planner_seed.strategy_templates),
        parameter_space={str(k): dict(v or {}) for k, v in dict(parameter_space or {}).items()} or dict(planner_seed.parameter_space or {}),
        required_features=_dedupe_keep_order(required_features) or list(planner_seed.required_features),
        risk_hypothesis=str(risk_hypothesis or "").strip(),
        invalidation_rules=_dedupe_keep_order(invalidation_rules),
        expected_holding_period=str(expected_holding_period or "1d").strip() or "1d",
        planner_version="planner_v1" if source_value == "ai" else "manual_v1",
        origin_context={},
        notes=_dedupe_keep_order(notes),
        metadata={"created_by": actor, **dict(metadata or {})},
    )
    proposal_id, display_name, seq, day_key = _next_proposal_identity(app, proposal.thesis)
    proposal.proposal_id = proposal_id
    proposal.metadata["display_name"] = display_name
    proposal.metadata["proposal_sequence"] = seq
    proposal.metadata["proposal_day"] = day_key
    save_proposal(app, proposal)
    record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="proposal",
        object_id=proposal.proposal_id,
        from_state=None,
        to_state="draft",
        actor=actor,
        reason="proposal created",
    )
    return proposal


def generate_planned_proposal(
    app: FastAPI,
    *,
    actor: str,
    goal: str,
    market_regime: str,
    symbols: List[str],
    timeframes: List[str],
    constraints: Dict[str, Any],
    metadata: Dict[str, Any],
    origin_context: Dict[str, Any],
    market_context: Optional[Dict[str, Any]] = None,
    llm_research_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(app)
    planner_output = generate_research_proposal(
        PlannerGenerateRequest(
            goal=goal,
            market_regime=market_regime,
            symbols=symbols,
            timeframes=timeframes,
            constraints=constraints,
            metadata={**dict(metadata or {}), "created_by": actor},
            origin_context=origin_context,
            market_context=dict(market_context or {}),
            llm_research_output=dict(llm_research_output or {}),
        ),
        actor=actor,
    )
    proposal_id, display_name, seq, day_key = _next_proposal_identity(app, planner_output.proposal.thesis)
    planner_output.proposal.proposal_id = proposal_id
    planner_output.proposal.metadata["display_name"] = display_name
    planner_output.proposal.metadata["proposal_sequence"] = seq
    planner_output.proposal.metadata["proposal_day"] = day_key
    save_proposal(app, planner_output.proposal)
    record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="proposal",
        object_id=planner_output.proposal.proposal_id,
        from_state=None,
        to_state="draft",
        actor=actor,
        reason="proposal generated by planner",
        metadata={"planner_notes": planner_output.planner_notes},
    )
    return {
        "proposal": planner_output.proposal,
        "planner_notes": planner_output.planner_notes,
        "filtered_templates": list(planner_output.proposal.filtered_templates),
        "filtered_reasons": dict(planner_output.proposal.filtered_reasons),
    }


def build_research_config_from_proposal(
    proposal: ResearchProposal,
    *,
    exchange: str,
    symbol: Optional[str],
    days: int,
    commission_rate: float,
    slippage_bps: float,
    initial_capital: float,
    timeframes: List[str],
    strategies: List[str],
) -> ResearchConfig:
    resolved_symbol = normalize_symbol(symbol or (proposal.target_symbols[0] if proposal.target_symbols else "BTC/USDT"))
    resolved_timeframes = normalize_timeframes(timeframes or proposal.target_timeframes)
    strategy_programs: Dict[str, Any] = {}
    program_parameter_space: Dict[str, Dict[str, Any]] = {}
    for draft in list(getattr(proposal, "strategy_drafts", []) or []):
        program = getattr(draft, "program", None)
        if program is None:
            continue
        strategy_name = program_strategy_name(program, fallback=str(getattr(draft, "name", "") or "OpenAI Draft Strategy"))
        strategy_programs[strategy_name] = program
        if getattr(program, "parameter_space", None):
            program_parameter_space[strategy_name] = dict(program.parameter_space or {})
    draft_template_hints = _dedupe_keep_order(
        [
            str(getattr(draft, "template_hint", "") or "").strip()
            for draft in list(getattr(proposal, "strategy_drafts", []) or [])
            if str(getattr(draft, "template_hint", "") or "").strip()
        ]
    )
    requested_strategies = _dedupe_keep_order(strategies or proposal.strategy_templates or draft_template_hints)
    resolved_strategies, dropped = _filter_supported_research_strategies(requested_strategies)
    if dropped:
        proposal.metadata["last_dropped_unsupported_strategies"] = dropped
    if not resolved_strategies:
        fallback, _ = _filter_supported_research_strategies(proposal.strategy_templates or draft_template_hints)
        resolved_strategies = fallback
    all_executable = _dedupe_keep_order(list(resolved_strategies) + list(strategy_programs.keys()))
    if not all_executable:
        raise ValueError("proposal has no executable strategy templates or strategy programs to research")
    return ResearchConfig(
        exchange=str(exchange or "binance").strip().lower() or "binance",
        symbol=resolved_symbol,
        days=int(days),
        initial_capital=float(initial_capital),
        timeframes=resolved_timeframes,
        strategies=all_executable,
        commission_rate=float(commission_rate),
        slippage_bps=float(slippage_bps),
        # B: pass parameter_space from proposal so research can do grid search
        parameter_space={**dict(proposal.parameter_space or {}), **program_parameter_space},
        strategy_programs=strategy_programs,
    )


def delete_proposal(
    app: FastAPI,
    *,
    proposal_id: str,
    actor: str,
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(app)
    proposal = get_proposal(app, proposal_id)

    proposal_jobs: List[tuple[str, Dict[str, Any]]] = []
    for job_id, job in (app.state.research_jobs or {}).items():
        if str(job.get("proposal_id") or "") != str(proposal_id):
            continue
        raw = dict(job or {})
        status = str(raw.get("status") or "")
        task = (app.state.research_job_tasks or {}).get(str(job_id))
        # Allow deleting proposals whose persisted job is stale after restart or task loss.
        if status in {"pending", "running"} and (task is None or getattr(task, "done", lambda: False)()):
            raw["status"] = "failed"
            raw["finished_at"] = raw.get("finished_at") or _now_utc().isoformat()
            raw["error"] = raw.get("error") or "stale research job recovered during delete"
            app.state.research_jobs[str(job_id)] = raw
        proposal_jobs.append((str(job_id), raw))
    has_active_job = any(str(job.get("status") or "") in {"pending", "running"} for _, job in proposal_jobs)
    if has_active_job:
        raise HTTPException(status_code=409, detail="proposal has active research job, cannot delete")
    if proposal_jobs:
        _persist_research_jobs(app)

    blocked_states = {
        "paper_running",
        "shadow_running",
        "live_running",
    }
    if str(proposal.status) in blocked_states:
        raise HTTPException(status_code=409, detail=f"proposal in state {proposal.status}, cannot delete")

    experiment_rows = [row for row in app.state.ai_experiment_registry.list(limit=None) if row.proposal_id == str(proposal_id)]
    experiment_ids = [row.experiment_id for row in experiment_rows]
    experiment_id_set = set(experiment_ids)
    run_ids = [
        row.run_id
        for row in app.state.ai_experiment_run_registry.list(limit=None)
        if row.experiment_id in experiment_id_set
    ]
    candidate_rows = [row for row in app.state.ai_candidate_registry.list(limit=None) if row.proposal_id == str(proposal_id)]
    candidate_ids = [row.candidate_id for row in candidate_rows]

    removed_runs = app.state.ai_experiment_run_registry.delete_many(run_ids)
    removed_experiments = app.state.ai_experiment_registry.delete_many(experiment_ids)
    removed_candidates = app.state.ai_candidate_registry.delete_many(candidate_ids)
    removed_proposal = 1 if app.state.ai_proposal_registry.delete(proposal_id) else 0

    lifecycle_removed = 0
    lifecycle_removed += app.state.ai_lifecycle_registry.delete_for_object("proposal", proposal_id)
    lifecycle_removed += app.state.ai_lifecycle_registry.delete_for_objects("experiment", experiment_ids)
    lifecycle_removed += app.state.ai_lifecycle_registry.delete_for_objects("candidate", candidate_ids)

    removed_jobs = 0
    for job_id, _ in proposal_jobs:
        if app.state.research_jobs.pop(job_id, None) is not None:
            removed_jobs += 1
        app.state.research_job_tasks.pop(job_id, None)
    if removed_jobs > 0:
        _persist_research_jobs(app)

    return {
        "proposal_id": str(proposal_id),
        "deleted": bool(removed_proposal),
        "deleted_counts": {
            "proposal": removed_proposal,
            "experiments": removed_experiments,
            "runs": removed_runs,
            "candidates": removed_candidates,
            "lifecycle_records": lifecycle_removed,
            "jobs": removed_jobs,
        },
        "actor": str(actor or "system"),
    }


def _build_experiment_spec(proposal: ResearchProposal, config: ResearchConfig, actor: str) -> ExperimentSpec:
    now = _now_utc()
    return ExperimentSpec(
        experiment_id=f"experiment-{int(now.timestamp())}-{secrets.token_hex(4)}",
        proposal_id=proposal.proposal_id,
        created_at=now,
        exchange=config.exchange,
        symbol=config.symbol,
        research_mode=str(getattr(proposal, "research_mode", "template") or "template"),
        timeframes=list(config.timeframes),
        strategies=list(config.strategies),
        strategy_drafts=list(getattr(proposal, "strategy_drafts", []) or []),
        strategy_programs=[
            program
            for program in dict(getattr(config, "strategy_programs", {}) or {}).values()
            if program is not None
        ],
        parameter_space=dict(config.parameter_space or {}),
        search_summary=getattr(proposal, "search_summary", None),
        days=int(config.days),
        initial_capital=float(config.initial_capital),
        commission_rate=float(config.commission_rate),
        slippage_bps=float(config.slippage_bps),
        research_profile="standard",
        search_budget=getattr(proposal, "search_budget", None),
        lineage=getattr(proposal, "lineage", None),
        status="queued",
        metadata={
            "created_by": actor,
            "research_mode": str(getattr(proposal, "research_mode", "template") or "template"),
            "strategy_draft_count": len(list(getattr(proposal, "strategy_drafts", []) or [])),
        },
    )


def _build_experiment_run(experiment_id: str) -> ExperimentRun:
    return ExperimentRun(
        run_id=f"run-{int(_now_utc().timestamp())}-{secrets.token_hex(4)}",
        experiment_id=experiment_id,
        status="queued",
    )


def _correlation_filter_candidates(
    candidates: List[StrategyCandidate],
    corr_threshold: float = 0.85,
    existing_candidates: Optional[List[StrategyCandidate]] = None,
) -> None:
    """In-place: mark candidates whose equity curves are highly correlated with a better one.

    Uses Pearson correlation of the 50-point equity_curve_sample stored in candidate.metadata["best"].
    Candidates are assumed to be sorted by score desc (best first). The first one in each
    correlation group is kept; subsequent highly-correlated ones are downgraded to reject and
    flagged with metadata["correlation_filtered"] = True.

    existing_candidates: already-registered strategies (paper/shadow/live) whose
    equity curves count as pre-accepted baselines for correlation checking.
    """
    import numpy as np

    def _get_curve(c: StrategyCandidate) -> Optional[List[float]]:
        best_meta = dict(c.metadata.get("best") or {})
        raw = best_meta.get("equity_curve_sample") or []
        return list(raw) if len(raw) >= 10 else None

    curves: Dict[str, Optional[List[float]]] = {c.strategy: _get_curve(c) for c in candidates}

    # Pre-seed accepted list with existing running strategies
    accepted: List[str] = []
    accepted_curves: Dict[str, Optional[List[float]]] = {}
    existing_strategy_set = set()
    for exc in (existing_candidates or []):
        strat = exc.strategy
        curve = _get_curve(exc)
        if strat not in accepted:
            accepted.append(strat)
            accepted_curves[strat] = curve
            existing_strategy_set.add(strat)

    for cand in candidates:
        strat = cand.strategy
        my_curve = curves.get(strat)

        # Check against all accepted (existing + previously accepted new)
        max_corr = 0.0
        corr_peer: Optional[str] = None
        all_accepted_curves = {**accepted_curves, **{s: curves.get(s) for s in accepted if s in curves}}

        if my_curve is not None:
            for acc_strat, peer_curve in all_accepted_curves.items():
                if peer_curve is None or acc_strat == strat:
                    continue
                n = min(len(my_curve), len(peer_curve))
                x = np.array(my_curve[:n], dtype=float)
                y = np.array(peer_curve[:n], dtype=float)
                if x.std() < 1e-9 or y.std() < 1e-9:
                    continue
                corr = abs(float(np.corrcoef(x, y)[0, 1]))
                if corr > max_corr:
                    max_corr = corr
                    corr_peer = acc_strat

        if my_curve is not None and max_corr >= corr_threshold and corr_peer is not None:
            cand.metadata["correlation_filtered"] = True
            cand.metadata["correlated_with"] = corr_peer
            cand.metadata["correlation_value"] = round(max_corr, 3)
            cand.metadata["correlation_is_cross_batch"] = corr_peer in existing_strategy_set
            if cand.promotion and cand.promotion.decision != "reject":
                from core.research.experiment_schemas import PromotionDecision as _PD
                cand.promotion = _PD(
                    candidate_id=cand.candidate_id,
                    decision="reject",
                    reason=f"redundant — highly correlated with {corr_peer} (ρ={max_corr:.2f})",
                    constraints={},
                    created_at=_now_utc(),
                )
                cand.promotion_target = None
        else:
            accepted.append(strat)
            accepted_curves[strat] = my_curve


def _correlation_filter_candidates_v2(
    candidates: List[StrategyCandidate],
    corr_threshold: float = 0.85,
    existing_candidates: Optional[List[StrategyCandidate]] = None,
) -> None:
    """In-place: mark candidates that are effectively redundant."""
    import numpy as np

    def _get_curve(c: StrategyCandidate) -> Optional[List[float]]:
        best_meta = dict(c.metadata.get("best") or {})
        raw = best_meta.get("equity_curve_sample") or []
        return list(raw) if len(raw) >= 10 else None

    def _params_key(c: StrategyCandidate) -> tuple[tuple[str, str], ...]:
        params = dict(c.params or {})
        return tuple(sorted((str(k), str(v)) for k, v in params.items()))

    def _signature(c: StrategyCandidate) -> tuple[str, str, str, str]:
        meta = get_strategy_registry_entry(c.strategy)
        family = str(meta.get("family") or meta.get("decision_engine") or meta.get("category") or c.strategy)
        category = str(meta.get("category") or "")
        return (family, category, str(c.symbol or ""), str(c.timeframe or ""))

    def _reject(c: StrategyCandidate, reason: str) -> None:
        c.metadata["correlation_filtered"] = True
        if c.promotion and c.promotion.decision != "reject":
            from core.research.experiment_schemas import PromotionDecision as _PD

            c.promotion = _PD(
                candidate_id=c.candidate_id,
                decision="reject",
                reason=reason,
                constraints={},
                created_at=_now_utc(),
            )
            c.promotion_target = None

    curves: Dict[str, Optional[List[float]]] = {c.strategy: _get_curve(c) for c in candidates}
    accepted: List[Dict[str, Any]] = []
    accepted_exact_keys: set[tuple[str, str, str, tuple[tuple[str, str], ...]]] = set()
    existing_strategy_set = set()

    for exc in (existing_candidates or []):
        exact_key = (str(exc.strategy or ""), str(exc.symbol or ""), str(exc.timeframe or ""), _params_key(exc))
        if exact_key in accepted_exact_keys:
            continue
        accepted_exact_keys.add(exact_key)
        accepted.append(
            {
                "strategy": exc.strategy,
                "curve": _get_curve(exc),
                "signature": _signature(exc),
            }
        )
        existing_strategy_set.add(exc.strategy)

    for cand in candidates:
        strat = cand.strategy
        my_curve = curves.get(strat)
        my_signature = _signature(cand)
        my_exact_key = (str(cand.strategy or ""), str(cand.symbol or ""), str(cand.timeframe or ""), _params_key(cand))

        if my_exact_key in accepted_exact_keys:
            cand.metadata["correlated_with"] = str(cand.strategy)
            cand.metadata["correlation_value"] = 1.0
            cand.metadata["correlation_is_cross_batch"] = True
            cand.metadata["duplicate_signature"] = True
            _reject(cand, "redundant candidate: identical strategy/timeframe/params already exists")
            continue

        max_corr = 0.0
        corr_peer: Optional[str] = None
        corr_peer_signature: Optional[tuple[str, str, str, str]] = None
        effective_threshold = corr_threshold

        if my_curve is not None:
            for accepted_item in accepted:
                acc_strat = str(accepted_item.get("strategy") or "")
                peer_curve = accepted_item.get("curve")
                if peer_curve is None or acc_strat == strat:
                    continue
                n = min(len(my_curve), len(peer_curve))
                x = np.array(my_curve[:n], dtype=float)
                y = np.array(peer_curve[:n], dtype=float)
                if x.std() < 1e-9 or y.std() < 1e-9:
                    continue
                corr = abs(float(np.corrcoef(x, y)[0, 1]))
                if corr > max_corr:
                    max_corr = corr
                    corr_peer = acc_strat
                    corr_peer_signature = accepted_item.get("signature")
                    effective_threshold = min(corr_threshold, 0.72) if corr_peer_signature == my_signature else corr_threshold

        if my_curve is not None and max_corr >= effective_threshold and corr_peer is not None:
            cand.metadata["correlated_with"] = corr_peer
            cand.metadata["correlation_value"] = round(max_corr, 3)
            cand.metadata["correlation_is_cross_batch"] = corr_peer in existing_strategy_set
            cand.metadata["duplicate_signature"] = corr_peer_signature == my_signature
            if corr_peer_signature == my_signature:
                reason = f"redundant candidate: same family/signature and highly correlated with {corr_peer} (corr={max_corr:.2f})"
            else:
                reason = f"redundant candidate: highly correlated with {corr_peer} (corr={max_corr:.2f})"
            _reject(cand, reason)
        else:
            accepted.append(
                {
                    "strategy": strat,
                    "curve": my_curve,
                    "signature": my_signature,
                }
            )
            accepted_exact_keys.add(my_exact_key)


def _create_candidates_from_result(
    proposal: ResearchProposal,
    experiment: ExperimentSpec,
    result: Dict[str, Any],
    existing_candidates: Optional[List[StrategyCandidate]] = None,
) -> "tuple[Any, List[StrategyCandidate], Optional[StrategyCandidate]]":
    """Create one StrategyCandidate per strategy; return (overall_summary, all_candidates, best_candidate)."""
    overall_summary = build_validation_summary_from_research_result(result)
    best = dict(result.get("best") or {})
    best_per_strategy = dict(result.get("best_per_strategy") or {})
    raw_top = result.get("top_results")
    global_top = list(raw_top) if isinstance(raw_top, list) else []
    valid_counts = dict(result.get("strategy_valid_counts") or {})
    error_counts = dict(result.get("strategy_error_counts") or {})

    # Fall back to single best when best_per_strategy not populated
    if not best_per_strategy and best and best.get("strategy"):
        best_per_strategy = {str(best["strategy"]): best}

    candidates: List[StrategyCandidate] = []
    for strat_name, strat_best in best_per_strategy.items():
        if not strat_best:
            continue
        strategy_meta = get_strategy_registry_entry(str(strat_best.get("strategy") or strat_name))
        program_lookup = {
            program_strategy_name(program): program
            for program in list(getattr(experiment, "strategy_programs", []) or [])
            if program is not None
        }
        strategy_program = program_lookup.get(str(strat_best.get("strategy") or strat_name))
        strategy_family = "ai_openai" if strategy_program is not None else strategy_meta.get("family")
        decision_engine = "openai" if strategy_program is not None else strategy_meta.get("decision_engine")
        # Build per-strategy validation using strategy-level run counts
        valid_r = max(int(valid_counts.get(strat_name, 0) or 0), 1)
        error_r = int(error_counts.get(strat_name, 0) or 0)
        per_result = {
            "runs": valid_r + error_r,
            "valid_runs": valid_r,
            "quality_counts": {"ok": valid_r},
            "best": strat_best,
        }
        strat_summary = build_validation_summary_from_research_result(per_result)
        strat_top = [r for r in global_top if r.get("strategy") == strat_name] or [strat_best]
        # B: populate candidate.params from grid-search best_params
        best_params_from_research = dict(strat_best.get("best_params") or {})
        candidate = StrategyCandidate(
            candidate_id=f"candidate-{int(_now_utc().timestamp())}-{secrets.token_hex(4)}",
            proposal_id=proposal.proposal_id,
            experiment_id=experiment.experiment_id,
            created_at=_now_utc(),
            strategy=str(strat_best.get("strategy") or strat_name),
            timeframe=str(strat_best.get("timeframe") or (proposal.target_timeframes[0] if proposal.target_timeframes else "1h")),
            symbol=str(experiment.symbol),
            params=best_params_from_research,
            score=float(strat_best.get("score", 0.0) or 0.0),
            validation_summary=strat_summary,
            metadata={
                "exchange": experiment.exchange,
                "best": strat_best,
                "research_mode": str(getattr(proposal, "research_mode", "template") or "template"),
                "strategy_draft_count": len(list(getattr(proposal, "strategy_drafts", []) or [])),
                "strategy_drafts": [
                    draft.model_dump(mode="json") if hasattr(draft, "model_dump") else dict(draft or {})
                    for draft in list(getattr(proposal, "strategy_drafts", []) or [])
                ],
                "search_budget": (
                    proposal.search_budget.model_dump(mode="json")
                    if getattr(proposal, "search_budget", None) is not None
                    else {}
                ),
                "search_summary": (
                    proposal.search_summary.model_dump(mode="json")
                    if getattr(proposal, "search_summary", None) is not None
                    else {}
                ),
                "lineage": (
                    proposal.lineage.model_dump(mode="json")
                    if getattr(proposal, "lineage", None) is not None
                    else None
                ),
                "common_pnl": build_common_pnl_summary(
                    source="research_batch_backtest",
                    unit="pct_return",
                    gross_pnl=strat_best.get("gross_total_return"),
                    fee=strat_best.get("cost_drag_return_pct"),
                    slippage_cost=None,
                    funding_pnl=0.0,
                    net_pnl=strat_best.get("total_return"),
                    turnover=None,
                    trade_count=strat_best.get("total_trades"),
                    win_rate=strat_best.get("win_rate"),
                    cost_model_version="research_batch_v1",
                    metadata={
                        "timeframe": str(strat_best.get("timeframe") or ""),
                        "strategy": str(strat_best.get("strategy") or strat_name),
                        "funding_available": bool(result.get("funding_available", False)),
                    },
                ),
                "top_results": strat_top,
                "strategy_valid_counts": valid_counts,
                "strategy_error_counts": error_counts,
                "csv_path": result.get("csv_path"),
                "markdown_path": result.get("markdown_path"),
                "news_events_count": int(result.get("news_events_count", 0) or 0),
                "funding_available": bool(result.get("funding_available", False)),
                "decision_engine": decision_engine,
                "strategy_family": strategy_family,
                "ai_driven": bool(strategy_meta.get("ai_driven", False) or strategy_program is not None),
                "strategy_program": (
                    strategy_program.model_dump(mode="json")
                    if strategy_program is not None and hasattr(strategy_program, "model_dump")
                    else None
                ),
            },
        )
        promo = build_promotion_decision(candidate.candidate_id, strat_summary)
        candidate.promotion = promo
        normalized_target = "paper" if promo.decision == "shadow" else promo.decision
        candidate.promotion_target = normalized_target if normalized_target in {"paper", "live_candidate"} else None
        candidates.append(candidate)

    candidates.sort(key=lambda c: c.score, reverse=True)
    # Correlation filter: mark redundant candidates (within batch + cross-batch vs existing running)
    if len(candidates) > 1 or existing_candidates:
        _correlation_filter_candidates_v2(candidates, corr_threshold=0.85, existing_candidates=existing_candidates or [])

    best_candidate = next((c for c in candidates if not c.metadata.get("correlation_filtered")), None) or (candidates[0] if candidates else None)
    if best_candidate is not None:
        for cand in candidates:
            if cand.candidate_id == best_candidate.candidate_id and not cand.metadata.get("correlation_filtered"):
                cand.metadata["search_role"] = "champion"
                cand.metadata["champion_candidate_id"] = cand.candidate_id
                cand.metadata["champion_strategy"] = cand.strategy
            else:
                cand.metadata["search_role"] = "challenger"
                cand.metadata["champion_candidate_id"] = best_candidate.candidate_id
                cand.metadata["champion_strategy"] = best_candidate.strategy
    return overall_summary, candidates, best_candidate


async def _finalize_research_run(
    app: FastAPI,
    *,
    proposal_id: str,
    experiment_id: str,
    run_id: str,
    request_payload: Dict[str, Any],
    config: ResearchConfig,
    actor: str,
    job_id: str | None,
) -> Dict[str, Any]:
    proposal = get_proposal(app, proposal_id)
    experiment = get_experiment(app, experiment_id)
    run = app.state.ai_experiment_run_registry.get(run_id)
    if run is None:
        raise RuntimeError("experiment run not found")

    proposal.metadata["last_research_request"] = request_payload
    proposal.metadata["last_research_job_id"] = job_id
    if proposal.status == "research_queued":
        transition_proposal(proposal, to_state="research_running", lifecycle_registry=app.state.ai_lifecycle_registry, actor=actor, reason="research execution started")
        save_proposal(app, proposal)

    experiment.status = "running"
    app.state.ai_experiment_registry.save(experiment)
    record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="experiment",
        object_id=experiment.experiment_id,
        from_state="queued",
        to_state="running",
        actor=actor,
        reason="research execution started",
    )
    run.status = "running"
    run.started_at = _now_utc()
    app.state.ai_experiment_run_registry.save(run)

    # Fetch existing active candidates for cross-batch correlation check
    existing_active: List[StrategyCandidate] = []
    try:
        active_statuses = {"new", "paper_running", "shadow_running", "live_candidate", "live_running"}
        for _c in app.state.ai_candidate_registry.list(limit=None):
            if str(_c.status) in active_statuses:
                existing_active.append(_c)
    except Exception:
        pass

    result = await run_strategy_research(config)
    summary, candidates, candidate = _create_candidates_from_result(proposal, experiment, result, existing_candidates=existing_active)
    promotion = candidate.promotion if candidate else None

    # LLM rationale generation — best-effort, non-blocking
    try:
        import asyncio as _asyncio
        from core.ai.promotion_narrator import generate_promotion_rationale as _gen_rationale

        async def _add_rationale(cand: StrategyCandidate) -> None:
            if cand.validation_summary is None:
                return
            rationale = await _gen_rationale(
                candidate_dict=cand.model_dump(mode="json"),
                validation_summary_dict=cand.validation_summary.model_dump(mode="json"),
                timeout=25,
            )
            if rationale:
                cand.metadata["llm_rationale"] = rationale

        worthy = [c for c in candidates if
                  not c.metadata.get("correlation_filtered") and
                  c.promotion and c.promotion.decision != "reject"][:3]
        if worthy:
            await _asyncio.gather(*[_add_rationale(c) for c in worthy], return_exceptions=True)
    except Exception as _llm_err:
        logger.warning(f"LLM rationale generation failed: {_llm_err}")

    run.status = "completed"
    run.finished_at = _now_utc()
    run.result = result
    app.state.ai_experiment_run_registry.save(run)

    experiment.status = "completed"
    app.state.ai_experiment_registry.save(experiment)
    record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="experiment",
        object_id=experiment.experiment_id,
        from_state="running",
        to_state="completed",
        actor=actor,
        reason="research execution finished",
        metadata={"run_id": run.run_id},
    )

    proposal.validation_summary = summary
    proposal.latest_experiment_id = experiment.experiment_id
    proposal.metadata["last_research_result"] = {
        "job_id": job_id,
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "csv_path": result.get("csv_path"),
        "markdown_path": result.get("markdown_path"),
        "news_events_count": int(result.get("news_events_count", 0) or 0),
        "funding_available": bool(result.get("funding_available", False)),
        "best": result.get("best"),
        "common_pnl": build_common_pnl_summary(
            source="research_batch_backtest",
            unit="pct_return",
            gross_pnl=(result.get("best") or {}).get("gross_total_return"),
            fee=(result.get("best") or {}).get("cost_drag_return_pct"),
            slippage_cost=None,
            funding_pnl=0.0,
            net_pnl=(result.get("best") or {}).get("total_return"),
            turnover=None,
            trade_count=(result.get("best") or {}).get("total_trades"),
            win_rate=(result.get("best") or {}).get("win_rate"),
            cost_model_version="research_batch_v1",
            metadata={
                "symbol": config.symbol,
                "exchange": config.exchange,
                "timeframes": list(config.timeframes),
                "funding_available": bool(result.get("funding_available", False)),
            },
        ),
        "validation_summary": summary.model_dump(mode="json"),
    }
    proposal.metadata.pop("last_research_error", None)

    if candidate is None:
        transition_proposal(proposal, to_state="rejected", lifecycle_registry=app.state.ai_lifecycle_registry, actor=actor, reason="no valid candidate produced")
        save_proposal(app, proposal)
        return {
            "proposal": proposal,
            "experiment": experiment,
            "run": run,
            "candidates": [],
            "candidate": None,
            "promotion": None,
            "research_result": result,
        }

    # Save all per-strategy candidates; lifecycle record for each
    for cand in candidates:
        app.state.ai_candidate_registry.save(cand)
        record_lifecycle(
            app.state.ai_lifecycle_registry,
            object_type="candidate",
            object_id=cand.candidate_id,
            from_state=None,
            to_state="new",
            actor=actor,
            reason="candidate created from research result",
            metadata={"experiment_id": experiment.experiment_id, "is_best": cand is candidate},
        )
    proposal.latest_candidate_id = candidate.candidate_id
    if proposal.status == "research_running":
        transition_proposal(proposal, to_state="validated", lifecycle_registry=app.state.ai_lifecycle_registry, actor=actor, reason=f"{len(candidates)} candidate(s) validated, best: {candidate.candidate_id}")
    save_proposal(app, proposal)

    promotion_result = None
    try:
        strategy_spec_result = await governance_propose_strategy(
            GovernanceIdentity(actor="ai_research", role="SYSTEM"),
            strategy_id=f"ai::{proposal.proposal_id}::{candidate.strategy}",
            name=f"AI/{candidate.strategy}/{experiment.symbol}/{candidate.timeframe}",
            strategy_class=candidate.strategy,
            params=dict(candidate.params or {}),
            guardrails={
                "promotion_decision": str((promotion.decision if promotion else "reject")),
                "deployment_score": float((candidate.validation_summary.deployment_score if candidate.validation_summary else 0.0) or 0.0),
                "max_drawdown_pct": float((candidate.validation_summary.metrics.get("best", {}).get("max_drawdown", 0.0) if candidate.validation_summary else 0.0) or 0.0),
            },
            metrics={
                "validation_summary": candidate.validation_summary.model_dump(mode="json") if candidate.validation_summary else {},
                "top_results": candidate.metadata.get("top_results") or [],
            },
            regime=str(proposal.market_regime or "mixed"),
        )
        candidate.metadata["strategy_spec"] = strategy_spec_result
        proposal.metadata["latest_strategy_spec"] = strategy_spec_result
    except Exception as exc:
        candidate.metadata["strategy_spec_error"] = str(exc)
        proposal.metadata["latest_strategy_spec_error"] = str(exc)

    governance_enabled = bool(getattr(settings, "GOVERNANCE_ENABLED", True))
    if promotion is not None and promotion.decision != "reject":
        normalized_target = "paper" if str(promotion.decision) == "shadow" else str(promotion.decision)
        promotion.decision = normalized_target
        candidate.promotion_target = normalized_target if normalized_target in {"paper", "live_candidate"} else None
        candidate.metadata["recommended_runtime_target"] = normalized_target
        candidate.metadata["manual_register_required"] = True
        proposal.metadata["manual_register_required"] = True
        candidate.metadata.pop("promotion_pending_human_gate", None)
        proposal.metadata.pop("promotion_pending_human_gate", None)
        if governance_enabled:
            candidate.metadata["promotion_pending_human_gate"] = True
            proposal.metadata["promotion_pending_human_gate"] = True
            app.state.ai_candidate_registry.save(candidate)
            save_proposal(app, proposal)
        else:
            app.state.ai_candidate_registry.save(candidate)
            save_proposal(app, proposal)
    elif promotion is not None:
        if proposal.status == "validated":
            transition_proposal(proposal, to_state="rejected", lifecycle_registry=app.state.ai_lifecycle_registry, actor=actor, reason=promotion.reason)
        save_proposal(app, proposal)

    latest_payload = {
        "job_id": job_id,
        "proposal_id": proposal_id,
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "request_summary": request_payload,
        "output_dir": str(Path(result.get("csv_path") or "").resolve().parent) if result.get("csv_path") else str(config.output_dir.resolve()),
        "csv_path": result.get("csv_path"),
        "markdown_path": result.get("markdown_path"),
        "top_result_summary": result.get("best"),
        "finished_at": _now_utc().isoformat(),
    }
    latest_path = (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "latest.json").resolve()
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "proposal": proposal,
        "experiment": experiment,
        "run": run,
        "candidates": candidates,
        "candidate": candidate,
        "promotion": promotion_result["promotion"] if promotion_result else promotion,
        "promotion_result": promotion_result,
        "research_result": result,
    }


async def _run_proposal_background_job(
    app: FastAPI,
    *,
    job_id: str,
    proposal_id: str,
    experiment_id: str,
    run_id: str,
    request_payload: Dict[str, Any],
    config: ResearchConfig,
    actor: str,
) -> None:
    job = app.state.research_jobs.get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    app.state.research_jobs[job_id] = job
    _persist_research_jobs(app)
    try:
        result = await _finalize_research_run(
            app,
            proposal_id=proposal_id,
            experiment_id=experiment_id,
            run_id=run_id,
            request_payload=request_payload,
            config=config,
            actor=actor,
            job_id=job_id,
        )
        job.update(
            {
                "status": "completed",
                "finished_at": _now_utc().isoformat(),
                "result": {
                    "proposal_id": proposal_id,
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "status": result["proposal"].status,
                },
                "error": None,
            }
        )
    except asyncio.CancelledError:
        # Cancellation may come from explicit user action; preserve cancellation status.
        if str(job.get("status") or "") not in {"completed", "failed", "cancelled"}:
            job.update(
                {
                    "status": "cancelled",
                    "finished_at": _now_utc().isoformat(),
                    "result": None,
                    "error": "cancelled",
                }
            )
    except Exception as exc:
        proposal = get_proposal(app, proposal_id)
        if proposal.status in {"research_queued", "research_running", "validated"}:
            if proposal.status == "research_running":
                transition_proposal(proposal, to_state="rejected", lifecycle_registry=app.state.ai_lifecycle_registry, actor=actor, reason=f"research failed: {exc}")
            else:
                proposal.status = "rejected"
            proposal.metadata["last_research_error"] = str(exc)
            save_proposal(app, proposal)
        experiment = get_experiment(app, experiment_id)
        experiment.status = "failed"
        app.state.ai_experiment_registry.save(experiment)
        run = app.state.ai_experiment_run_registry.get(run_id)
        if run is not None:
            run.status = "failed"
            run.finished_at = _now_utc()
            run.error = str(exc)
            app.state.ai_experiment_run_registry.save(run)
        job.update(
            {
                "status": "failed",
                "finished_at": _now_utc().isoformat(),
                "result": None,
                "error": str(exc),
            }
        )
    finally:
        app.state.research_jobs[job_id] = job
        app.state.research_job_tasks.pop(job_id, None)
        _persist_research_jobs(app)


async def run_proposal(
    app: FastAPI,
    *,
    proposal_id: str,
    actor: str,
    exchange: str,
    symbol: Optional[str],
    days: int,
    commission_rate: float,
    slippage_bps: float,
    initial_capital: float,
    background: bool,
    timeframes: List[str],
    strategies: List[str],
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(app)
    proposal = get_proposal(app, proposal_id)
    config = build_research_config_from_proposal(
        proposal,
        exchange=exchange,
        symbol=symbol,
        days=days,
        commission_rate=commission_rate,
        slippage_bps=slippage_bps,
        initial_capital=initial_capital,
        timeframes=timeframes,
        strategies=strategies,
    )
    experiment = _build_experiment_spec(proposal, config, actor)
    run = _build_experiment_run(experiment.experiment_id)

    target_state = "research_queued" if background else "research_running"
    try:
        transition_proposal(
            proposal,
            to_state=target_state,
            lifecycle_registry=app.state.ai_lifecycle_registry,
            actor=actor,
            reason="proposal research submitted",
            metadata={"experiment_id": experiment.experiment_id},
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    app.state.ai_experiment_registry.save(experiment)
    app.state.ai_experiment_run_registry.save(run)
    proposal.latest_experiment_id = experiment.experiment_id
    save_proposal(app, proposal)
    record_lifecycle(
        app.state.ai_lifecycle_registry,
        object_type="experiment",
        object_id=experiment.experiment_id,
        from_state=None,
        to_state="queued",
        actor=actor,
        reason="experiment created",
        metadata={"proposal_id": proposal.proposal_id, "run_id": run.run_id},
    )

    request_payload = {
        "proposal_id": proposal_id,
        "exchange": config.exchange,
        "symbol": config.symbol,
        "days": config.days,
        "timeframes": list(config.timeframes),
        "strategies": list(config.strategies),
        "commission_rate": float(config.commission_rate),
        "slippage_bps": float(config.slippage_bps),
        "initial_capital": float(config.initial_capital),
        "background": bool(background),
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "proposal_status_before": proposal.status,
    }
    proposal.metadata["last_research_request"] = request_payload
    save_proposal(app, proposal)

    if not background:
        return await _finalize_research_run(
            app,
            proposal_id=proposal.proposal_id,
            experiment_id=experiment.experiment_id,
            run_id=run.run_id,
            request_payload=request_payload,
            config=config,
            actor=actor,
            job_id=None,
        )

    job_id = f"proposal-research-{int(_now_utc().timestamp())}-{secrets.token_hex(4)}"
    job = {
        "job_id": job_id,
        "proposal_id": proposal.proposal_id,
        "experiment_id": experiment.experiment_id,
        "run_id": run.run_id,
        "status": "pending",
        "created_at": _now_utc().isoformat(),
        "started_at": None,
        "finished_at": None,
        "request": request_payload,
        "result": None,
        "error": None,
    }
    app.state.research_jobs[job_id] = job
    _persist_research_jobs(app)
    proposal.metadata["last_research_job_id"] = job_id
    save_proposal(app, proposal)
    task = asyncio.create_task(
        _run_proposal_background_job(
            app,
            job_id=job_id,
            proposal_id=proposal.proposal_id,
            experiment_id=experiment.experiment_id,
            run_id=run.run_id,
            request_payload=request_payload,
            config=config,
            actor=actor,
        ),
        name=f"ai_research_{job_id}",
    )
    app.state.research_job_tasks[job_id] = task
    return {"job": job, "proposal": proposal, "experiment": experiment, "run": run}


async def cancel_proposal_job(
    app: FastAPI,
    *,
    proposal_id: str,
    actor: str,
    reason: str = "research cancelled by user",
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(app)
    proposal = get_proposal(app, proposal_id)
    jobs = dict(getattr(app.state, "research_jobs", {}) or {})

    def _is_active(_job: Dict[str, Any]) -> bool:
        return str(_job.get("status") or "") in {"pending", "running"}

    job_id = str(proposal.metadata.get("last_research_job_id") or "").strip()
    job = jobs.get(job_id) if job_id else None
    if not (job and _is_active(job)):
        active = [
            (jid, j)
            for jid, j in jobs.items()
            if str(j.get("proposal_id") or "") == str(proposal_id) and _is_active(j)
        ]
        if active:
            active.sort(
                key=lambda pair: str(
                    pair[1].get("started_at")
                    or pair[1].get("created_at")
                    or ""
                )
            )
            job_id, job = active[-1]
        else:
            old_status = str(proposal.status)
            if old_status in {"research_queued", "research_running"}:
                stale_reason = "no active research job; recovered stale queued/running status"
                if old_status == "research_running":
                    transition_proposal(
                        proposal,
                        to_state="rejected",
                        lifecycle_registry=app.state.ai_lifecycle_registry,
                        actor=actor,
                        reason=stale_reason,
                    )
                else:
                    proposal.status = "rejected"  # type: ignore[assignment]
                    proposal.updated_at = _now_utc()
                    record_lifecycle(
                        app.state.ai_lifecycle_registry,
                        object_type="proposal",
                        object_id=proposal.proposal_id,
                        from_state=old_status,
                        to_state="rejected",
                        actor=actor,
                        reason=stale_reason,
                    )
                proposal.metadata["last_research_error"] = str(reason)
                save_proposal(app, proposal)
                return {
                    "proposal_id": proposal_id,
                    "cancelled": True,
                    "reason": "stale proposal status recovered",
                    "proposal_status": proposal.status,
                    "job_id": None,
                    "job_status": None,
                }
            return {
                "proposal_id": proposal_id,
                "cancelled": False,
                "reason": "no active research job",
            }

    assert job_id is not None and job is not None
    job["status"] = "cancelled"
    job["finished_at"] = _now_utc().isoformat()
    job["error"] = str(reason)
    job["result"] = None
    app.state.research_jobs[str(job_id)] = job

    task = app.state.research_job_tasks.get(str(job_id))
    if task is not None and not task.done():
        task.cancel()

    old_status = str(proposal.status)
    if old_status == "research_running":
        transition_proposal(
            proposal,
            to_state="rejected",
            lifecycle_registry=app.state.ai_lifecycle_registry,
            actor=actor,
            reason=str(reason),
        )
    elif old_status == "research_queued":
        proposal.status = "rejected"  # type: ignore[assignment]
        proposal.updated_at = _now_utc()
        record_lifecycle(
            app.state.ai_lifecycle_registry,
            object_type="proposal",
            object_id=proposal.proposal_id,
            from_state=old_status,
            to_state="rejected",
            actor=actor,
            reason=str(reason),
        )
    proposal.metadata["last_research_error"] = str(reason)
    save_proposal(app, proposal)

    experiment_id = str(job.get("experiment_id") or "")
    if experiment_id:
        experiment = app.state.ai_experiment_registry.get(experiment_id)
        if experiment is not None and str(experiment.status) in {"queued", "running"}:
            experiment.status = "failed"
            app.state.ai_experiment_registry.save(experiment)

    run_id = str(job.get("run_id") or "")
    if run_id:
        run = app.state.ai_experiment_run_registry.get(run_id)
        if run is not None and str(run.status) in {"queued", "running"}:
            run.status = "failed"
            run.finished_at = _now_utc()
            run.error = str(reason)
            app.state.ai_experiment_run_registry.save(run)

    _persist_research_jobs(app)
    return {
        "proposal_id": proposal_id,
        "cancelled": True,
        "job_id": str(job_id),
        "proposal_status": proposal.status,
        "job_status": job.get("status"),
        "reason": str(reason),
    }


async def promote_existing_candidate(
    app: FastAPI,
    *,
    candidate_id: str,
    actor: str,
    target: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(app)
    if bool(getattr(settings, "GOVERNANCE_ENABLED", True)):
        raise HTTPException(
            status_code=409,
            detail="governance enabled: use /ops/governance/strategy/* approvals before runtime promotion",
        )
    candidate = get_candidate(app, candidate_id)
    proposal = get_proposal(app, candidate.proposal_id)
    if candidate.promotion is None:
        if candidate.validation_summary is None:
            raise HTTPException(status_code=400, detail="candidate has no validation summary")
        candidate.promotion = build_promotion_decision(candidate.candidate_id, candidate.validation_summary)
    promotion = candidate.promotion
    if target:
        decision = str(target).strip()
        if decision == "shadow":
            decision = "paper"
        if decision not in {"paper", "live_candidate"}:
            raise HTTPException(status_code=400, detail="unsupported promotion target")
        paper_allocation_cap = max(0.0, min(1.0, float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15)))
        promotion.decision = decision
        promotion.constraints["allocation_cap"] = paper_allocation_cap if decision == "paper" else 0.0
        promotion.constraints["runtime_mode"] = "paper" if decision == "paper" else "candidate_only"
    elif str(promotion.decision) == "shadow":
        promotion.decision = "paper"
        promotion.constraints["allocation_cap"] = max(0.0, min(1.0, float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15)))
        promotion.constraints["runtime_mode"] = "paper"
    result = await promote_candidate(app, proposal=proposal, candidate=candidate, promotion=promotion, actor=actor)
    app.state.ai_candidate_registry.save(candidate)
    save_proposal(app, proposal)
    return result
