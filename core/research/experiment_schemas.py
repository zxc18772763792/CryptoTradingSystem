"""Schemas for AI research experiments, candidates, and lifecycle tracking."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from core.ai.proposal_schemas import (
    ProposalValidationSummary,
    ResearchLineage,
    ResearchMode,
    ResearchSearchSummary,
    ResearchSearchBudget,
    StrategyDraft,
    StrategyProgram,
)


ExperimentStatus = Literal["queued", "running", "completed", "failed"]
CandidateStatus = Literal["new", "paper_running", "shadow_running", "live_candidate", "live_running", "retired"]
PromotionTarget = Literal["paper", "shadow", "live_candidate"]
PromotionDecisionType = Literal["reject", "paper", "shadow", "live_candidate"]
LifecycleObjectType = Literal["proposal", "experiment", "candidate"]


class ExperimentSpec(BaseModel):
    experiment_id: str
    proposal_id: str
    created_at: datetime
    exchange: str
    symbol: str
    research_mode: ResearchMode = "template"
    timeframes: List[str] = Field(default_factory=list)
    strategies: List[str] = Field(default_factory=list)
    strategy_drafts: List[StrategyDraft] = Field(default_factory=list)
    strategy_programs: List[StrategyProgram] = Field(default_factory=list)
    parameter_space: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    search_summary: Optional[ResearchSearchSummary] = None
    days: int = 90
    initial_capital: float = 10000.0
    commission_rate: float = 0.0004
    slippage_bps: float = 2.0
    research_profile: Literal["fast", "standard", "strict"] = "standard"
    search_budget: ResearchSearchBudget = Field(default_factory=ResearchSearchBudget)
    lineage: Optional[ResearchLineage] = None
    status: ExperimentStatus = "queued"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExperimentRun(BaseModel):
    run_id: str
    experiment_id: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    status: ExperimentStatus = "queued"
    result: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class PromotionDecision(BaseModel):
    candidate_id: str
    decision: PromotionDecisionType = "reject"
    reason: str
    constraints: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class StrategyCandidate(BaseModel):
    candidate_id: str
    proposal_id: str
    experiment_id: str
    created_at: datetime
    strategy: str
    timeframe: str
    symbol: str
    params: Dict[str, Any] = Field(default_factory=dict)
    score: float = 0.0
    validation_summary: Optional[ProposalValidationSummary] = None
    promotion: Optional[PromotionDecision] = None
    promotion_target: Optional[PromotionTarget] = None
    status: CandidateStatus = "new"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LifecycleRecord(BaseModel):
    object_type: LifecycleObjectType
    object_id: str
    from_state: Optional[str] = None
    to_state: str
    actor: str
    ts: datetime
    reason: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
