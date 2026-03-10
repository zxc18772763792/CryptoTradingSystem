"""Structured schemas for AI research proposals."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


ProposalState = Literal[
    "draft",
    "research_queued",
    "research_running",
    "validated",
    "rejected",
    "paper_running",
    "shadow_running",
    "live_candidate",
    "live_running",
    "retired",
]

ProposalSource = Literal["ai", "human", "hybrid"]
ValidationDecision = Literal["reject", "paper", "shadow", "live_candidate"]


class ProposalValidationSummary(BaseModel):
    computed_at: datetime
    decision: ValidationDecision = "reject"
    edge_score: float = 0.0
    risk_score: float = 0.0
    stability_score: float = 0.0
    efficiency_score: float = 0.0
    deployment_score: float = 0.0
    reasons: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    # IS/OOS/WF validation fields (C requirement)
    is_score: Optional[float] = None       # In-sample Sharpe ratio
    oos_score: Optional[float] = None      # Out-of-sample Sharpe ratio
    wf_stability: Optional[float] = None  # Walk-forward stability [0, 1]
    robustness_score: Optional[float] = None  # Combined OOS + WF robustness [0, 100]
    dsr_score: Optional[float] = None      # Deflated Sharpe Ratio (multiple testing correction) [0, 1]
    wf_consistency: Optional[float] = None  # Fraction of WF folds with positive return [0, 1]


class ResearchProposal(BaseModel):
    proposal_id: str
    created_at: datetime
    updated_at: datetime
    status: ProposalState = "draft"
    source: ProposalSource = "ai"
    thesis: str
    market_regime: str = "mixed"
    target_symbols: List[str] = Field(default_factory=list)
    target_timeframes: List[str] = Field(default_factory=list)
    strategy_templates: List[str] = Field(default_factory=list)
    # A requirement: filtered templates (dropped at planning time)
    filtered_templates: List[str] = Field(default_factory=list)
    filtered_reasons: Dict[str, str] = Field(default_factory=dict)
    parameter_space: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    required_features: List[str] = Field(default_factory=list)
    risk_hypothesis: str = ""
    invalidation_rules: List[str] = Field(default_factory=list)
    expected_holding_period: str = "1d"
    planner_version: str = "planner_v1"
    origin_context: Dict[str, Any] = Field(default_factory=dict)
    latest_experiment_id: Optional[str] = None
    latest_candidate_id: Optional[str] = None
    notes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    validation_summary: Optional[ProposalValidationSummary] = None
