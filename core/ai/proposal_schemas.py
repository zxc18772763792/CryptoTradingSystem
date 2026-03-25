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
ResearchMode = Literal["template", "hybrid", "autonomous_draft"]
StrategyDraftMode = Literal["template_seed", "hybrid_seed", "dsl_seed"]
StrategyIndicatorKind = Literal["price", "sma", "ema", "rsi", "zscore", "returns"]
StrategyConditionOp = Literal["gt", "gte", "lt", "lte", "cross_over", "cross_under"]
StrategyConditionCombine = Literal["all", "any"]
StrategyExecutionMode = Literal["stateful_long", "signal_long"]


class StrategyIndicatorSpec(BaseModel):
    name: str
    kind: StrategyIndicatorKind = "price"
    source: str = "close"
    period: Optional[int] = None


class StrategyCondition(BaseModel):
    left: str
    op: StrategyConditionOp = "gt"
    right: Any


class StrategyProgram(BaseModel):
    program_id: str = ""
    name: str = ""
    description: str = ""
    indicators: List[StrategyIndicatorSpec] = Field(default_factory=list)
    entry_conditions: List[StrategyCondition] = Field(default_factory=list)
    exit_conditions: List[StrategyCondition] = Field(default_factory=list)
    entry_combine: StrategyConditionCombine = "all"
    exit_combine: StrategyConditionCombine = "any"
    execution_mode: StrategyExecutionMode = "stateful_long"
    params: Dict[str, Any] = Field(default_factory=dict)
    parameter_space: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    source: str = "llm"


class StrategyDraft(BaseModel):
    draft_id: str = ""
    name: str = ""
    mode: StrategyDraftMode = "dsl_seed"
    template_hint: str = ""
    thesis: str = ""
    rationale: str = ""
    features: List[str] = Field(default_factory=list)
    entry_logic: List[str] = Field(default_factory=list)
    exit_logic: List[str] = Field(default_factory=list)
    risk_logic: List[str] = Field(default_factory=list)
    params: Dict[str, Any] = Field(default_factory=dict)
    program: Optional[StrategyProgram] = None
    confidence: float = 0.0
    tags: List[str] = Field(default_factory=list)
    source: str = "llm"


class ResearchSearchBudget(BaseModel):
    max_templates: int = 5
    max_strategy_drafts: int = 3
    max_backtest_runs: int = 60
    exploration_bias: float = 0.35
    notes: List[str] = Field(default_factory=list)


class ResearchLineage(BaseModel):
    lineage_id: str = ""
    parent_proposal_id: Optional[str] = None
    parent_candidate_id: Optional[str] = None
    generation: int = 0
    mutation_notes: List[str] = Field(default_factory=list)


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
    research_mode: ResearchMode = "template"
    thesis: str
    market_regime: str = "mixed"
    target_symbols: List[str] = Field(default_factory=list)
    target_timeframes: List[str] = Field(default_factory=list)
    strategy_templates: List[str] = Field(default_factory=list)
    strategy_drafts: List[StrategyDraft] = Field(default_factory=list)
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
    search_budget: ResearchSearchBudget = Field(default_factory=ResearchSearchBudget)
    lineage: Optional[ResearchLineage] = None
    validation_summary: Optional[ProposalValidationSummary] = None
