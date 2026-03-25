"""Minimal Phase 3 search loop for autonomous research drafting.

This module keeps the loop intentionally simple:
    hypothesis -> seed draft -> mutate -> score -> accept/reject

It does not attempt open-ended code generation. The goal is to turn a single
LLM draft into a small, traceable search batch with novelty constraints and
clear champion/challenger labels.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config.strategy_registry import get_backtest_optimization_grid
from core.ai.proposal_schemas import (
    ResearchSearchBudget,
    ResearchSearchSummary,
    SearchDraftEvaluation,
    StrategyDraft,
)
from core.research.strategy_program import build_strategy_program_from_draft


_SUPPORTED_TEMPLATE_SEEDS = {
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "MeanReversionStrategy",
    "MomentumStrategy",
}


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _slug_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    out = []
    prev_sep = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_sep = False
        elif ch in {"_", "-"} and not prev_sep:
            out.append("_")
            prev_sep = True
        elif ch.isspace() and not prev_sep:
            out.append("_")
            prev_sep = True
    return "".join(out).strip("_") or "draft"


def _param_defaults(template: str, exploration_bias: float) -> Dict[str, Any]:
    grid = dict(get_backtest_optimization_grid(template) or {})
    out: Dict[str, Any] = {}
    bias = max(0.0, min(1.0, float(exploration_bias or 0.0)))
    for key, values in grid.items():
        if not isinstance(values, list) or not values:
            out[key] = values
            continue
        if bias >= 0.6:
            idx = max(0, min(len(values) - 1, int(round((len(values) - 1) * 0.75))))
        else:
            idx = len(values) // 2
        out[key] = values[idx]
    return out


def _seed_change_for_template(
    template: str,
    *,
    goal: str,
    order: int,
    params: Dict[str, Any],
    parameter_space: Dict[str, Any],
    mutation_notes: Optional[List[str]] = None,
    parent_draft_id: Optional[str] = None,
    generation: int = 0,
    confidence: float = 0.48,
) -> Optional[Dict[str, Any]]:
    title = f"{template} Search Seed {order + 1}"
    base_tags = ["search_loop", "template_seed", template]
    notes = list(mutation_notes or [])

    if template == "MAStrategy":
        return {
            "draft_id": f"seed-ma-{order + 1:02d}",
            "name": title,
            "strategy": template,
            "thesis": goal,
            "rationale": "Template seed promoted into autonomous search.",
            "features": ["ma_fast", "ma_slow"],
            "entry_logic": ["cross_over(ma_fast, ma_slow)"],
            "exit_logic": ["cross_under(ma_fast, ma_slow)"],
            "params": params,
            "confidence": confidence,
            "tags": base_tags,
            "program": {
                "name": title,
                "indicators": [
                    {"name": "ma_fast", "kind": "sma", "period": int(params.get("fast_period", 10))},
                    {"name": "ma_slow", "kind": "sma", "period": int(params.get("slow_period", 30))},
                ],
                "entry_conditions": [{"left": "ma_fast", "op": "cross_over", "right": "ma_slow"}],
                "exit_conditions": [{"left": "ma_fast", "op": "cross_under", "right": "ma_slow"}],
                "parameter_space": parameter_space,
            },
            "mutation_notes": notes,
            "parent_draft_id": parent_draft_id,
            "generation": generation,
        }
    if template == "EMAStrategy":
        return {
            "draft_id": f"seed-ema-{order + 1:02d}",
            "name": title,
            "strategy": template,
            "thesis": goal,
            "rationale": "EMA crossover seed for autonomous search.",
            "features": ["ema_fast", "ema_slow"],
            "entry_logic": ["cross_over(ema_fast, ema_slow)"],
            "exit_logic": ["cross_under(ema_fast, ema_slow)"],
            "params": params,
            "confidence": confidence,
            "tags": base_tags,
            "program": {
                "name": title,
                "indicators": [
                    {"name": "ema_fast", "kind": "ema", "period": int(params.get("fast_period", 12))},
                    {"name": "ema_slow", "kind": "ema", "period": int(params.get("slow_period", 26))},
                ],
                "entry_conditions": [{"left": "ema_fast", "op": "cross_over", "right": "ema_slow"}],
                "exit_conditions": [{"left": "ema_fast", "op": "cross_under", "right": "ema_slow"}],
                "parameter_space": parameter_space,
            },
            "mutation_notes": notes,
            "parent_draft_id": parent_draft_id,
            "generation": generation,
        }
    if template == "RSIStrategy":
        return {
            "draft_id": f"seed-rsi-{order + 1:02d}",
            "name": title,
            "strategy": template,
            "thesis": goal,
            "rationale": "RSI mean-reversion seed for autonomous search.",
            "features": ["rsi"],
            "entry_logic": [f"rsi <= {int(params.get('oversold', 30))}"],
            "exit_logic": [f"rsi >= {int(params.get('overbought', 70))}"],
            "params": params,
            "confidence": confidence,
            "tags": base_tags,
            "program": {
                "name": title,
                "indicators": [
                    {"name": "rsi", "kind": "rsi", "period": int(params.get("period", 14))},
                ],
                "entry_conditions": [{"left": "rsi", "op": "lte", "right": int(params.get("oversold", 30))}],
                "exit_conditions": [{"left": "rsi", "op": "gte", "right": int(params.get("overbought", 70))}],
                "parameter_space": parameter_space,
            },
            "mutation_notes": notes,
            "parent_draft_id": parent_draft_id,
            "generation": generation,
        }
    if template == "MeanReversionStrategy":
        return {
            "draft_id": f"seed-zscore-{order + 1:02d}",
            "name": title,
            "strategy": template,
            "thesis": goal,
            "rationale": "Z-score mean-reversion seed for autonomous search.",
            "features": ["close_zscore"],
            "entry_logic": [f"close_zscore <= {-abs(float(params.get('entry_z_score', 2.0))):.2f}"],
            "exit_logic": ["close_zscore >= 0"],
            "params": params,
            "confidence": confidence,
            "tags": base_tags,
            "program": {
                "name": title,
                "indicators": [
                    {
                        "name": "close_zscore",
                        "kind": "zscore",
                        "period": int(params.get("lookback_period", 20)),
                    },
                ],
                "entry_conditions": [
                    {"left": "close_zscore", "op": "lte", "right": -abs(float(params.get("entry_z_score", 2.0)))}
                ],
                "exit_conditions": [{"left": "close_zscore", "op": "gte", "right": 0.0}],
                "parameter_space": parameter_space,
            },
            "mutation_notes": notes,
            "parent_draft_id": parent_draft_id,
            "generation": generation,
        }
    if template == "MomentumStrategy":
        lookback = int(params.get("lookback_period", 14))
        threshold = float(params.get("momentum_threshold", 0.02))
        return {
            "draft_id": f"seed-mom-{order + 1:02d}",
            "name": title,
            "strategy": template,
            "thesis": goal,
            "rationale": "Returns-momentum seed for autonomous search.",
            "features": [f"returns_{lookback}"],
            "entry_logic": [f"returns_{lookback} >= {threshold:.4f}"],
            "exit_logic": [f"returns_{lookback} <= {-threshold * 0.5:.4f}"],
            "params": params,
            "confidence": confidence,
            "tags": base_tags,
            "program": {
                "name": title,
                "indicators": [
                    {"name": f"returns_{lookback}", "kind": "returns", "period": lookback},
                ],
                "entry_conditions": [{"left": f"returns_{lookback}", "op": "gte", "right": threshold}],
                "exit_conditions": [{"left": f"returns_{lookback}", "op": "lte", "right": -threshold * 0.5}],
                "parameter_space": parameter_space,
            },
            "mutation_notes": notes,
            "parent_draft_id": parent_draft_id,
            "generation": generation,
        }
    return None


def _coerce_text_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return _dedupe_keep_order(str(item or "").strip() for item in value if str(item or "").strip())
    text = str(value or "").strip()
    return [text] if text else []


def _draft_from_change(change: Dict[str, Any], *, goal: str, index: int) -> StrategyDraft:
    template_hint = str(change.get("strategy") or change.get("strategy_template") or "").strip()
    name = str(change.get("name") or change.get("title") or template_hint or f"Draft {index + 1}").strip()
    params = dict(change.get("params") or {})
    tags = _dedupe_keep_order(_coerce_text_list(change.get("tags")))
    features = _dedupe_keep_order(_coerce_text_list(change.get("features")))
    entry_logic = _dedupe_keep_order(_coerce_text_list(change.get("entry_logic")))
    exit_logic = _dedupe_keep_order(_coerce_text_list(change.get("exit_logic")))
    risk_logic = _dedupe_keep_order(_coerce_text_list(change.get("risk_logic")))
    mutation_notes = _dedupe_keep_order(_coerce_text_list(change.get("mutation_notes")))
    critique = _dedupe_keep_order(_coerce_text_list(change.get("critique")))
    try:
        confidence = max(0.0, min(1.0, float(change.get("confidence", 0.0) or 0.0)))
    except Exception:
        confidence = 0.0
    try:
        generation = max(0, int(change.get("generation", 0) or 0))
    except Exception:
        generation = 0

    program = build_strategy_program_from_draft(
        raw_change=change,
        draft_id=str(change.get("draft_id") or f"draft-{index + 1:02d}"),
        draft_name=name,
        thesis=str(change.get("thesis") or goal or "").strip(),
        template_hint=template_hint,
        features=features,
        entry_logic=entry_logic,
        exit_logic=exit_logic,
        params=params,
        tags=tags,
    )
    if template_hint and (features or entry_logic or exit_logic or params):
        mode = "hybrid_seed"
    elif template_hint:
        mode = "template_seed"
    else:
        mode = "dsl_seed"

    return StrategyDraft(
        draft_id=str(change.get("draft_id") or f"draft-{index + 1:02d}"),
        name=name,
        mode=mode,
        template_hint=template_hint,
        thesis=str(change.get("thesis") or goal or "").strip(),
        rationale=str(change.get("rationale") or "").strip(),
        features=features,
        entry_logic=entry_logic,
        exit_logic=exit_logic,
        risk_logic=risk_logic,
        params=params,
        program=program,
        confidence=confidence,
        tags=tags,
        source=str(change.get("source") or "search_loop"),
        parent_draft_id=str(change.get("parent_draft_id") or "").strip() or None,
        generation=generation,
        mutation_notes=mutation_notes,
        critique=critique,
        heuristic_score=float(change.get("heuristic_score", 0.0) or 0.0),
        novelty_score=float(change.get("novelty_score", 0.0) or 0.0),
        selection_status=str(change.get("selection_status") or "seed"),
        rejection_reason=str(change.get("rejection_reason") or ""),
    )


def _draft_signature(draft: StrategyDraft) -> Tuple[str, str, Tuple[Tuple[str, str], ...], Tuple[str, ...], Tuple[str, ...]]:
    return (
        str(draft.template_hint or ""),
        str(draft.program.program_id if draft.program else ""),
        tuple(sorted((str(k), str(v)) for k, v in dict(draft.params or {}).items())),
        tuple(str(item) for item in list(draft.entry_logic or [])),
        tuple(str(item) for item in list(draft.exit_logic or [])),
    )


def _candidate_pool(
    *,
    goal: str,
    selected_templates: List[str],
    base_drafts: List[StrategyDraft],
    search_budget: ResearchSearchBudget,
) -> tuple[List[StrategyDraft], List[str]]:
    notes: List[str] = []
    pool: List[StrategyDraft] = [draft.model_copy(deep=True) for draft in list(base_drafts or [])]
    represented_templates = {
        str(draft.template_hint or "").strip()
        for draft in pool
        if str(draft.template_hint or "").strip()
    }

    for idx, template in enumerate(selected_templates or []):
        if len(pool) >= max(int(search_budget.max_strategy_drafts or 0) * 2, int(search_budget.max_strategy_drafts or 0) + 2):
            break
        if template in represented_templates:
            continue
        if template not in _SUPPORTED_TEMPLATE_SEEDS:
            continue
        params = _param_defaults(template, float(search_budget.exploration_bias or 0.0))
        parameter_space = dict(get_backtest_optimization_grid(template) or {})
        change = _seed_change_for_template(
            template,
            goal=goal,
            order=idx,
            params=params,
            parameter_space=parameter_space,
        )
        if change is None:
            continue
        draft = _draft_from_change(change, goal=goal, index=len(pool))
        pool.append(draft)
        represented_templates.add(template)
    if len(pool) > len(base_drafts):
        notes.append(f"template_seeds_added={len(pool) - len(base_drafts)}")

    mutated: List[StrategyDraft] = []
    for base in list(pool):
        if len(pool) + len(mutated) >= max(int(search_budget.max_strategy_drafts or 0) * 2, int(search_budget.max_strategy_drafts or 0) + 2):
            break
        template = str(base.template_hint or "").strip()
        if template not in _SUPPORTED_TEMPLATE_SEEDS:
            continue
        grid = dict(get_backtest_optimization_grid(template) or {})
        if not grid:
            continue
        current_params = dict(base.params or {})
        param_name = next((key for key, values in grid.items() if isinstance(values, list) and len(values) > 1), None)
        if not param_name:
            continue
        values = list(grid[param_name])
        current_value = current_params.get(param_name, values[len(values) // 2])
        try:
            idx = values.index(current_value)
        except Exception:
            idx = len(values) // 2
        delta = 1 if float(search_budget.exploration_bias or 0.0) < 0.5 else max(1, len(values) // 2)
        next_idx = max(0, min(len(values) - 1, idx + delta if idx + delta < len(values) else idx - delta))
        if values[next_idx] == current_value:
            continue
        mutated_params = dict(current_params)
        mutated_params[param_name] = values[next_idx]
        mutation_note = f"{param_name}: {current_value} -> {values[next_idx]}"
        change = _seed_change_for_template(
            template,
            goal=base.thesis or goal,
            order=len(mutated),
            params=mutated_params,
            parameter_space=grid,
            mutation_notes=list(base.mutation_notes or []) + [mutation_note],
            parent_draft_id=base.draft_id,
            generation=int(base.generation or 0) + 1,
            confidence=max(0.35, min(0.95, float(base.confidence or 0.45) + 0.03)),
        )
        if change is None:
            continue
        change["name"] = f"{base.name} Mutant {len(mutated) + 1}"
        change["rationale"] = f"Mutated from {base.name} for broader search coverage."
        mutated.append(_draft_from_change(change, goal=goal, index=len(pool) + len(mutated)))
    if mutated:
        notes.append(f"mutations_generated={len(mutated)}")
    pool.extend(mutated)
    return pool, notes


def run_research_search_loop(
    *,
    goal: str,
    selected_templates: List[str],
    base_drafts: List[StrategyDraft],
    search_budget: ResearchSearchBudget,
    enabled: bool,
) -> tuple[List[StrategyDraft], ResearchSearchSummary]:
    if not enabled:
        accepted = [draft.model_copy(deep=True) for draft in list(base_drafts or [])]
        for idx, draft in enumerate(accepted):
            draft.selection_status = "champion" if idx == 0 else "accepted"
        summary = ResearchSearchSummary(
            loop_enabled=False,
            evaluated_drafts=len(accepted),
            accepted_drafts=len(accepted),
            rejected_drafts=0,
            champion_draft_id=accepted[0].draft_id if accepted else None,
            challenger_draft_ids=[],
            draft_evaluations=[
                SearchDraftEvaluation(
                    draft_id=draft.draft_id,
                    name=draft.name,
                    template_hint=draft.template_hint,
                    generation=int(draft.generation or 0),
                    heuristic_score=float(draft.heuristic_score or 0.0),
                    novelty_score=float(draft.novelty_score or 0.0),
                    selection_status=str(draft.selection_status or "accepted"),
                    critique=list(draft.critique or []),
                    mutation_notes=list(draft.mutation_notes or []),
                )
                for draft in accepted
            ],
            notes=["search_loop_disabled"],
        )
        return accepted, summary

    pool, notes = _candidate_pool(
        goal=goal,
        selected_templates=selected_templates,
        base_drafts=base_drafts,
        search_budget=search_budget,
    )
    evaluated: List[SearchDraftEvaluation] = []
    accepted: List[StrategyDraft] = []
    seen_signatures: set[Tuple[str, str, Tuple[Tuple[str, str], ...], Tuple[str, ...], Tuple[str, ...]]] = set()
    template_counts: Counter[str] = Counter()

    scored_rows: List[Tuple[float, StrategyDraft, str]] = []
    for draft in pool:
        critiques = list(draft.critique or [])
        template = str(draft.template_hint or "").strip()
        signature = _draft_signature(draft)
        exact_duplicate = signature in seen_signatures
        same_template_seen = template_counts.get(template, 0) > 0 if template else False
        novelty_score = 1.0
        rejection_reason = ""
        if exact_duplicate:
            novelty_score = 0.0
            rejection_reason = "duplicate_signature"
            critiques.append("same signature as an earlier draft")
        elif same_template_seen:
            novelty_score = 0.58
            critiques.append("shares template family with an earlier draft")
        elif not draft.program:
            novelty_score = 0.42
            critiques.append("not yet executable as a strategy program")

        if not draft.exit_logic and not (draft.program and draft.program.exit_conditions):
            critiques.append("missing explicit exit logic")
        if not draft.params:
            critiques.append("parameter surface is narrow")
        if float(draft.confidence or 0.0) < 0.4:
            critiques.append("low prior confidence")

        heuristic_score = (
            float(draft.confidence or 0.0) * 34.0
            + (22.0 if draft.program is not None else 0.0)
            + novelty_score * 28.0
            + (8.0 if draft.exit_logic or (draft.program and draft.program.exit_conditions) else 0.0)
            + (8.0 if draft.params else 0.0)
        )
        if str(draft.source or "").strip() and str(draft.source or "").strip() != "search_loop":
            heuristic_score += 6.0
        if draft.parent_draft_id:
            heuristic_score += 4.0
        if template and template_counts.get(template, 0) == 0:
            heuristic_score += 3.0
        heuristic_score = round(max(0.0, min(100.0, heuristic_score)), 2)

        draft.critique = _dedupe_keep_order(critiques)
        draft.novelty_score = round(max(0.0, min(1.0, novelty_score)), 3)
        draft.heuristic_score = heuristic_score
        scored_rows.append((heuristic_score, draft, rejection_reason))

        if not exact_duplicate:
            seen_signatures.add(signature)
            if template:
                template_counts[template] += 1

    scored_rows.sort(key=lambda item: item[0], reverse=True)
    max_accept = max(1, min(int(search_budget.max_strategy_drafts or 3), 12))
    rejected_reason_counts: Counter[str] = Counter()

    for rank, (_, draft, rejection_reason) in enumerate(scored_rows):
        if rejection_reason:
            draft.selection_status = "rejected"
            draft.rejection_reason = rejection_reason
            rejected_reason_counts[rejection_reason] += 1
        elif len(accepted) >= max_accept:
            draft.selection_status = "rejected"
            draft.rejection_reason = "budget_trimmed"
            rejected_reason_counts["budget_trimmed"] += 1
        else:
            draft.selection_status = "accepted"
            draft.rejection_reason = ""
            accepted.append(draft)

        evaluated.append(
            SearchDraftEvaluation(
                draft_id=draft.draft_id,
                name=draft.name,
                template_hint=draft.template_hint,
                parent_draft_id=draft.parent_draft_id,
                generation=int(draft.generation or 0),
                heuristic_score=float(draft.heuristic_score or 0.0),
                novelty_score=float(draft.novelty_score or 0.0),
                selection_status=str(draft.selection_status or "accepted"),
                rejection_reason=str(draft.rejection_reason or ""),
                critique=list(draft.critique or []),
                mutation_notes=list(draft.mutation_notes or []),
            )
        )

    champion_draft_id: Optional[str] = None
    challenger_draft_ids: List[str] = []
    if accepted:
        accepted[0].selection_status = "champion"
        champion_draft_id = accepted[0].draft_id
        for draft in accepted[1:]:
            draft.selection_status = "challenger"
            challenger_draft_ids.append(draft.draft_id)

    accepted_eval_map = {row.draft_id: row for row in evaluated}
    if champion_draft_id and champion_draft_id in accepted_eval_map:
        accepted_eval_map[champion_draft_id].selection_status = "champion"
    for draft_id in challenger_draft_ids:
        if draft_id in accepted_eval_map:
            accepted_eval_map[draft_id].selection_status = "challenger"

    notes.extend(
        [
            f"accepted={len(accepted)}",
            f"rejected={len(evaluated) - len(accepted)}",
        ]
    )
    if champion_draft_id:
        notes.append(f"champion={champion_draft_id}")

    summary = ResearchSearchSummary(
        loop_enabled=True,
        evaluated_drafts=len(evaluated),
        accepted_drafts=len(accepted),
        rejected_drafts=max(0, len(evaluated) - len(accepted)),
        champion_draft_id=champion_draft_id,
        challenger_draft_ids=challenger_draft_ids,
        rejected_reason_counts=dict(rejected_reason_counts),
        draft_evaluations=evaluated,
        notes=notes,
    )
    return accepted, summary
