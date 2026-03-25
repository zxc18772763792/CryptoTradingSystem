from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from core.ai.proposal_schemas import (
    StrategyCondition,
    StrategyIndicatorSpec,
    StrategyProgram,
)


_OPERAND_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-\.]*$")
_COMPARE_RE = re.compile(r"^\s*([a-zA-Z_][a-zA-Z0-9_\-\.]*)\s*(>=|<=|>|<)\s*([a-zA-Z0-9_\-\.\+]+)\s*$")
_CROSS_RE = re.compile(
    r"^\s*(cross_over|crossunder|cross_under|crosses_above|crosses_below)\(\s*([^,\)]+)\s*,\s*([^\)]+)\s*\)\s*$",
    re.IGNORECASE,
)


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _slug_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text.replace("-", "_").replace(" ", "_"))
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def program_strategy_name(program: StrategyProgram, *, fallback: str = "OpenAI Draft Strategy") -> str:
    display = str(program.name or fallback).strip() or fallback
    return display


def _normalize_indicator_payload(item: Dict[str, Any]) -> Optional[StrategyIndicatorSpec]:
    name = _slug_token(item.get("name") or item.get("alias") or "")
    kind = str(item.get("kind") or item.get("type") or "").strip().lower()
    if not name or kind not in {"price", "sma", "ema", "rsi", "zscore", "returns"}:
        return None
    period_raw = item.get("period")
    try:
        period = int(period_raw) if period_raw is not None else None
    except Exception:
        period = None
    return StrategyIndicatorSpec(
        name=name,
        kind=kind,
        source=str(item.get("source") or "close").strip().lower() or "close",
        period=period,
    )


def _normalize_condition_payload(item: Dict[str, Any]) -> Optional[StrategyCondition]:
    left = _slug_token(item.get("left") or "")
    op = str(item.get("op") or "").strip().lower()
    right = item.get("right")
    if not left or op not in {"gt", "gte", "lt", "lte", "cross_over", "cross_under"}:
        return None
    if isinstance(right, str):
        right = _slug_token(right) or right.strip()
    return StrategyCondition(left=left, op=op, right=right)


def _infer_indicator_from_token(token: str, params: Dict[str, Any], template_hint: str = "") -> Optional[StrategyIndicatorSpec]:
    alias = _slug_token(token)
    if not alias or alias in {"open", "high", "low", "close", "volume"}:
        return None

    tmpl = str(template_hint or "").strip().lower()

    def _param_int(name: str, default: int) -> int:
        try:
            return max(1, int(params.get(name, default) or default))
        except Exception:
            return default

    m = re.fullmatch(r"(?:sma|ma)[_\-]?(\d+)", alias)
    if m:
        return StrategyIndicatorSpec(name=alias, kind="sma", period=int(m.group(1)))
    if alias in {"ma_fast", "sma_fast", "fast_ma", "fast_sma"}:
        return StrategyIndicatorSpec(name=alias, kind="sma", period=_param_int("fast_period", 10))
    if alias in {"ma_slow", "sma_slow", "slow_ma", "slow_sma"}:
        return StrategyIndicatorSpec(name=alias, kind="sma", period=_param_int("slow_period", 30))

    m = re.fullmatch(r"ema[_\-]?(\d+)", alias)
    if m:
        return StrategyIndicatorSpec(name=alias, kind="ema", period=int(m.group(1)))
    if alias in {"ema_fast", "fast_ema"}:
        return StrategyIndicatorSpec(name=alias, kind="ema", period=_param_int("fast_period", 12))
    if alias in {"ema_slow", "slow_ema"}:
        return StrategyIndicatorSpec(name=alias, kind="ema", period=_param_int("slow_period", 26))

    m = re.fullmatch(r"rsi(?:[_\-]?(\d+))?", alias)
    if m:
        period = int(m.group(1)) if m.group(1) else _param_int("period", 14)
        return StrategyIndicatorSpec(name=alias, kind="rsi", period=period)

    m = re.fullmatch(r"(?:close_)?zscore(?:[_\-]?(\d+))?", alias)
    if m:
        period = int(m.group(1)) if m.group(1) else _param_int("window", _param_int("lookback_period", 20))
        return StrategyIndicatorSpec(name=alias, kind="zscore", period=period)

    m = re.fullmatch(r"(?:returns|roc)(?:[_\-]?(\d+))?", alias)
    if m:
        period = int(m.group(1)) if m.group(1) else _param_int("period", 1)
        return StrategyIndicatorSpec(name=alias, kind="returns", period=period)

    if "ema" in alias or tmpl.startswith("ema"):
        if "fast" in alias:
            return StrategyIndicatorSpec(name=alias, kind="ema", period=_param_int("fast_period", 12))
        if "slow" in alias:
            return StrategyIndicatorSpec(name=alias, kind="ema", period=_param_int("slow_period", 26))
    if "ma" in alias or tmpl.startswith("ma"):
        if "fast" in alias:
            return StrategyIndicatorSpec(name=alias, kind="sma", period=_param_int("fast_period", 10))
        if "slow" in alias:
            return StrategyIndicatorSpec(name=alias, kind="sma", period=_param_int("slow_period", 30))

    return None


def _parse_condition_text(text: str) -> Optional[StrategyCondition]:
    raw = str(text or "").strip()
    if not raw:
        return None

    cross_match = _CROSS_RE.fullmatch(raw)
    if cross_match:
        op_raw = cross_match.group(1).strip().lower()
        op = "cross_over" if op_raw in {"cross_over", "crosses_above"} else "cross_under"
        return StrategyCondition(
            left=_slug_token(cross_match.group(2)),
            op=op,
            right=_slug_token(cross_match.group(3)),
        )

    compare_match = _COMPARE_RE.fullmatch(raw)
    if compare_match:
        op_map = {
            ">": "gt",
            ">=": "gte",
            "<": "lt",
            "<=": "lte",
        }
        right_raw = compare_match.group(3).strip()
        try:
            right: Any = float(right_raw)
            if right.is_integer():
                right = int(right)
        except Exception:
            right = _slug_token(right_raw)
        return StrategyCondition(
            left=_slug_token(compare_match.group(1)),
            op=op_map[compare_match.group(2)],
            right=right,
        )

    return None


def _normalize_conditions(raw_items: Any) -> List[StrategyCondition]:
    out: List[StrategyCondition] = []
    if isinstance(raw_items, list):
        for item in raw_items:
            if isinstance(item, dict):
                cond = _normalize_condition_payload(item)
            else:
                cond = _parse_condition_text(str(item or ""))
            if cond is not None:
                out.append(cond)
    elif isinstance(raw_items, dict):
        cond = _normalize_condition_payload(raw_items)
        if cond is not None:
            out.append(cond)
    else:
        cond = _parse_condition_text(str(raw_items or ""))
        if cond is not None:
            out.append(cond)
    return out


def _conditions_identifiers(conditions: List[StrategyCondition]) -> List[str]:
    tokens: List[str] = []
    for cond in conditions or []:
        tokens.append(str(cond.left or ""))
        if isinstance(cond.right, str) and _OPERAND_RE.fullmatch(str(cond.right)):
            tokens.append(str(cond.right))
    return tokens


def _coerce_program_from_payload(
    raw_program: Any,
    *,
    fallback_id: str,
    fallback_name: str,
    fallback_description: str,
    fallback_params: Dict[str, Any],
    fallback_tags: List[str],
) -> Optional[StrategyProgram]:
    if not isinstance(raw_program, dict):
        return None

    indicators_raw = raw_program.get("indicators") or []
    indicators = [
        spec
        for spec in (_normalize_indicator_payload(dict(item or {})) for item in indicators_raw if isinstance(item, dict))
        if spec is not None
    ]
    entry_conditions = _normalize_conditions(
        raw_program.get("entry_conditions") or raw_program.get("entry") or raw_program.get("entries")
    )
    exit_conditions = _normalize_conditions(
        raw_program.get("exit_conditions") or raw_program.get("exit") or raw_program.get("exits")
    )
    if not entry_conditions:
        return None

    return StrategyProgram(
        program_id=str(raw_program.get("program_id") or fallback_id),
        name=str(raw_program.get("name") or fallback_name),
        description=str(raw_program.get("description") or fallback_description),
        indicators=indicators,
        entry_conditions=entry_conditions,
        exit_conditions=exit_conditions,
        entry_combine=str(raw_program.get("entry_combine") or "all").strip().lower() if str(raw_program.get("entry_combine") or "all").strip().lower() in {"all", "any"} else "all",
        exit_combine=str(raw_program.get("exit_combine") or "any").strip().lower() if str(raw_program.get("exit_combine") or "any").strip().lower() in {"all", "any"} else "any",
        execution_mode=str(raw_program.get("execution_mode") or "stateful_long").strip().lower() if str(raw_program.get("execution_mode") or "stateful_long").strip().lower() in {"stateful_long", "signal_long"} else "stateful_long",
        params={**dict(fallback_params or {}), **dict(raw_program.get("params") or {})},
        parameter_space=dict(raw_program.get("parameter_space") or {}),
        tags=_dedupe_keep_order(list(fallback_tags or []) + list(raw_program.get("tags") or [])),
        source=str(raw_program.get("source") or "llm"),
    )


def build_strategy_program_from_draft(
    *,
    raw_change: Dict[str, Any],
    draft_id: str,
    draft_name: str,
    thesis: str,
    template_hint: str,
    features: List[str],
    entry_logic: List[str],
    exit_logic: List[str],
    params: Dict[str, Any],
    tags: List[str],
) -> Optional[StrategyProgram]:
    program = _coerce_program_from_payload(
        raw_change.get("program") or raw_change.get("dsl") or raw_change.get("strategy_program"),
        fallback_id=draft_id,
        fallback_name=draft_name,
        fallback_description=thesis,
        fallback_params=params,
        fallback_tags=tags,
    )
    if program is not None:
        return program

    entry_conditions = _normalize_conditions(entry_logic)
    exit_conditions = _normalize_conditions(exit_logic)

    # Template-derived fallback for crossover strategies when the LLM only gave short text.
    tmpl = str(template_hint or "").strip().lower()
    if not entry_conditions and ("ma" in tmpl or "ema" in tmpl):
        left = "ema_fast" if "ema" in tmpl else "ma_fast"
        right = "ema_slow" if "ema" in tmpl else "ma_slow"
        entry_conditions = [StrategyCondition(left=left, op="cross_over", right=right)]
        if not exit_conditions:
            exit_conditions = [StrategyCondition(left=left, op="cross_under", right=right)]
    if not entry_conditions and "rsi" in tmpl:
        entry_conditions = [StrategyCondition(left="rsi", op="lte", right=int(params.get("oversold", 30) or 30))]
        if not exit_conditions:
            exit_conditions = [StrategyCondition(left="rsi", op="gte", right=int(params.get("overbought", 70) or 70))]

    if not entry_conditions:
        return None

    token_pool = _dedupe_keep_order(
        list(features or [])
        + _conditions_identifiers(entry_conditions)
        + _conditions_identifiers(exit_conditions)
    )
    indicators: List[StrategyIndicatorSpec] = []
    for token in token_pool:
        spec = _infer_indicator_from_token(token, params, template_hint=template_hint)
        if spec is not None:
            indicators.append(spec)

    return StrategyProgram(
        program_id=draft_id,
        name=draft_name,
        description=thesis,
        indicators=_dedupe_indicator_specs(indicators),
        entry_conditions=entry_conditions,
        exit_conditions=exit_conditions,
        entry_combine="all",
        exit_combine="any",
        execution_mode="stateful_long",
        params=dict(params or {}),
        parameter_space={},
        tags=_dedupe_keep_order(tags or []),
        source="llm",
    )


def _dedupe_indicator_specs(items: List[StrategyIndicatorSpec]) -> List[StrategyIndicatorSpec]:
    seen = set()
    out: List[StrategyIndicatorSpec] = []
    for item in items or []:
        key = (str(item.name), str(item.kind), int(item.period or 0), str(item.source or "close"))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _series_for_indicator(df: pd.DataFrame, spec: StrategyIndicatorSpec) -> pd.Series:
    source = str(spec.source or "close").strip().lower() or "close"
    if source not in df.columns:
        source = "close"
    base = pd.to_numeric(df[source], errors="coerce").replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0.0)
    period = max(1, int(spec.period or 1))
    kind = str(spec.kind or "price").strip().lower()

    if kind == "price":
        return base
    if kind == "sma":
        return base.rolling(period, min_periods=period).mean()
    if kind == "ema":
        return base.ewm(span=period, adjust=False).mean()
    if kind == "rsi":
        delta = base.diff().fillna(0.0)
        gain = delta.clip(lower=0.0)
        loss = (-delta.clip(upper=0.0))
        avg_gain = gain.rolling(period, min_periods=period).mean()
        avg_loss = loss.rolling(period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi.fillna(50.0)
    if kind == "zscore":
        mean = base.rolling(period, min_periods=period).mean()
        std = base.rolling(period, min_periods=period).std().replace(0, np.nan)
        return ((base - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if kind == "returns":
        return base.pct_change(periods=period).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return base


def _resolve_operand(operand: Any, series_map: Dict[str, pd.Series], index: pd.Index) -> pd.Series:
    if isinstance(operand, (int, float)) and not isinstance(operand, bool):
        return pd.Series(float(operand), index=index, dtype=float)
    token = _slug_token(operand)
    if token in series_map:
        return series_map[token]
    try:
        return pd.Series(float(operand), index=index, dtype=float)
    except Exception:
        return pd.Series(0.0, index=index, dtype=float)


def _evaluate_condition(cond: StrategyCondition, series_map: Dict[str, pd.Series], index: pd.Index) -> pd.Series:
    left = _resolve_operand(cond.left, series_map, index)
    right = _resolve_operand(cond.right, series_map, index)
    op = str(cond.op or "gt").strip().lower()
    if op == "gt":
        return (left > right).fillna(False)
    if op == "gte":
        return (left >= right).fillna(False)
    if op == "lt":
        return (left < right).fillna(False)
    if op == "lte":
        return (left <= right).fillna(False)
    if op == "cross_over":
        return ((left > right) & (left.shift(1) <= right.shift(1))).fillna(False)
    if op == "cross_under":
        return ((left < right) & (left.shift(1) >= right.shift(1))).fillna(False)
    return pd.Series(False, index=index, dtype=bool)


def _combine_conditions(
    conditions: List[StrategyCondition],
    series_map: Dict[str, pd.Series],
    index: pd.Index,
    combine: str,
) -> pd.Series:
    if not conditions:
        return pd.Series(False, index=index, dtype=bool)
    evaluated = [_evaluate_condition(cond, series_map, index) for cond in conditions]
    frame = pd.concat(evaluated, axis=1).fillna(False)
    if str(combine or "all").strip().lower() == "any":
        return frame.any(axis=1)
    return frame.all(axis=1)


def build_program_positions(
    program: StrategyProgram,
    df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
) -> pd.Series:
    merged_params = {**dict(program.params or {}), **dict(params or {})}
    indicators = list(program.indicators or [])
    if not indicators:
        tokens = _conditions_identifiers(list(program.entry_conditions or []) + list(program.exit_conditions or []))
        inferred = [_infer_indicator_from_token(token, merged_params) for token in tokens]
        indicators = [item for item in inferred if item is not None]

    series_map: Dict[str, pd.Series] = {
        "open": pd.to_numeric(df.get("open"), errors="coerce").fillna(0.0),
        "high": pd.to_numeric(df.get("high"), errors="coerce").fillna(0.0),
        "low": pd.to_numeric(df.get("low"), errors="coerce").fillna(0.0),
        "close": pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0),
        "volume": pd.to_numeric(df.get("volume"), errors="coerce").fillna(0.0),
    }
    for spec in indicators:
        series_map[str(spec.name)] = _series_for_indicator(df, spec)

    entry = _combine_conditions(list(program.entry_conditions or []), series_map, df.index, program.entry_combine)
    exit_ = _combine_conditions(list(program.exit_conditions or []), series_map, df.index, program.exit_combine)

    if str(program.execution_mode or "stateful_long") == "signal_long" or not list(program.exit_conditions or []):
        return entry.astype(float).fillna(0.0)

    position = pd.Series(0.0, index=df.index, dtype=float)
    holding = 0.0
    for i, idx in enumerate(df.index):
        if holding > 0.0 and bool(exit_.iloc[i]):
            holding = 0.0
        elif holding <= 0.0 and bool(entry.iloc[i]):
            holding = 1.0
        position.loc[idx] = holding
    return position.fillna(0.0)
