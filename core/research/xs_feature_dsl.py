"""Cross-sectional feature formulas the research LLM may propose (research loop v2).

The LLM writes a feature as a small JSON expression tree over one coin's DAILY
series; the evaluator ranks the value across coins each week. Nothing here is
executed as code: every node is checked against a closed vocabulary with bounded
windows and size, then compiled to pandas ops.

Node forms:
  {"col": "close"}                                   base column
  {"const": 0.5}                                     constant
  {"op": "div", "args": [node, node]}                binary op
  {"op": "mean", "args": [node], "window": 30}       rolling / windowed op
  {"op": "log",  "args": [node]}                     unary op

A formula is {"name", "direction": "high"|"low", "expr": node, "thesis"}.
"direction" says which end of the cross-sectional ranking is expected to pump.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, List, Mapping

import numpy as np
import pandas as pd

BASE_COLUMNS: Dict[str, str] = {
    "close": "daily close price",
    "volume": "daily quote volume in USD",
    "oi": "futures open interest in USD (known with a 1-day delay)",
    "mcap": "market cap in USD (known with a 1-day delay)",
    "funding": "daily mean perpetual funding rate",
}
BINARY_OPS = {"add", "sub", "mul", "div", "max2", "min2"}
UNARY_OPS = {"log", "abs", "neg", "sign"}
WINDOW_OPS = {"mean", "std", "max", "min", "sum", "median", "pct_change", "lag", "zscore", "ts_rank", "ema"}
SHIFT_OPS = {"pct_change", "lag"}  # a 1-day shift is meaningful; rolling stats need >= 2 points
MIN_WINDOW, MAX_WINDOW = 2, 180
MAX_DEPTH, MAX_NODES = 5, 20


class FormulaError(ValueError):
    pass


def validate_formula(payload: Any) -> Dict[str, Any]:
    """Return a normalized formula dict or raise FormulaError."""
    if not isinstance(payload, Mapping):
        raise FormulaError("formula must be an object")
    name = str(payload.get("name") or "").strip()[:80]
    if not name:
        raise FormulaError("formula needs a name")
    direction = str(payload.get("direction") or "high").strip().lower()
    if direction not in {"high", "low"}:
        raise FormulaError("direction must be 'high' or 'low'")
    counter = {"nodes": 0, "cols": 0}
    expr = _validate_node(payload.get("expr"), depth=1, counter=counter)
    if counter["cols"] == 0:
        raise FormulaError("formula must reference at least one base column")
    return {
        "name": name,
        "direction": direction,
        "expr": expr,
        "thesis": str(payload.get("thesis") or "").strip()[:400],
    }


def _validate_node(node: Any, *, depth: int, counter: Dict[str, int]) -> Dict[str, Any]:
    if depth > MAX_DEPTH:
        raise FormulaError(f"expression deeper than {MAX_DEPTH}")
    counter["nodes"] += 1
    if counter["nodes"] > MAX_NODES:
        raise FormulaError(f"expression has more than {MAX_NODES} nodes")
    if not isinstance(node, Mapping):
        raise FormulaError("every node must be an object")
    if "col" in node:
        col = str(node["col"]).strip().lower()
        if col not in BASE_COLUMNS:
            raise FormulaError(f"unknown column {col!r}")
        counter["cols"] += 1
        return {"col": col}
    if "const" in node:
        try:
            value = float(node["const"])
        except (TypeError, ValueError) as exc:
            raise FormulaError("const must be a number") from exc
        if not math.isfinite(value):
            raise FormulaError("const must be finite")
        return {"const": value}
    op = str(node.get("op") or "").strip().lower()
    args = node.get("args")
    if not isinstance(args, list):
        raise FormulaError(f"op {op!r} needs an args list")
    if op in BINARY_OPS:
        if len(args) != 2:
            raise FormulaError(f"{op} takes 2 args")
        return {"op": op, "args": [_validate_node(a, depth=depth + 1, counter=counter) for a in args]}
    if op in UNARY_OPS or op in WINDOW_OPS:
        if len(args) != 1:
            raise FormulaError(f"{op} takes 1 arg")
        out = {"op": op, "args": [_validate_node(args[0], depth=depth + 1, counter=counter)]}
        if op in WINDOW_OPS:
            try:
                window = int(node.get("window"))
            except (TypeError, ValueError) as exc:
                raise FormulaError(f"{op} needs an integer window") from exc
            low = 1 if op in SHIFT_OPS else MIN_WINDOW
            if not low <= window <= MAX_WINDOW:
                raise FormulaError(f"{op} window must be in [{low}, {MAX_WINDOW}]")
            out["window"] = window
        return out
    raise FormulaError(f"unknown op {op!r}")


def fingerprint(formula: Mapping[str, Any]) -> str:
    """Identity of what is actually computed (name/thesis do not count)."""
    body = json.dumps({"d": formula["direction"], "e": formula["expr"]}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]


def max_window(expr: Mapping[str, Any]) -> int:
    own = int(expr.get("window") or 0)
    return max([own] + [max_window(a) for a in expr.get("args") or []])


def evaluate_formula(formula: Mapping[str, Any], daily: pd.DataFrame) -> pd.Series:
    """Compute the feature on ONE coin's daily frame (index = date)."""
    values = _eval(formula["expr"], daily)
    return values.replace([np.inf, -np.inf], np.nan)


def _eval(node: Mapping[str, Any], daily: pd.DataFrame) -> pd.Series:
    if "col" in node:
        col = node["col"]
        if col not in daily.columns:
            return pd.Series(np.nan, index=daily.index, dtype=float)
        return pd.to_numeric(daily[col], errors="coerce").astype(float)
    if "const" in node:
        return pd.Series(float(node["const"]), index=daily.index, dtype=float)
    op = node["op"]
    args: List[pd.Series] = [_eval(a, daily) for a in node["args"]]
    if op == "add":
        return args[0] + args[1]
    if op == "sub":
        return args[0] - args[1]
    if op == "mul":
        return args[0] * args[1]
    if op == "div":
        return args[0] / args[1].where(args[1].abs() > 1e-12)
    if op == "max2":
        return np.maximum(args[0], args[1])
    if op == "min2":
        return np.minimum(args[0], args[1])
    x = args[0]
    if op == "log":
        return np.log(x.where(x > 0))
    if op == "abs":
        return x.abs()
    if op == "neg":
        return -x
    if op == "sign":
        return np.sign(x)
    w = int(node["window"])
    minp = max(2, w // 2)
    if op == "mean":
        return x.rolling(w, min_periods=minp).mean()
    if op == "std":
        return x.rolling(w, min_periods=minp).std()
    if op == "max":
        return x.rolling(w, min_periods=minp).max()
    if op == "min":
        return x.rolling(w, min_periods=minp).min()
    if op == "sum":
        return x.rolling(w, min_periods=minp).sum()
    if op == "median":
        return x.rolling(w, min_periods=minp).median()
    if op == "pct_change":
        return x / x.shift(w).where(x.shift(w).abs() > 1e-12) - 1.0
    if op == "lag":
        return x.shift(w)
    if op == "zscore":
        mean = x.rolling(w, min_periods=minp).mean()
        std = x.rolling(w, min_periods=minp).std()
        return (x - mean) / std.where(std > 1e-12)
    if op == "ts_rank":
        return x.rolling(w, min_periods=minp).apply(lambda a: (a[-1] >= a).mean(), raw=True)
    if op == "ema":
        return x.ewm(span=w, adjust=False, min_periods=minp).mean()
    raise FormulaError(f"unknown op {op!r}")  # unreachable after validation


def describe_grammar() -> str:
    """Grammar text for the LLM prompt (kept next to the validator so they agree)."""
    cols = "; ".join(f"{k} = {v}" for k, v in BASE_COLUMNS.items())
    return (
        f"Columns (one coin, one row per day): {cols}. "
        f"Binary ops (args=[a,b]): {', '.join(sorted(BINARY_OPS))}. "
        f"Unary ops (args=[a]): {', '.join(sorted(UNARY_OPS))}. "
        f"Window ops (args=[a], window={MIN_WINDOW}..{MAX_WINDOW} days; pct_change/lag also allow 1): {', '.join(sorted(WINDOW_OPS))}. "
        "Leaves: {\"col\": name} or {\"const\": number}. "
        f"Max depth {MAX_DEPTH}, max {MAX_NODES} nodes. "
        "div by ~0 and log of <=0 yield missing values. ts_rank = percentile of the latest value within the window."
    )
