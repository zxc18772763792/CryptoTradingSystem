"""Time-isolated multi-threshold challengers for remaining +200% upside.

The established direct continuation model uses a sparse binary +200% label.
This study pre-registers two richer targets while keeping the feature set,
decision clock, calibration windows, daily limit, event de-duplication, and
confirmation folds unchanged:

* ``ordinal``: a fixed weighted average of +50/+100/+150/+200% probabilities;
* ``ridge_intensity``: ridge regression on clipped log future MFE.

Family/checkpoint/threshold selection is calibration-only.  Folds 3-6 are
historical confirmation and can never be described as true OOS.  The module is
research-only and has no order-routing imports.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
CHECKPOINTS = (4, 8, 12, 16, 20, 24)
QUANTILES = (0.60, 0.70, 0.80, 0.90)
FAMILIES = ("ordinal", "ridge_intensity")
ORDINAL_THRESHOLDS = (0.50, 1.00, 1.50, 2.00)
ORDINAL_WEIGHTS = np.asarray(ORDINAL_THRESHOLDS, dtype=float)
ORDINAL_WEIGHTS /= ORDINAL_WEIGHTS.sum()
CALIBRATION_WINDOWS = (
    ("cal_1", "2026-01-23", "2026-02-07"),
    ("cal_2", "2026-02-07", "2026-02-22"),
    ("cal_3", "2026-02-22", "2026-03-09"),
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTINUATION = _load(
    "binance_continuation_entry_for_extreme_intensity",
    ROOT / "scripts" / "analyze_binance_continuation_entry.py",
)
SEQUENTIAL = CONTINUATION.SEQUENTIAL
VALIDATION = CONTINUATION.VALIDATION
FEATURES = tuple(CONTINUATION.FEATURES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def _score_family(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    family: str,
    features: Sequence[str] = FEATURES,
) -> tuple[np.ndarray, np.ndarray]:
    if family == "ordinal":
        train_parts: list[np.ndarray] = []
        test_parts: list[np.ndarray] = []
        for threshold in ORDINAL_THRESHOLDS:
            label = f"intensity_ge_{int(threshold * 100)}"
            train[label] = pd.to_numeric(train["late_future_max_return_14d"], errors="coerce") >= threshold
            test[label] = pd.to_numeric(test["late_future_max_return_14d"], errors="coerce") >= threshold
            if int(train[label].sum()) < 3 or int((~train[label]).sum()) < 3:
                raise ValueError(f"Insufficient ordinal classes for {label}")
            train_score, test_score = SEQUENTIAL.fit_sequence_score(
                train,
                test,
                family="logistic",
                features=features,
                label_column=label,
            )
            train_parts.append(np.asarray(train_score, dtype=float))
            test_parts.append(np.asarray(test_score, dtype=float))
        return (
            np.average(np.vstack(train_parts), axis=0, weights=ORDINAL_WEIGHTS),
            np.average(np.vstack(test_parts), axis=0, weights=ORDINAL_WEIGHTS),
        )
    if family == "ridge_intensity":
        train_matrix, test_matrix = SEQUENTIAL._prepare_features(train, test, features)
        target = np.log1p(
            np.clip(
                pd.to_numeric(train["late_future_max_return_14d"], errors="coerce").fillna(0.0).to_numpy(),
                0.0,
                9.0,
            )
        )
        model = Ridge(alpha=10.0)
        model.fit(train_matrix, target)
        return model.predict(train_matrix), model.predict(test_matrix)
    raise ValueError(f"Unknown intensity family: {family}")


def _score_split(
    rows: pd.DataFrame,
    *,
    train_before: pd.Timestamp,
    test: pd.DataFrame,
    family: str,
    quantile: float,
) -> pd.DataFrame | None:
    train = rows[
        pd.to_datetime(rows["decision_time"], utc=True) + pd.Timedelta(days=14)
        < pd.Timestamp(train_before)
    ].copy()
    test = test.copy()
    train = train[train["late_target200"].notna() & train["late_future_max_return_14d"].notna()]
    test = test[test["late_target200"].notna() & test["late_future_max_return_14d"].notna()]
    if len(train) < 30 or int(train["late_target200"].sum()) < 3 or test.empty:
        return None
    try:
        train_score, test_score = _score_family(train, test, family=family)
    except ValueError:
        return None
    threshold = float(np.quantile(train_score, quantile))
    test["intensity_family"] = family
    test["intensity_score"] = np.asarray(test_score, dtype=float)
    test["intensity_threshold"] = threshold
    test["selected_raw"] = test["intensity_score"] >= threshold
    test["selected"] = CONTINUATION.apply_daily_limit(test, "selected_raw", "intensity_score")
    return test


def _split_metrics(result: pd.DataFrame, split: str) -> dict[str, Any]:
    labels = result["late_target200"].astype(int)
    selected = result[result["selected"]]
    return {
        "validation_split": split,
        "test_rows": int(len(result)),
        "test_positives": int(labels.sum()),
        "selected_rows": int(len(selected)),
        "selected_positives": int(selected["late_target200"].sum()),
        "base_precision": float(labels.mean()),
        "selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
        "price_average_precision": float(average_precision_score(labels, result["price_model_score"])) if labels.nunique() > 1 else np.nan,
        "intensity_average_precision": float(average_precision_score(labels, result["intensity_score"])) if labels.nunique() > 1 else np.nan,
    }


def _calibration_scores(
    rows: pd.DataFrame,
    *,
    family: str,
    quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    for name, start_raw, end_raw in CALIBRATION_WINDOWS:
        start = pd.Timestamp(start_raw, tz="UTC")
        end = pd.Timestamp(end_raw, tz="UTC")
        test = rows[
            (pd.to_datetime(rows["decision_time"], utc=True) >= start)
            & (pd.to_datetime(rows["decision_time"], utc=True) < end)
        ].copy()
        result = _score_split(
            rows,
            train_before=start,
            test=test,
            family=family,
            quantile=quantile,
        )
        if result is None:
            continue
        result["validation_split"] = name
        metrics.append(_split_metrics(result, name))
        scored.append(result)
    return (
        pd.concat(scored, ignore_index=True) if scored else pd.DataFrame(),
        pd.DataFrame(metrics),
    )


def _confirmation_scores(
    rows: pd.DataFrame,
    folds: pd.DataFrame,
    *,
    family: str,
    quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    for fold in (3, 4, 5, 6):
        start = pd.Timestamp(folds.loc[folds["fold"] == fold, "test_start"].iloc[0])
        test = rows[rows["fold"] == fold].copy()
        result = _score_split(
            rows,
            train_before=start,
            test=test,
            family=family,
            quantile=quantile,
        )
        if result is None:
            continue
        result["validation_split"] = f"fold_{fold}"
        result["fold"] = fold
        metrics.append({"fold": fold, **_split_metrics(result, f"fold_{fold}")})
        scored.append(result)
    return (
        pd.concat(scored, ignore_index=True) if scored else pd.DataFrame(),
        pd.DataFrame(metrics),
    )


def _summary(scored: pd.DataFrame, folds: pd.DataFrame) -> dict[str, Any]:
    if scored.empty:
        return {}
    labels = scored["late_target200"].astype(int)
    selected = scored[scored["selected"]]
    joint = (
        (folds["selected_precision"] > folds["base_precision"])
        & (folds["intensity_average_precision"] > folds["price_average_precision"])
    ) if not folds.empty else pd.Series(dtype=bool)
    precision_delta = folds["selected_precision"] - folds["base_precision"] if not folds.empty else pd.Series(dtype=float)
    return {
        "rows": int(len(scored)),
        "positives": int(labels.sum()),
        "base_precision": float(labels.mean()),
        "price_average_precision": float(average_precision_score(labels, scored["price_model_score"])) if labels.nunique() > 1 else np.nan,
        "intensity_average_precision": float(average_precision_score(labels, scored["intensity_score"])) if labels.nunique() > 1 else np.nan,
        "selected_rows": int(len(selected)),
        "selected_positives": int(selected["late_target200"].sum()),
        "selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
        "positive_retention": float(selected["late_target200"].sum() / labels.sum()) if labels.sum() else 0.0,
        "unique_selected_symbols": int(selected["symbol"].nunique()),
        "fold_joint_improvement_share": float(joint.mean()) if len(joint) else 0.0,
        "fold_precision_delta_mean": float(precision_delta.mean()) if len(precision_delta) else np.nan,
        "fold_precision_delta_std": float(precision_delta.std(ddof=0)) if len(precision_delta) else np.nan,
    }


def _recognition_bootstrap(
    scored: pd.DataFrame,
    *,
    samples: int,
    cluster: str,
    seed: int,
) -> dict[str, Any]:
    rows = scored.copy()
    if cluster == "week":
        rows["cluster"] = pd.to_datetime(rows["decision_time"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        rows["cluster"] = rows["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    groups = [group.index.to_numpy() for _, group in rows.groupby("cluster", sort=False)]
    rng = np.random.default_rng(seed)
    precision_delta: list[float] = []
    ap_delta: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = rows.loc[indices]
        labels = draw["late_target200"].astype(int)
        selected = draw[draw["selected"]]
        if labels.nunique() < 2 or selected.empty:
            continue
        precision_delta.append(float(selected["late_target200"].mean() - labels.mean()))
        ap_delta.append(float(average_precision_score(labels, draw["intensity_score"]) - average_precision_score(labels, draw["price_model_score"])))
    precision = pd.Series(precision_delta, dtype=float)
    ap = pd.Series(ap_delta, dtype=float)
    return {
        "cluster": cluster,
        "samples": int(min(len(precision), len(ap))),
        "precision_delta_median": float(precision.median()),
        "precision_delta_lower_95pct": float(precision.quantile(0.025)),
        "precision_delta_upper_95pct": float(precision.quantile(0.975)),
        "ap_delta_median": float(ap.median()),
        "ap_delta_lower_95pct": float(ap.quantile(0.025)),
        "ap_delta_upper_95pct": float(ap.quantile(0.975)),
    }


def _event_metrics(scored: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    renamed = scored.rename(columns={"intensity_score": "continuation_score"})
    return CONTINUATION.event_metrics(renamed)


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    candidates["decision_time"] = pd.to_datetime(candidates["decision_time"], utc=True)
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)

    records: list[dict[str, Any]] = []
    for family in FAMILIES:
        for hours in CHECKPOINTS:
            rows = candidates[candidates["checkpoint_hours"] == hours].copy()
            for quantile in QUANTILES:
                scored, split_metrics = _calibration_scores(rows, family=family, quantile=quantile)
                summary = _summary(scored, split_metrics)
                eligible = bool(
                    summary and len(split_metrics) == 3 and summary["selected_rows"] >= 20
                    and summary["unique_selected_symbols"] >= 10
                    and summary["intensity_average_precision"] > summary["price_average_precision"]
                    and summary["selected_precision"] > summary["base_precision"]
                    and summary["positive_retention"] >= 0.25
                    and summary["fold_joint_improvement_share"] >= 2 / 3
                )
                score = float(
                    summary.get("fold_precision_delta_mean", -1)
                    - 0.5 * summary.get("fold_precision_delta_std", 1)
                    + 0.5 * (summary.get("intensity_average_precision", 0) - summary.get("price_average_precision", 0))
                    + 0.10 * summary.get("positive_retention", 0)
                    - 0.0005 * hours
                ) if summary else -np.inf
                records.append(
                    {
                        "family": family,
                        "checkpoint_hours": hours,
                        "selection_quantile": quantile,
                        **summary,
                        "selection_score": score,
                        "selection_eligible": eligible,
                    }
                )
    calibration = pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)
    calibration.to_csv(report / "extreme_intensity_calibration.csv", index=False)

    selected = calibration.iloc[0]
    family = str(selected["family"])
    hours = int(selected["checkpoint_hours"])
    quantile = float(selected["selection_quantile"])
    rows = candidates[candidates["checkpoint_hours"] == hours].copy()
    confirmation, confirmation_folds = _confirmation_scores(
        rows,
        folds,
        family=family,
        quantile=quantile,
    )
    confirmation.to_csv(report / "extreme_intensity_confirmation_scored.csv.gz", index=False, compression="gzip")
    confirmation_folds.to_csv(report / "extreme_intensity_confirmation_folds.csv", index=False)
    result = _summary(confirmation, confirmation_folds)
    week = _recognition_bootstrap(confirmation, samples=args.bootstrap_samples, cluster="week", seed=20260820)
    symbol = _recognition_bootstrap(confirmation, samples=args.bootstrap_samples, cluster="symbol", seed=20260821)
    events, event_result = _event_metrics(confirmation)
    events.to_csv(report / "extreme_intensity_event_clusters.csv", index=False)

    gate = bool(
        selected["selection_eligible"] and result
        and result["fold_joint_improvement_share"] > 0.50
        and result["intensity_average_precision"] > result["price_average_precision"]
        and result["selected_precision"] > result["base_precision"]
        and result["positive_retention"] >= 0.25
        and week["precision_delta_lower_95pct"] > 0 and week["ap_delta_lower_95pct"] > 0
        and symbol["precision_delta_lower_95pct"] > 0 and symbol["ap_delta_lower_95pct"] > 0
    )
    prior = json.loads((report / "continuation_entry_decision.json").read_text(encoding="utf-8"))
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_time_isolated_intensity_challenger",
        "prediction_clock": "completed 4h bars strictly before checkpoint; score at checkpoint open",
        "training_families": list(FAMILIES),
        "ordinal_thresholds": list(ORDINAL_THRESHOLDS),
        "ordinal_weights": ORDINAL_WEIGHTS.tolist(),
        "ridge_target": "log1p(clipped positive future 14d MFE), cap +900%",
        "candidate_checkpoints_hours": list(CHECKPOINTS),
        "candidate_quantiles": list(QUANTILES),
        "candidate_count": int(len(FAMILIES) * len(CHECKPOINTS) * len(QUANTILES)),
        "features": list(FEATURES),
        "selected_family": family,
        "selected_checkpoint_hours": hours,
        "selected_quantile": quantile,
        "calibration_eligible": bool(selected["selection_eligible"]),
        "confirmation": result,
        "event_metrics": event_result,
        "week_bootstrap": week,
        "symbol_bootstrap": symbol,
        "prior_binary200_confirmation": prior["direct_continuation"]["confirmation"],
        "ranking_gate_pass": gate,
        "classification": "ranking_candidate_pending_true_oos" if gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (report / "extreme_intensity_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
