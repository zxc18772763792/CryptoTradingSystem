"""New-date validation of chat activity, concentration and volatility forecasts.

Independent local research only. No order, model or Telegram calls; frozen
studies remain unchanged. Inputs and fitted transforms are strictly causal.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# The imported research module establishes this conda interpreter's DLL paths.
from scripts import research_binance_chat_activity as original
import numpy as np
import pandas as pd
from core.research.telegram_event_sentiment import read_archive, digest, utc
from core.research.telegram_sentiment_observer import _quality_filter

OLD = ROOT / "data/research/binance_chat_activity_market_20261031"
SOURCE = ROOT / "data/research/telegram_live_20261024"
COUNTS = ("human_available", "authors_available", "without_top_author", "unique_texts_available", "capped_three")
MARKET_FEATURES = ("log_rv", "log_rv3", "log_quote_volume", "abs_ret_pct", "hour_sin", "hour_cos")
MODELS = {"market": (), "market_activity": ("human_available_surprise",),
          "market_authors": ("authors_available_surprise",),
          "market_without_top": ("without_top_author_surprise",)}


def json_ready(value):
    if isinstance(value, dict):
        return {k: json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, np.generic):
        return json_ready(value.item())
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def save_new(path, value):
    original.frozen(path, json_ready(value))


def concentration_fields(rows, hours):
    """Aggregate only information available at the corresponding hour close."""
    counts, texts = defaultdict(Counter), defaultdict(set)
    for mid, pub, receipt, author, has_text, service, bot, unknown, forwarded, revised, sha in rows:
        p, r = pd.Timestamp(pub), pd.Timestamp(receipt)
        hour = p.floor("h").isoformat()
        if (hour not in hours or not author or not has_text or any((service, bot, unknown, forwarded, revised))
                or not 0 <= (r - p).total_seconds() <= 1800 or r > p.floor("h") + pd.Timedelta(hours=1)):
            continue
        counts[hour][author] += 1
        texts[hour].add(sha)
    result = {}
    for hour in hours:
        c = counts[hour]
        total, top = sum(c.values()), max(c.values(), default=0)
        result[hour] = {"human_available": total, "authors_available": len(c),
                        "without_top_author": total - top, "capped_three": sum(min(v, 3) for v in c.values()),
                        "unique_texts_available": len(texts[hour]), "top_available_count": top,
                        "top_available_share": top / total if total else None}
    return result


def prepare(study):
    path = study / "chat_hourly.json"
    if path.exists():
        return original.read(path)
    plan, status = original.read(study / "protocol.json"), original.read(study / "source_status.json")
    if status["checked_at"] != plan["as_of"]:
        raise ValueError("Source checkpoint changed")
    activation = original.read(SOURCE / "activation.json")
    messages, metadata, coverage = read_archive(SOURCE, activation=activation, status=status)
    messages, quality = _quality_filter(SOURCE, messages)
    views = [{"published_at": m.published.isoformat(), "available_at": m.available.isoformat(), "side": m.side}
             for m in messages if m.side is not None]
    sql = original.read(OLD / "chat_hourly.json")["sql"]
    params = ("BinanceChinese", plan["start"], plan["as_of"], plan["as_of"])
    with closing(sqlite3.connect((SOURCE / "observations.sqlite").resolve().as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        rows = db.execute(sql, params).fetchall()
    if len({r[0] for r in rows}) != len(rows):
        raise ValueError("Duplicate source message identifiers")
    hours, counters = original.aggregate_chat(rows, views, plan["start"], plan["as_of"], coverage)
    extras = concentration_fields(rows, {h["hour"] for h in hours})
    old_hours = {h["hour"]: h for h in original.read(OLD / "chat_hourly.json")["hours"]}
    drift = []
    for h in hours:
        e = extras[h["hour"]]
        if h["human_available"] != e["human_available"] or h["authors_available"] != e["authors_available"]:
            raise ValueError("Count and concentration definitions diverged")
        h.update(e)
        if h["hour"] in old_hours:
            previous = old_hours[h["hour"]]
            changed = {k: [previous[k], h[k]] for k in ("human_available", "authors_available") if previous[k] != h[k]}
            if changed:
                drift.append({"hour": h["hour"], "changed": changed})
            h["latest_certified"] = h["certified"]
            # Preserve the previous study's audit population on its overlap.
            h["certified"], h["coverage_minutes"] = previous["certified"], previous["coverage_minutes"]
        h["overlap_drift"] = any(v["hour"] == h["hour"] for v in drift)
        h["unknown_empty"] = not h["certified"] and h["raw_messages"] == 0
    snapshot = {"extracted_at": datetime.now(timezone.utc).isoformat(), "sql": sql, "params": params,
                "source_rows_sha": digest(rows), "source_counts": counters, "source_metadata": metadata,
                "directional_quality": quality, "coverage_segments": [[a.isoformat(), b.isoformat()] for a, b in coverage],
                "overlap_drift": drift, "hours": hours}
    save_new(path, snapshot)
    for src in sorted(OLD.glob("market_*.json")):
        target = study / src.name
        if target.exists():
            if target.read_bytes() != src.read_bytes():
                raise ValueError("Historical market cache differs")
        else:
            shutil.copy2(src, target)
    return snapshot


def causal_baselines(frame, fields=COUNTS, min_past_hours=24):
    """Same-clock mean/median and fixed 3-observation shrinkage; no future fit."""
    frame = frame.sort_index().copy()
    for field in fields:
        past, by_clock = [], defaultdict(list)
        for suffix in ("global", "mean", "median", "shrink", "surprise"):
            frame[field + "_" + suffix] = np.nan
        frame[field + "_clock_n"] = 0
        for index, row in frame.iterrows():
            value = float(np.log1p(row[field]))
            previous = by_clock[int(row["local_hour"])]
            n = len(previous)
            frame.loc[index, field + "_clock_n"] = n
            if len(past) >= min_past_hours:
                global_mean = float(np.mean(past))
                frame.loc[index, field + "_global"] = global_mean
                shrink = (sum(previous) + 3 * global_mean) / (n + 3)
                frame.loc[index, field + "_shrink"] = shrink
                frame.loc[index, field + "_surprise"] = value - shrink
                if n >= 3:
                    frame.loc[index, field + "_mean"] = float(np.mean(previous))
                    frame.loc[index, field + "_median"] = float(np.median(previous))
            previous.append(value)
            past.append(value)
    return frame


def ridge_predict(train_x, train_y, x, alpha=1.0):
    mean, scale = train_x.mean(axis=0), train_x.std(axis=0)
    scale = np.where(scale > 1e-10, scale, 1.)
    z, at = (train_x - mean) / scale, (x - mean) / scale
    center = float(train_y.mean())
    coefficients = np.linalg.solve(z.T @ z + alpha * np.eye(z.shape[1]), z.T @ (train_y - center))
    return max(0., float(center + at @ coefficients))


def forecast(frame, min_train=48):
    """All fixed models use exactly the same mature rows and forecast origins."""
    needed = list(dict.fromkeys([*MARKET_FEATURES, *(v for extra in MODELS.values() for v in extra)]))
    complete = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=[*needed, "target_log_rv"])
    records = []
    for hour, row in complete.iterrows():
        when = pd.Timestamp(hour) + pd.Timedelta(hours=1)
        # A row's next-hour volatility matures two hours after its start.
        train = complete[(pd.to_datetime(complete.index, utc=True) + pd.Timedelta(hours=2) <= when)
                         & (pd.to_datetime(complete.index, utc=True) < pd.Timestamp(hour))]
        if len(train) < min_train:
            continue
        common = {"hour": hour, "local_date": row["date"], "prediction_at": when.isoformat(),
                  "target_end": (pd.Timestamp(hour) + pd.Timedelta(hours=2)).isoformat(),
                  "train_rows": len(train), "last_train_target_end": (pd.Timestamp(train.index[-1]) + pd.Timedelta(hours=2)).isoformat(),
                  "actual": float(row["target_log_rv"])}
        records.append({**common, "model": "persistence", "prediction": float(row["log_rv"])})
        for model, extra in MODELS.items():
            fields = [*MARKET_FEATURES, *extra]
            prediction = ridge_predict(train[fields].to_numpy(float), train["target_log_rv"].to_numpy(float), row[fields].to_numpy(float))
            records.append({**common, "model": model, "prediction": prediction})
    return records


def forecast_metrics(records):
    if not records:
        return []
    data = pd.DataFrame(records)
    data["abs_error"] = abs(data.prediction - data.actual)
    data["squared_error"] = (data.prediction - data.actual) ** 2
    result = []
    for period, group in (("new_date", data[data.local_date >= "2026-11-01"]),
                           ("prior_dates", data[data.local_date < "2026-11-01"]), ("all_dates", data)):
        for model in ("persistence", *MODELS):
            cell = group[group.model == model]
            baseline = group[group.model == "market"]
            n = len(cell)
            base_mae = baseline.abs_error.mean() if len(baseline) else np.nan
            mae = cell.abs_error.mean() if n else np.nan
            result.append({"period": period, "model": model, "n": n, "dates": int(cell.local_date.nunique()),
                           "mae_log": mae, "rmse_log": np.sqrt(cell.squared_error.mean()) if n else None,
                           "mae_change_vs_market_pct": (mae / base_mae - 1) * 100 if n and base_mae > 0 else None})
    return result


def count_metrics(frame):
    records = []
    for period, group in (("new_date", frame[frame.date >= "2026-11-01"]),
                           ("prior_dates", frame[frame.date < "2026-11-01"]), ("all_dates", frame)):
        fields = ["human_available_" + name for name in ("global", "mean", "median", "shrink")]
        common = group.dropna(subset=fields)
        actual = np.log1p(common.human_available)
        base = float(np.mean(abs(actual - common["human_available_global"]))) if len(common) else np.nan
        for method in ("global", "mean", "median", "shrink"):
            error = float(np.mean(abs(actual - common["human_available_" + method]))) if len(common) else None
            records.append({"period": period, "baseline": method, "n": len(common), "mae_log": error,
                            "change_vs_global_pct": (error / base - 1) * 100 if len(common) and base > 0 else None})
    return records


def analyze(study):
    plan, snapshot = original.read(study / "protocol.json"), prepare(study)
    if plan["forecast"]["ridge_alpha"] != 1.0 or plan["forecast"]["min_mature_training_rows"] != 48:
        raise ValueError("Fixed forecast parameters changed")
    market, quality = original.market_hours(study)
    base = pd.DataFrame(snapshot["hours"]).set_index("hour").join(market)
    for horizon in (1, 3):
        values = pd.DataFrame([original.future_metrics(t, market, horizon) for t in base.index], index=base.index)
        base[f"next_{horizon}h_ret"], base[f"next_{horizon}h_rv"] = values.future_ret, values.future_rv
    base["log_rv"] = np.log1p(base.rv_pct)
    base["log_rv3"] = [np.log1p(np.sqrt(np.mean([market.loc[(pd.Timestamp(t) - pd.Timedelta(hours=i)).isoformat(), "rv_pct"] ** 2 for i in range(3)]))) for t in base.index]
    base["hour_sin"], base["hour_cos"] = np.sin(2 * np.pi * base.local_hour / 24), np.cos(2 * np.pi * base.local_hour / 24)
    base["target_log_rv"] = np.log1p(base.next_1h_rv)
    correlations, metrics, count_errors, predictions, concentration, coverage = [], [], [], [], [], []
    for case in ("certified", "all_retained"):
        frame = base[~base.overlap_drift & ~base.unknown_empty].copy()
        if case == "certified":
            frame = frame[frame.certified]
        frame = causal_baselines(frame.dropna(subset=["ret_pct"]))
        for period, group in (("new_date", frame[frame.date >= "2026-11-01"]),
                              ("prior_dates", frame[frame.date < "2026-11-01"]), ("all_dates", frame)):
            coverage.append({"case": case, "period": period, "hours": len(group), "dates": group.date.nunique(),
                             "human_available": group.human_available.sum(), "btc_available": group.btc_available.sum()})
            concentration.append({"case": case, "period": period, "median_top_author_share": group.top_available_share.median(),
                                  "largest_author_removed_fraction": group.top_available_count.sum() / group.human_available.sum() if group.human_available.sum() else None,
                                  "duplicate_text_fraction": 1 - group.unique_texts_available.sum() / group.human_available.sum() if group.human_available.sum() else None})
            for horizon in (1, 3):
                for field in COUNTS:
                    for kind, feature in (("count", field), ("shrunk_surprise", field + "_surprise")):
                        correlations.append({"case": case, "period": period, "feature": field, "kind": kind,
                                             "horizon": horizon, **original.association(group, feature, f"next_{horizon}h_rv",
                                                 numeric_controls=("ret_pct", "rv_pct", "log_quote_volume"))})
        records = forecast(frame)
        predictions.extend({"case": case, **v} for v in records)
        metrics.extend({"case": case, **v} for v in forecast_metrics(records))
        count_errors.extend({"case": case, **v} for v in count_metrics(frame))
        frame.to_csv(study / f"panel_{case}.csv", encoding="utf-8-sig")
    result = json_ready({"protocol_sha": digest(plan), "as_of": plan["as_of"], "source_counts": snapshot["source_counts"],
                         "quality_checks": snapshot["directional_quality"], "market_quality": quality,
                         "overlap_drift": snapshot["overlap_drift"], "coverage": coverage, "concentration": concentration,
                         "correlations": correlations, "count_baselines": count_errors, "forecast_metrics": metrics,
                         "forecast_records": predictions, "implementation_sha": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                         "paper_only": True, "promotion_allowed": False})
    save_new(study / "results.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", type=Path, default=ROOT / "data/research/binance_chat_activity_followup_20261101")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    study = args.study.resolve()
    if not study.is_relative_to(ROOT / "data/research") or (study / "activation.json").exists():
        raise ValueError("Use a separate inactive research directory")
    if args.prepare_only:
        result = prepare(study)
        print(json.dumps(json_ready({k: result[k] for k in ("source_counts", "directional_quality", "overlap_drift")}), ensure_ascii=True, indent=2))
    else:
        result = analyze(study)
        print(json.dumps({k: result[k] for k in ("as_of", "coverage", "concentration", "forecast_metrics")}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
