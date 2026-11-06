"""Metadata-only study of chat activity, BTC markets and local time.

Independent snapshots; no Telegram/model requests, ledger writes or orders.
All coefficients are descriptive, with day deletion rather than IID p-values.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

# Desktop shells need not be conda-activated. Retain the handle so LAPACK's
# delayed DLL loads use this interpreter's native libraries on Windows.
_native_dll_handle = None
if os.name == "nt" and (Path(sys.prefix) / "Library/bin").is_dir():
    _native_bin = str(Path(sys.prefix) / "Library/bin")
    # Some conda LAPACK dispatchers resolve their own backend through PATH.
    # This changes the research process only, not the user's saved environment.
    os.environ["PATH"] = _native_bin + os.pathsep + os.environ.get("PATH", "")
    _native_dll_handle = os.add_dll_directory(_native_bin)

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.telegram_event_sentiment import digest, utc

SOURCE = ROOT / "data/research/telegram_live_20261024"
PRIOR = ROOT / "data/research/binance_chat_backtest_20261031/inputs.json"
XS = ("human_available", "authors_available", "btc_available")
YS = ("ret_pct", "abs_ret_pct", "rv_pct", "log_quote_volume")
LAGS = (-3, -2, -1, 0, 1, 2, 3)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def frozen(path, value):
    if path.exists():
        if read(path) != value:
            raise ValueError(f"Frozen output changed: {path.name}")
    else:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def full_hours(start, end):
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    if not start.tzinfo or not end.tzinfo or end < start:
        raise ValueError("Require ordered timezone-aware boundaries")
    return pd.date_range(start.ceil("h"), end.floor("h"), freq="h", inclusive="left")


def covered_minutes(start, intervals):
    start = pd.Timestamp(start)
    end, clipped = start + pd.Timedelta(hours=1), []
    for a, b in intervals:
        left, right = max(start, pd.Timestamp(a)), min(end, pd.Timestamp(b))
        if left < right:
            clipped.append((left, right))
    total, rightmost = 0., start
    for a, b in sorted(clipped):
        if b > rightmost:
            total += (b - max(a, rightmost)).total_seconds() / 60
            rightmost = b
    return total


def aggregate_chat(rows, views, start, end, coverage):
    grid = full_hours(start, end)
    cells = {t: Counter() for t in grid}
    authors, timely_authors, hashes, author_counts = defaultdict(set), defaultdict(set), defaultdict(set), defaultdict(Counter)
    counters = Counter()
    for row in rows:
        mid, pub, receipt, author, has_text, service, bot, unknown, forwarded, revised, sha = row
        counters["source_rows"] += 1
        try:
            p, r = pd.Timestamp(pub), pd.Timestamp(receipt)
            if not p.tzinfo or not r.tzinfo:
                raise ValueError("naive message timestamp")
        except (ValueError, TypeError):
            counters["invalid_timestamp"] += 1
            continue
        hour = p.floor("h")
        if hour not in cells:
            counters["partial_hour_messages"] += 1
            continue
        cell = cells[hour]
        cell["raw_messages"] += 1
        human = bool(author and has_text and not any((service, bot, unknown, forwarded, revised)))
        if not human:
            counters["non_original_human_text"] += 1
            continue
        cell["human_messages"] += 1
        authors[hour].add(author)
        hashes[hour].add(sha)
        author_counts[hour][author] += 1
        lag = (r - p).total_seconds()
        if 0 <= lag <= 1800:
            cell["human_timely"] += 1
            if r <= hour + pd.Timedelta(hours=1):
                cell["human_available"] += 1
                timely_authors[hour].add(author)
        else:
            cell["late_or_invalid_receipt"] += 1
    for view in views:
        p, a = pd.Timestamp(view["published_at"]), pd.Timestamp(view["available_at"])
        hour = p.floor("h")
        if hour in cells:
            cells[hour]["btc_views"] += 1
            if a <= hour + pd.Timedelta(hours=1):
                cells[hour]["btc_available"] += 1
                cells[hour]["btc_bullish_available"] += view["side"] == 1
                cells[hour]["btc_bearish_available"] += view["side"] == -1
    result = []
    counts = ("raw_messages", "human_messages", "human_timely", "human_available", "late_or_invalid_receipt",
              "btc_views", "btc_available", "btc_bullish_available", "btc_bearish_available")
    for hour, cell in cells.items():
        minutes = covered_minutes(hour, coverage)
        local = hour.tz_convert("Asia/Shanghai")
        result.append({"hour": hour.isoformat(), "local_time": local.isoformat(), "local_hour": local.hour,
                       "date": str(local.date()), "weekday": local.weekday(),
                       "coverage_minutes": minutes, "certified": minutes >= 60 - 1e-7,
                       **{name: int(cell[name]) for name in counts},
                       "authors": len(authors[hour]), "authors_available": len(timely_authors[hour]),
                       "unique_texts": len(hashes[hour]),
                       "top_author_share": max(author_counts[hour].values()) / cell["human_messages"] if cell["human_messages"] else None})
    return result, dict(counters)


def freeze_chat(study):
    path = study / "chat_hourly.json"
    if path.exists():
        return read(path)
    plan, prior = read(study / "protocol.json"), read(PRIOR)
    if hashlib.sha256(PRIOR.read_bytes()).hexdigest() != plan["directional_input_sha256"]:
        raise ValueError("Directional input snapshot changed")
    sql = """SELECT message_id,published_at,first_received_at,
        json_extract(row_json,'$.author_hash'),length(json_extract(row_json,'$.content'))>0,
        json_extract(row_json,'$.service'),json_extract(row_json,'$.sender_is_bot'),
        json_extract(row_json,'$.sender_unknown'),json_extract(row_json,'$.forwarded'),
        json_extract(row_json,'$.revision_observed'),text_sha
        FROM messages WHERE channel=? AND mode='forward' AND published_at>=? AND published_at<=?
        AND first_received_at<=? ORDER BY published_at,message_id"""
    params = ("BinanceChinese", plan["start"], plan["as_of"], plan["as_of"])
    with closing(sqlite3.connect((SOURCE / "observations.sqlite").as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        rows = db.execute(sql, params).fetchall()
    if len({r[0] for r in rows}) != len(rows):
        raise ValueError("Duplicate message identifiers")
    hourly, counters = aggregate_chat(rows, prior["views"], plan["start"], plan["as_of"], prior["coverage"])
    snapshot = {"extracted_at": datetime.now(timezone.utc).isoformat(), "sql": sql, "params": params,
                "source_rows_sha": digest(rows), "source_counts": counters, "coverage_segments": prior["coverage"],
                "directional_quality": prior["quality_checks"], "hours": hourly}
    frozen(path, snapshot)
    return snapshot


def market_hours(study):
    plan = read(study / "protocol.json")
    start = pd.Timestamp(plan["start"]).ceil("h") - pd.Timedelta(hours=3)
    end = pd.Timestamp(plan["as_of"]).floor("h")
    bars, hashes = {}, {}
    for path in sorted(study.glob("market_*.json")):
        page = read(path)
        if (page["tool"] != "get_futures_usds_kline_candlestick_data"
                or page["request"]["symbol"] != "BTCUSDT" or page["request"]["interval"] != "5m"):
            raise ValueError("Wrong market venue, symbol or interval")
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        for row in page["response"]["result"]:
            stamp = pd.to_datetime(int(row[0]), unit="ms", utc=True)
            values = [float(row[i]) for i in (1, 2, 3, 4, 5, 7)]
            if (stamp in bars or int(row[0]) % 300000 or not np.isfinite(values).all()
                    or min(values[:4]) <= 0 or min(values[4:]) < 0):
                raise ValueError("Duplicate, unaligned or invalid market bar")
            bars[stamp] = values
    expected = pd.date_range(start, end, freq="5min")
    missing = [t.isoformat() for t in expected if t not in bars]
    result = []
    for hour in pd.date_range(start, end, freq="h", inclusive="left"):
        stamps = pd.date_range(hour, hour + pd.Timedelta(hours=1), freq="5min")
        if any(t not in bars for t in stamps):
            continue
        opens = np.array([bars[t][0] for t in stamps])
        returns = np.diff(np.log(opens))
        ret = (opens[-1] / opens[0] - 1) * 100
        volume = sum(bars[t][5] for t in stamps[:-1])
        result.append({"hour": hour.isoformat(), "open": opens[0], "next_open": opens[-1],
                       "ret_pct": ret, "abs_ret_pct": abs(ret), "rv_pct": np.sqrt(np.sum(returns**2)) * 100,
                       "quote_volume": volume, "log_quote_volume": np.log1p(volume)})
    columns = ["hour", "open", "next_open", "ret_pct", "abs_ret_pct", "rv_pct", "quote_volume", "log_quote_volume"]
    return pd.DataFrame(result, columns=columns).set_index("hour"), {
        "expected_5m_bars": len(expected), "observed_5m_bars": len(bars),
        "missing_5m_bars": missing, "receipt_hashes": hashes}


def rank_correlation(frame, x, y, *, fixed_effects=False, numeric_controls=()):
    columns = list(dict.fromkeys([x, y, "local_hour", "date", *numeric_controls]))
    data = frame[columns].replace([np.inf, -np.inf], np.nan).dropna()
    n, days = len(data), data["date"].nunique()
    result = {"n": n, "days": int(days), "rho": None, "residual_dof": None}
    if n < 8 or data[x].nunique() < 2 or data[y].nunique() < 2:
        return result
    controls = [np.ones((n, 1))]
    if fixed_effects:
        dummies = pd.get_dummies(data[["local_hour", "date"]].astype(str), drop_first=True, dtype=float)
        controls.append(dummies.to_numpy())
    for name in numeric_controls:
        controls.append(data[name].rank().to_numpy()[:, None])
    matrix = np.hstack(controls)
    dof = n - np.linalg.matrix_rank(matrix) - 1
    result["residual_dof"] = int(dof)
    if dof < 5:
        return result
    a, b = data[x].rank().to_numpy(), data[y].rank().to_numpy()
    a -= matrix @ np.linalg.lstsq(matrix, a, rcond=None)[0]
    b -= matrix @ np.linalg.lstsq(matrix, b, rcond=None)[0]
    if np.std(a) > 1e-9 and np.std(b) > 1e-9:
        result["rho"] = float(np.corrcoef(a, b)[0, 1])
    return result


def association(frame, x, y, *, fixed_effects=False, numeric_controls=()):
    result = rank_correlation(frame, x, y, fixed_effects=fixed_effects, numeric_controls=numeric_controls)
    valid = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=list(dict.fromkeys([x, y, *numeric_controls])))
    deletions = [rank_correlation(valid[valid["date"] != day], x, y, fixed_effects=fixed_effects,
                                  numeric_controls=numeric_controls)["rho"] for day in sorted(valid["date"].unique())]
    values = [v for v in deletions if v is not None]
    result.update(day_deletion_min=min(values) if values else None,
                  day_deletion_max=max(values) if values else None,
                  day_deletion_evaluations=len(values))
    return result


def add_walk_forward_baseline(frame, minimum_dates=3):
    result, history = frame.copy(), defaultdict(list)
    result["past_same_hour_days"] = 0
    result["expected_log_count"] = np.nan
    result["activity_surprise"] = np.nan
    for index, row in result.sort_index().iterrows():
        previous = history[row["local_hour"]]
        result.loc[index, "past_same_hour_days"] = len(previous)
        if len(previous) >= minimum_dates:
            baseline = float(np.mean(previous))
            result.loc[index, "expected_log_count"] = baseline
            result.loc[index, "activity_surprise"] = np.log1p(row["human_available"]) - baseline
        previous.append(np.log1p(row["human_available"]))
    return result


def future_metrics(hour, market, horizon):
    at = pd.Timestamp(hour)
    stamps = [(at + pd.Timedelta(hours=i)).isoformat() for i in range(1, horizon + 1)]
    if any(t not in market.index for t in stamps):
        return {"future_ret": None, "future_rv": None}
    cells = market.loc[stamps]
    return {"future_ret": (cells.iloc[-1]["next_open"] / cells.iloc[0]["open"] - 1) * 100,
            "future_rv": float(np.sqrt(np.sum(cells["rv_pct"] ** 2)))}


def analyze(study):
    snapshot = freeze_chat(study)
    market, market_quality = market_hours(study)
    base = pd.DataFrame(snapshot["hours"]).set_index("hour")
    panel = base.join(market)
    for horizon in (1, 3):
        values = pd.DataFrame([future_metrics(hour, market, horizon) for hour in panel.index], index=panel.index)
        panel[f"next_{horizon}h_ret"] = values["future_ret"]
        panel[f"next_{horizon}h_rv"] = values["future_rv"]
    profiles, lagged, forecasts, clocks, extremes = [], [], [], [], []
    frames = {}
    for case in ("certified", "all_retained"):
        frame = panel[panel["certified"]].copy() if case == "certified" else panel.copy()
        frame = frame.dropna(subset=["ret_pct"])
        frame = add_walk_forward_baseline(frame)
        frames[case] = frame
        for hour, group in frame.groupby("local_hour"):
            profiles.append({"case": case, "local_hour": int(hour), "observed_hours": len(group),
                             "dates": group["date"].nunique(), "mean_human_messages": group["human_messages"].mean(),
                             "median_human_messages": group["human_messages"].median(),
                             "mean_authors": group["authors"].mean(), "mean_rv_pct": group["rv_pct"].mean(),
                             "mean_quote_volume": group["quote_volume"].mean(),
                             "btc_views_total": int(group["btc_views"].sum())})
        for x in XS:
            for y in YS:
                for lag in LAGS:
                    column = "market_shifted"
                    shifted = [(pd.Timestamp(t) + pd.Timedelta(hours=lag)).isoformat() for t in frame.index]
                    working = frame.copy()
                    working[column] = [market.loc[t, y] if t in market.index else np.nan for t in shifted]
                    for method in ("raw", "clock_date_adjusted"):
                        lagged.append({"case": case, "feature": x, "market": y, "lag": lag, "method": method,
                                       **association(working, x, column, fixed_effects=method != "raw")})
        for horizon in (1, 3):
            for x in ("human_available", "authors_available", "btc_available", "activity_surprise"):
                for y in ("ret", "rv"):
                    target = f"next_{horizon}h_{y}"
                    forecasts.append({"case": case, "feature": x, "horizon": horizon, "market": y,
                                      "method": "past_clock_baseline" if x == "activity_surprise" else "clock_date_current_market_adjusted",
                                      **association(frame, x, target, fixed_effects=x != "activity_surprise",
                                                    numeric_controls=() if x == "activity_surprise" else ("ret_pct", "rv_pct", "log_quote_volume"))})
        clocks.append({"case": case, "hours": len(frame), "days": frame["date"].nunique(),
                       "human_messages": int(frame["human_messages"].sum()), "btc_views": int(frame["btc_views"].sum()),
                       "walk_forward_hours": int(frame["activity_surprise"].notna().sum()),
                       "median_top_author_share": frame["top_author_share"].median(),
                       "human_available_fraction": frame["human_available"].sum() / frame["human_messages"].sum() if frame["human_messages"].sum() else None})
        for index, row in frame.nlargest(5, "human_available").iterrows():
            extremes.append({"case": case, "hour": index, "local_time": row["local_time"],
                             **{k: row[k] for k in ("human_available", "authors_available", "btc_available", "ret_pct", "rv_pct", "next_1h_ret", "next_3h_ret")}})
    for case, frame in frames.items():
        frame.to_csv(study / f"panel_{case}.csv", encoding="utf-8-sig")
    result = {"protocol_sha": digest(read(study / "protocol.json")), "as_of": read(study / "protocol.json")["as_of"],
              "source_counts": snapshot["source_counts"], "market_quality": market_quality,
              "coverage": clocks, "profiles": profiles, "lead_lag": lagged, "future": forecasts,
              "largest_activity_hours": extremes, "paper_only": True,
              "input_hashes": {n: hashlib.sha256((study / n).read_bytes()).hexdigest() for n in ("protocol.json", "chat_hourly.json")},
              "implementation_sha": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    # CSV is a reproducible export; JSON keeps nulls instead of NaN for missing evidence.
    result = json.loads(pd.Series([result]).to_json(orient="values"))[0]
    frozen(study / "results.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", type=Path, default=ROOT / "data/research/binance_chat_activity_market_20261031")
    args = parser.parse_args()
    study = args.study.resolve()
    if not study.is_relative_to(ROOT / "data/research") or (study / "activation.json").exists():
        raise ValueError("Use an independent local research directory")
    result = analyze(study)
    print(json.dumps({k: result[k] for k in ("as_of", "source_counts", "market_quality", "coverage")}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
