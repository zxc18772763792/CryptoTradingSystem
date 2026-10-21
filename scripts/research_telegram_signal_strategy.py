"""Reproducible offline study of frozen Telegram signals and local perpetual bars.

Consumes one-off AI annotations; never calls an AI service or an order endpoint.
All selection decisions precede the price join. Public preview revisions are not
a true point-in-time archive, so results remain historical exploratory evidence.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/research/telegram_signal_study_20261020"
HOUR = pd.Timedelta(hours=1)


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def entry_time(publication: Any) -> pd.Timestamp:
    return (utc(publication) + pd.Timedelta(seconds=60)).ceil("h")


def cryptoquant_signal(message: dict) -> dict | None:
    text = message["content"]
    # Exchange hashtags are not assets. Restrict this hypothesis to explicit BTC/ETH.
    asset = re.search(r"#(BTC|ETH)\b", text, re.I)
    if not asset or message["payload"].get("telegram_edited"):
        return None
    # An uncertainty disclaimer ("could be an internal transfer") accompanies
    # ordinary outflow alerts. It is not a later correction of that event.
    if re.search(r"^\s*(?:correction\b|false alert\b|it was an? internal transfer\b|this (?:alert|transaction|transfer) (?:was|is) (?:an? )?internal transfer\b)", text, re.I):
        return None
    if re.search(r"flows from miners to exchanges", text, re.I):
        direction, kind = -1, "miner_exchange_inflow"
    elif re.search(r"aggregated inflows to|unusual exchange inflows", text, re.I):
        direction, kind = -1, "exchange_inflow"
    elif re.search(r"aggregated outflows from|unusual exchange outflows", text, re.I):
        direction, kind = 1, "exchange_outflow"
    else:
        return None
    exchange = re.search(r"aggregated inflows to\s+([^\n]+)", text, re.I)
    return {
        "source_id": message["source_id"], "source": "cryptoquant_alert",
        "symbol": asset.group(1).upper() + "USDT", "direction": direction,
        "kind": kind, "exchange": exchange.group(1).strip() if exchange else "multiple_or_miners",
        "has_uncertainty_disclaimer": bool(re.search(r"could be|might be|custody|internal transfer", text, re.I)),
        "published_at": message["published_at"], "url": message["url"],
        "evidence": text[:350], "confidence": None,
    }


class LocalMarket:
    def __init__(self, root: Path = ROOT):
        self.root = root
        self.cache: dict[tuple[str, str], pd.DataFrame] = {}
        self.sources: dict[str, dict] = {}

    def frame(self, symbol: str, kind: str = "1h") -> pd.DataFrame:
        key = symbol, kind
        if key in self.cache:
            return self.cache[key]
        if not re.fullmatch(r"[A-Z0-9]{1,25}USDT", symbol):
            return pd.DataFrame()
        directory = self.root / "data/historical/binance_futures" / (symbol[:-4] + "_USDT")
        files = [p for p in [directory / f"{kind}.parquet", *sorted((directory / f"{kind}_parts").glob("*.parquet"))] if p.exists()]
        frames = []
        for path in files:
            blob = path.read_bytes()
            frame = pd.read_parquet(io.BytesIO(blob))
            frame.index = pd.to_datetime(frame.index, utc=True)
            frames.append(frame)
            self.sources[str(path.relative_to(self.root))] = {
                "sha256": hashlib.sha256(blob).hexdigest(), "bytes": len(blob),
                "rows": len(frame), "first": str(frame.index.min()), "last": str(frame.index.max()),
            }
        result = pd.concat(frames).sort_index(kind="stable") if frames else pd.DataFrame()
        result = result[~result.index.duplicated(keep="last")]
        self.cache[key] = result
        return result


def funding_cost(frame: pd.DataFrame, entry: pd.Timestamp, exit_: pd.Timestamp,
                 price: float, direction: int) -> tuple[float | None, list[dict], str | None]:
    if frame.empty or not {"funding_rate", "mark_price"}.issubset(frame.columns):
        return None, [], "funding_unavailable"
    before = frame.index[frame.index <= entry]
    after = frame.index[frame.index >= exit_]
    if len(before) == 0 or len(after) == 0:
        return None, [], "funding_boundary_uncovered"
    covered = frame.loc[before[-1]:after[0]]
    if covered.index.to_series().diff().max() > pd.Timedelta(hours=12, seconds=1):
        return None, [], "funding_gap_over_12h"
    paid = covered[(covered.index > entry) & (covered.index <= exit_)]
    if not np.isfinite(paid[["funding_rate", "mark_price"]].to_numpy(dtype=float)).all() or (paid["mark_price"] <= 0).any():
        return None, [], "invalid_funding_values"
    settlements = [
        {"at": str(at), "return": -direction * float(row.funding_rate) * float(row.mark_price) / price}
        for at, row in paid.iterrows()
    ]
    return float(sum(row["return"] for row in settlements)), settlements, None


def evaluate(signal: dict, market: LocalMarket, hours: int, direction: int | None = None) -> dict:
    side = int(signal["direction"] if direction is None else direction)
    start = entry_time(signal["published_at"])
    end = start + hours * HOUR
    base = {**signal, "direction": side, "holding_hours": hours,
            "entry_at": str(start), "planned_exit_at": str(end)}
    frame = market.frame(signal["symbol"])
    if frame.empty:
        return {**base, "status": "missing_symbol_history"}
    wanted = pd.date_range(start, end, freq="h")
    if not wanted.isin(frame.index).all():
        return {**base, "status": "price_coverage_gap"}
    path = frame.loc[wanted]
    required = ["open", "high", "low", "close", "volume"]
    if (not np.isfinite(path[required].to_numpy(dtype=float)).all()
        or (path[["open", "high", "low", "close"]] <= 0).any().any()
        or (path["volume"] <= 0).any()):
        return {**base, "status": "invalid_price_or_volume"}
    p0 = float(path.iloc[0].open)
    traded = path.iloc[:-1]
    liquidated = False
    # Conservative 1x isolated-collateral approximation using traded-price OHLC.
    # Mark-price paths / exact maintenance tiers are not available for this study.
    hit = traded["high"] >= p0 * 1.99 if side < 0 else traded["low"] <= p0 * .005
    if hit.any():
        end = hit.index[np.flatnonzero(hit.to_numpy())[0]] + HOUR
        liquidated = True
        p1 = p0 * (1.99 if side < 0 else .005)
        gross = -1.0
    else:
        p1 = float(path.iloc[-1].open)
        gross = side * (p1 / p0 - 1)
    fund, settlements, funding_error = funding_cost(market.frame(signal["symbol"], "funding_rate"), start, end, p0, side)
    observed = traded[traded.index < end]
    adverse = float((observed["low"].min() / p0 - 1) if side > 0 else (1 - observed["high"].max() / p0))
    favorable = float((observed["high"].max() / p0 - 1) if side > 0 else (1 - observed["low"].min() / p0))
    return {
        **base, "status": "ok", "exit_at": str(end), "entry_price": p0,
        "exit_price": p1, "gross_return": gross, "round_trip_cost": .002,
        "net_before_funding": gross - .002, "funding_return": fund,
        "net_return": gross - .002 + fund if fund is not None else None,
        "net_stress_40bps": gross - .004 + fund if fund is not None else None,
        "funding_error": funding_error, "settlements": settlements,
        "max_adverse_excursion": adverse, "max_favorable_excursion": favorable,
        "liquidated_1x_approx": liquidated,
    }


def mark_return(row: dict, at: pd.Timestamp, market: LocalMarket, funded: bool) -> float:
    start, end = utc(row["entry_at"]), utc(row["exit_at"])
    if at < start:
        return 0.0
    if at >= end:
        return float(row["net_return"] if funded else row["net_before_funding"])
    frame = market.frame(row["symbol"])
    if at not in frame.index:
        raise ValueError("Missing hourly mark inside an admitted complete trade path")
    gross = row["direction"] * (float(frame.loc[at, "open"]) / row["entry_price"] - 1)
    funding = sum(x["return"] for x in row["settlements"] if utc(x["at"]) <= at) if funded else 0.0
    return gross - row["round_trip_cost"] / 2 + funding


def choose_book(rows: list[dict], market: LocalMarket, funded: bool) -> tuple[list[dict], dict]:
    chosen: list[dict] = []
    reasons: Counter = Counter()
    for row in sorted(rows, key=lambda r: (r["entry_at"], r["source_id"])):
        if row["status"] != "ok":
            reasons[row["status"]] += 1
            continue
        if funded and row["net_return"] is None:
            reasons["funding_uncovered"] += 1
            continue
        at = utc(row["entry_at"])
        # Keep the planned reservation even after an estimated liquidation so
        # matched inverse/long baselines use the same non-overlapping entries.
        active = [r for r in chosen if utc(r["planned_exit_at"]) > at]
        if any(r["symbol"] == row["symbol"] for r in active):
            reasons["overlapping_symbol"] += 1
            continue
        if len(active) >= 5:
            reasons["five_position_limit"] += 1
            continue
        equity = 1 + .2 * sum(mark_return(r, at, market, funded) for r in chosen)
        if equity - .2 * len(active) < .2:
            reasons["insufficient_collateral"] += 1
            continue
        chosen.append(row)
    return chosen, dict(reasons)


def week_bootstrap(rows: list[dict], field: str) -> list[float] | None:
    if len(rows) < 8:
        return None
    grouped: dict[str, list[float]] = {}
    for row in rows:
        # Calendar-week blocks retain within-week concentration/correlation.
        stamp = utc(row["entry_at"])
        week = str((stamp - pd.Timedelta(days=stamp.weekday())).date())
        grouped.setdefault(week, []).append(float(row[field]))
    values = list(grouped.values())
    if len(values) < 8:
        return None
    sums = np.array([sum(v) for v in values])
    counts = np.array([len(v) for v in values])
    draws = np.random.default_rng(705).integers(0, len(values), size=(2000, len(values)))
    means = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return np.quantile(means, [.025, .975]).tolist()


def metrics(rows: list[dict], field: str) -> dict:
    if not rows:
        return {"n": 0}
    values = np.array([float(r[field]) for r in rows])
    positive = float(values[values > 0].sum())
    negative = float(-values[values < 0].sum())
    return {
        "n": len(rows), "mean_trade_return": float(values.mean()),
        "median_trade_return": float(np.median(values)), "win_rate": float((values > 0).mean()),
        "profit_factor": positive / negative if negative else None,
        "mean_without_best_trade": float(np.delete(values, values.argmax()).mean()) if len(values) > 1 else None,
        "calendar_week_bootstrap_95pct_mean": week_bootstrap(rows, field),
        "active_months": len({r["entry_at"][:7] for r in rows}),
        "liquidation_approximations": sum(r["liquidated_1x_approx"] for r in rows),
    }


def equity_curve(rows: list[dict], market: LocalMarket, funded: bool) -> list[dict]:
    if not rows:
        return []
    timeline = pd.date_range(min(utc(r["entry_at"]) for r in rows) - HOUR,
                             max(utc(r["exit_at"]) for r in rows), freq="h")
    # Track hourly unrealized PnL, fees and funding rather than realized-only DD.
    equity = np.ones(len(timeline))
    for row in rows:
        start, end = utc(row["entry_at"]), utc(row["exit_at"])
        first, last = timeline.searchsorted(start), timeline.searchsorted(end)
        active_index = timeline[first:last]
        prices = market.frame(row["symbol"]).loc[active_index, "open"].to_numpy(dtype=float)
        active_pnl = row["direction"] * (prices / row["entry_price"] - 1) - row["round_trip_cost"] / 2
        if funded:
            for settlement in row["settlements"]:
                applies = active_index.searchsorted(utc(settlement["at"]))
                active_pnl[applies:] += settlement["return"]
        equity[first:last] += .2 * active_pnl
        final_pnl = row["net_return"] if funded else row["net_before_funding"]
        equity[last:] += .2 * final_pnl
    return [{"at": str(at), "equity": float(value)} for at, value in zip(timeline, equity)]


def evaluate_source(signals: list[dict], market: LocalMarket) -> tuple[dict, list[dict], list[dict]]:
    ordered = sorted(signals, key=lambda s: (s["published_at"], s["source_id"]))
    cutoff = utc(ordered[min(len(ordered) - 1, math.floor(len(ordered) * .7))]["published_at"]) if ordered else None
    summary: dict = {"signals": len(signals), "historical_holdout_start": str(cutoff), "horizons": {}}
    outcomes, curves = [], []
    for hours in (6, 24, 72, 168):
        rows = []
        for signal in ordered:
            row = evaluate(signal, market, hours)
            published = utc(signal["published_at"])
            row["phase"] = "holdout" if published >= cutoff else "development"
            if row["phase"] == "development" and utc(row["planned_exit_at"]) > cutoff:
                row["phase"] = "purged"
            rows.append(row)
        outcomes.extend(rows)
        horizon = {}
        for phase in ("development", "holdout"):
            phase_rows = [r for r in rows if r["phase"] == phase]
            selected, skipped = choose_book(phase_rows, market, funded=False)
            # Hold the same selected entries across baselines; don't optimize each book.
            funded = [r for r in selected if r["net_return"] is not None]
            entry_index = {s["source_id"]: s for s in ordered}
            inverted = [evaluate(entry_index[r["source_id"]], market, hours, -r["direction"]) for r in funded]
            always_long = [evaluate(entry_index[r["source_id"]], market, hours, 1) for r in funded]
            valid_pairs = [(a,b,c) for a,b,c in zip(funded,inverted,always_long)
                           if b.get("net_return") is not None and c.get("net_return") is not None]
            paired = [a for a,_,_ in valid_pairs]
            phase_result = {
                "eligible_events": len(phase_rows), "skipped": skipped,
                "all_admitted_before_funding": metrics(selected, "net_before_funding"),
                "funding_covered_matched": metrics(paired, "net_return"),
                "inverted_same_entries": metrics([b for _,b,_ in valid_pairs], "net_return"),
                "always_long_same_entries": metrics([c for _,_,c in valid_pairs], "net_return"),
                "funding_missing_admitted": len(selected) - len(funded),
                "cost_stress_40bps": metrics(paired, "net_stress_40bps"),
                "by_symbol": {}, "by_kind": {},
            }
            for group in ("symbol", "kind"):
                for value in sorted({r[group] for r in paired}):
                    phase_result["by_"+group][value] = metrics([r for r in paired if r[group] == value], "net_return")
            if hours == 24:
                for label, book in (("signal", paired), ("inverted", [b for _,b,_ in valid_pairs]),
                                    ("always_long", [c for _,_,c in valid_pairs])):
                    curve = equity_curve(book, market, funded=True)
                    curves.extend({**p, "phase": phase, "variant": label} for p in curve)
                    if curve:
                        eq = np.array([p["equity"] for p in curve])
                        phase_result[label + "_portfolio"] = {
                            "return": float(eq[-1] - 1),
                            "max_hourly_drawdown": float((eq / np.maximum.accumulate(eq) - 1).min()),
                            "fixed_initial_notional_per_trade": .2,
                        }
                phase_result["trade_ids"] = [r["source_id"] for r in paired]
            horizon[phase] = phase_result
        summary["horizons"][str(hours)] = horizon
    return summary, outcomes, curves


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    annotation = json.loads((OUT / "ai_annotations.json").read_text(encoding="utf-8"))
    alastar_file = ROOT / "data/research/alastar_20261020/messages.jsonl"
    assert hashlib.sha256(alastar_file.read_bytes()).hexdigest() == annotation["input_sha256"]
    alastar = {m["source_id"]: m for m in read_jsonl(alastar_file)}
    signals = []
    for label in annotation["labels"]:
        if not label["direction"] or label["confidence"] < .75 or label["visible_edited"]:
            continue
        message = alastar[label["source_id"]]
        assert label["evidence_quote"] in message["content"]
        signals.append({"source_id": label["source_id"], "source": "alastartrades", "symbol": label["symbol"],
                        "direction": label["direction"], "published_at": message["published_at"],
                        "url": message["url"], "kind": "offline_ai_prospective_view", "evidence": label["evidence_quote"],
                        "confidence": label["confidence"]})
    alerts = read_jsonl(ROOT / "data/research/cryptoquant_alert_20261020/messages.jsonl")
    signals.extend(s for m in alerts if (s := cryptoquant_signal(m)) is not None)
    (OUT / "frozen_signals.json").write_text(json.dumps(signals, ensure_ascii=False, indent=2), encoding="utf-8")
    market = LocalMarket()
    results, all_outcomes, all_curves = {}, [], []
    for source in ("alastartrades", "cryptoquant_alert"):
        selected = [s for s in signals if s["source"] == source]
        result, outcomes, curves = evaluate_source(selected, market)
        results[source] = result
        all_outcomes.extend(outcomes)
        all_curves.extend({**p, "source": source} for p in curves)
        print(source, json.dumps(result["horizons"]["24"]["holdout"]["funding_covered_matched"]), flush=True)
    (OUT / "summary.json").write_text(json.dumps(results, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    with (OUT / "event_outcomes.jsonl").open("w", encoding="utf-8") as f:
        for row in all_outcomes:
            f.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    pd.DataFrame(all_curves).to_csv(OUT / "hourly_equity.csv", index=False)
    (OUT / "market_sources.json").write_text(json.dumps(market.sources, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
