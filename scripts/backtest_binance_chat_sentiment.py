"""Replay frozen count-based sentiment against public historical BTC prices.

Reads deidentified input/market snapshots only. Never connects to Telegram,
calls a model, changes an observer, or writes to an activated paper ledger.
The coverage-relaxed case is a hypothetical sensitivity, not forward evidence.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research.telegram_event_sentiment import digest, utc
from core.research.telegram_forward import outcome
from core.research import telegram_sentiment_observer as observer

CASES = ("strict", "coverage_relaxed")
SIZES = (10, 20)


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_new(path: Path, value):
    if path.exists():
        if load(path) != value:
            raise ValueError(f"Frozen output differs: {path.name}; use a new study directory")
        return
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def read_inputs(study: Path) -> dict:
    data = load(study / "inputs.json")
    if digest({k: v for k, v in data.items() if k != "snapshot_sha"}) != data["snapshot_sha"]:
        raise ValueError("Input snapshot checksum differs")
    cutoff = utc(data["source_checkpoint"])
    start = utc(data["metadata"]["activation_at"])
    if cutoff > datetime.now(timezone.utc):
        raise ValueError("Future source checkpoint")
    for v in data["views"]:
        if (type(v["side"]) is not int or v["side"] not in (-1, 1)
                or not start <= utc(v["published_at"]) <= utc(v["available_at"]) <= cutoff):
            raise ValueError("Invalid or future view in snapshot")
    if len({v["message_id"] for v in data["views"]}) != len(data["views"]):
        raise ValueError("Duplicate input message id")
    return data


def certified(batch: list[dict], coverage: list) -> bool:
    """Require publication through label availability to lie in one segment."""
    first = min(utc(v["published_at"]) for v in batch)
    last = max(utc(v["available_at"]) for v in batch)
    return any(utc(a) <= first and last <= utc(b) for a, b in coverage)


def make_features(data: dict) -> tuple[list[dict], list[dict]]:
    views = sorted(data["views"], key=lambda v: (utc(v["available_at"]), v["message_id"]))
    groups = defaultdict(list)
    for v in views:
        for i, (a, b) in enumerate(data["coverage"]):
            if utc(a) <= utc(v["published_at"]) <= utc(v["available_at"]) <= utc(b):
                groups[str(i)].append(v)
                break
    features, funnel = [], []
    for case in CASES:
        streams = groups if case == "strict" else {"archive": views}
        for size in SIZES:
            cell = []
            for segment, stream in streams.items():
                for i in range(0, len(stream) - size + 1, size):
                    batch = stream[i:i + size]
                    at = utc(batch[-1]["available_at"])
                    value = observer.measure(batch, size, at, covered=True)
                    value.update(case=case, segment=segment, batch=i // size,
                                 available_at=at.isoformat(),
                                 first_published_at=min(v["published_at"] for v in batch),
                                 coverage_certified=certified(batch, data["coverage"]))
                    value["feature_id"] = digest({"case": case, "size": size, "segment": segment,
                                                  "batch": i // size, "sha": value["feature_sha"]})
                    if case == "strict" and not value["coverage_certified"]:
                        raise ValueError("Strict batch crossed coverage boundary")
                    cell.append(value)
            features.extend(cell)
            funnel.append({"case": case, "size": size, "input_views": len(views),
                           "assigned_views": sum(map(len, streams.values())),
                           "complete_batches": len(cell), "candidate_batches": sum(v["qualified"] for v in cell),
                           "certified_candidates": sum(v["qualified"] and v["coverage_certified"] for v in cell),
                           "incomplete_batch_views": sum(len(s) % size for s in streams.values()),
                           "status_counts": dict(Counter(v["status"] for v in cell))})
    return features, funnel


def make_events(features: list[dict]) -> tuple[list[dict], list[dict]]:
    events, skipped, last_exit = [], [], {}
    for f in sorted(features, key=lambda v: (utc(v["available_at"]), v["feature_id"])):
        if not f["qualified"]:
            continue
        stamp = (utc(f["available_at"]) + timedelta(seconds=60)).timestamp()
        entry = datetime.fromtimestamp((int(stamp) // 300 + 1) * 300, timezone.utc)
        end = entry + timedelta(hours=6)
        for variant, sign in (("follow", 1), ("contrarian", -1)):
            key = (f["case"], f["size"], variant)
            if key in last_exit and entry < last_exit[key]:
                skipped.append({"case": key[0], "size": key[1], "variant": variant,
                                "feature_id": f["feature_id"], "reason": "overlap"})
                continue
            last_exit[key] = end
            events.append({"case": key[0], "size": key[1], "variant": variant,
                           "feature_id": f["feature_id"], "frozen_at": f["available_at"],
                           "entry_at": entry.isoformat(), "exit_at": end.isoformat(),
                           "side": (1 if f["score"] > 0 else -1) * sign,
                           "score": f["score"], "authors": f["authors"], "span_hours": f["span_hours"],
                           "coverage_certified": f["coverage_certified"]})
    return events, skipped


def positive(value) -> float:
    result = float(value)
    if isinstance(value, bool) or not math.isfinite(result) or result <= 0:
        raise ValueError("Invalid market price")
    return result


def market_snapshot(study: Path) -> dict:
    quotes, funds = load(study / "binance_klines.json"), load(study / "binance_funding.json")
    if (quotes["tool"] != "get_futures_usds_kline_candlestick_data"
            or funds["tool"] != "get_futures_usds_funding_rate_history"):
        raise ValueError("Requires public USDT perpetual market receipts")
    for receipt in (quotes, funds):
        if receipt["request"]["symbol"] != "BTCUSDT":
            raise ValueError("Wrong market symbol")
    if quotes["request"]["interval"] != "5m":
        raise ValueError("Requires 5-minute perpetual klines")
    raw_bars, raw_rates = quotes["response"]["result"], funds["response"]["result"]
    if not isinstance(raw_rates, list) or len(raw_rates) >= funds["request"]["limit"]:
        raise ValueError("Funding response is malformed or may be truncated")
    bars, rates, seen_bars, seen_rates = [], [], set(), set()
    for row in raw_bars:
        stamp = int(row[0])
        if stamp in seen_bars or stamp % 300_000:
            raise ValueError("Duplicate or unaligned price bar")
        seen_bars.add(stamp)
        bars.append({"at": datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat(),
                     "open": positive(row[1]), "close": positive(row[4])})
    for row in raw_rates:
        stamp, rate = int(row["fundingTime"]), float(row["fundingRate"])
        if row.get("symbol") != "BTCUSDT" or stamp in seen_rates or not math.isfinite(rate):
            raise ValueError("Invalid, duplicate or wrong-symbol funding row")
        seen_rates.add(stamp)
        rates.append({"at": datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat(),
                      "rate": rate, "mark": positive(row["markPrice"]) if row.get("markPrice") else None})
    return {"symbol": "BTCUSDT", "interval": "5m", "bars": bars, "funding": rates}


def settle(event: dict, market: dict, cutoff: datetime) -> dict:
    if utc(event["exit_at"]) > cutoff:
        return {"status": "immature_exit"}
    # Bracketing payments certify coverage only. Do not include future payments
    # in cash flow; the frozen helper uses entry < settlement <= exit.
    return outcome(event, market["bars"], market["funding"])


def metrics(trades: list[dict]) -> dict:
    settled = [t["outcome"] for t in trades if t["outcome"]["status"] == "settled"]
    values = [v["net"] * 100 for v in settled]
    mean = lambda xs: sum(xs) / len(xs) if xs else None
    without_best = sorted(values)[:-1]
    return {"events": len(trades), "settled": len(values), "pending_or_unavailable": len(trades) - len(values),
            "mean_net_pct": mean(values), "win_rate": mean([v > 0 for v in values]),
            "mean_stress_pct": mean([v["net_stress"] * 100 for v in settled]),
            "mean_same_entry_long_pct": mean([v["same_entry_long_net"] * 100 for v in settled]),
            "mean_excess_pp": mean([(v["net"] - v["same_entry_long_net"]) * 100 for v in settled]),
            "mean_without_best_pct": mean(without_best)}


def evaluate(study: Path) -> dict:
    protocol = load(study / "protocol.json")
    if (protocol["observer_protocol"] != observer.PROTOCOL
            or protocol["observer_implementation_sha"] != observer.implementation_sha()):
        raise ValueError("Observer protocol or implementation changed")
    if (protocol["clocks"] != list(SIZES) or protocol["hold_hours"] != 6
            or protocol["cost_bps"] != 20 or protocol["stress_cost_bps"] != 40
            or set(protocol["cases"]) != set(CASES)):
        raise ValueError("Study parameters do not match the frozen replay")
    data = read_inputs(study)
    features, funnel = make_features(data)
    events, skipped = make_events(features)
    market = market_snapshot(study) if events else {"bars": [], "funding": []}
    for e in events:
        e["outcome"] = settle(e, market, utc(data["source_checkpoint"]))
    cells = [{"case": case, "size": size, "variant": variant,
              **metrics([e for e in events if (e["case"], e["size"], e["variant"]) == (case, size, variant)])}
             for case in CASES for size in SIZES for variant in ("follow", "contrarian")]
    files = ("protocol.json", "inputs.json") + (("binance_klines.json", "binance_funding.json") if events else ())
    result = {"kind": "retrospective research, not actual trades", "as_of": data["source_checkpoint"],
              "activation_at": data["metadata"]["activation_at"], "source_counts": data["metadata"]["counts"],
              "quality_checks": data["quality_checks"], "funnel": funnel, "metrics": cells,
              "events": events, "skipped": skipped, "promotion_allowed": False,
              "input_hashes": {n: hashlib.sha256((study / n).read_bytes()).hexdigest() for n in files},
              "analysis_implementation_sha": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    save_new(study / "features.json", features)
    save_new(study / "results.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", type=Path, default=ROOT / "data/research/binance_chat_backtest_20261031")
    args = parser.parse_args()
    study = args.study.resolve()
    if not study.is_relative_to(ROOT / "data/research"):
        raise ValueError("Use an independent local research snapshot directory")
    if (study / "activation.json").exists() or (study / "observations.sqlite").exists():
        raise ValueError("Never write to an activated study")
    result = evaluate(study)
    print(json.dumps({k: result[k] for k in ("as_of", "quality_checks", "funnel", "metrics", "events")}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
