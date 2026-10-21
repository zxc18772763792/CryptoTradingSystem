"""Read-only historical exploration; protocol lives beside the generated results.

No order APIs, app imports, downloads, policy changes, or parameter sweep.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/research/strategy_exploration_20261020/carry"
DAY = pd.Timedelta(days=1)
SOURCES: list[dict] = []


def read_frame(path: Path) -> pd.DataFrame:
    blob = path.read_bytes()
    SOURCES.append({"path": path.relative_to(ROOT).as_posix(), "bytes": len(blob),
                    "sha256": hashlib.sha256(blob).hexdigest()})
    frame = pd.read_parquet(io.BytesIO(blob))
    frame.index = pd.to_datetime(frame.index, utc=True)
    return frame


def prices(exchange: str, coin: str) -> tuple[pd.DataFrame, dict]:
    directory = ROOT / "data/historical" / exchange / f"{coin}_USDT"
    # Known legacy spot monoliths use CST-naive, confirmed by independent UTC
    # exchange anchors, while newer partitions have mixed provenance. Do not
    # optimize a price lag or mix unverified partitions into this study.
    files = [directory / "1h.parquet"]
    if exchange != "binance":
        files += sorted((directory / "1h_parts").glob("*.parquet"))
    frames = [read_frame(path) for path in files if path.exists()]
    joined = pd.concat(frames).sort_index(kind="stable")
    timezone_audit = {"assumed_source": "UTC from futures collector"}
    if exchange == "binance":
        anchors = json.loads((OUT / "official_utc_anchors.json").read_text(encoding="utf-8"))
        shifted_matches = asis_matches = tested = 0
        for item in anchors:
            if item["request"]["symbol"] != coin + "USDT":
                continue
            for raw in item["result"]["result"]:
                t = pd.to_datetime(raw[0], unit="ms", utc=True)
                expected = np.array([float(raw[i]) for i in (1, 2, 3, 4)])
                for offset in (0, 8):
                    index = t + pd.Timedelta(hours=offset)
                    matched = index in joined.index and np.array_equal(
                        joined.loc[index, ["open", "high", "low", "close"]].to_numpy(), expected)
                    if offset == 0:
                        asis_matches += bool(matched)
                    else:
                        shifted_matches += bool(matched)
                tested += 1
        assert tested == 9 and shifted_matches == 9 and asis_matches == 0, "Unverified spot timezone"
        joined.index = joined.index - pd.Timedelta(hours=8)
        timezone_audit = {"assumed_source": "legacy Asia/Shanghai-naive, corrected by minus 8 hours",
                          "official_anchor_bars": tested, "shifted_exact_OHLC_matches": shifted_matches,
                          "unshifted_exact_OHLC_matches": asis_matches,
                          "unverified_spot_partitions_excluded": True,
                          "limitation": "Three separated anchor windows verify encoding, not every historical bar."}
    duplicates = int(joined.index.duplicated().sum())
    joined = joined[~joined.index.duplicated(keep="last")]
    valid = np.isfinite(joined["close"]) & (joined["close"] > 0) & (joined["volume"] > 0)
    joined = joined.loc[valid]
    return joined, {"files": len(frames), "rows": len(joined), "duplicates_merged": duplicates, "timezone_audit": timezone_audit,
                    "start": str(joined.index.min()), "end": str(joined.index.max())}


def funding_window(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, list[str]]:
    window = frame[(frame.index > start) & (frame.index <= end)]
    reasons = []
    if window.empty:
        return window, ["no_funding_rows"]
    if not np.isfinite(window["funding_rate"]).all():
        reasons.append("missing_funding_rate")
    if not (np.isfinite(window["mark_price"]) & (window["mark_price"] > 0)).all():
        reasons.append("missing_mark_price")
    largest_gap = max((window.index[0] - start).total_seconds(),
                      (end - window.index[-1]).total_seconds(),
                      window.index.to_series().diff().dt.total_seconds().max(skipna=True) or 0)
    if largest_gap > 12 * 3600:
        reasons.append("funding_coverage_gap_over_12h")
    return window, reasons


def interval_mean(values: list[float]) -> list[float] | None:
    if len(values) < 8:
        return None
    # Three consecutive 28-day books per block; do not treat assets as replicates.
    rng = np.random.default_rng(20261020)
    a = np.asarray(values, dtype=float)
    draws = []
    block_count = int(np.ceil(len(a) / 3))
    for _ in range(2000):
        starts = rng.integers(0, len(a), size=block_count)
        indices = [(int(s) + j) % len(a) for s in starts for j in range(3)][:len(a)]
        draws.append(float(np.mean(a[indices])))
    return [float(x) for x in np.percentile(draws, [5, 95])]


def stats(records: list[dict], field: str) -> dict:
    values = [r[field] for r in records if r.get(field) is not None]
    return {"n_books": len(values), "mean_pct": float(np.mean(values)) if values else None,
            "sum_fixed_capital_pct": float(np.sum(values)) if values else None,
            "positive_share": sum(v > 0 for v in values) / len(values) if values else None,
            "min_pct": min(values) if values else None, "max_pct": max(values) if values else None,
            "ci90_moving_block_pct": interval_mean(values)}


def main() -> None:
    assert (OUT / "protocol.md").exists(), "Freeze the protocol before calculation"
    metadata, loaded = {}, {}
    for coin in ("BTC", "ETH"):
        spot, sm = prices("binance", coin)
        perp, pm = prices("binance_futures", coin)
        fund = read_frame(ROOT / "data/historical/binance_futures" / f"{coin}_USDT" / "funding_rate.parquet")
        fund = fund.sort_index()
        duplicate_times = int(fund.index.duplicated().sum())
        fund = fund[~fund.index.duplicated(keep="last")]
        usable = fund[np.isfinite(fund["mark_price"]) & (fund["mark_price"] > 0)]
        metadata[coin] = {"spot": sm, "perpetual": pm, "funding_rows": len(fund),
                          "funding_duplicate_times": duplicate_times,
                          "funding_missing_mark_rows": int(fund["mark_price"].isna().sum()),
                          "first_valid_mark": str(usable.index.min())}
        loaded[coin] = spot, perp, fund
    end_available = min(min(s.index.max(), p.index.max()) for s, p, _ in loaded.values())
    starts = pd.date_range("2021-01-04T00:00:00Z", end_available - 28 * DAY, freq="28D")
    legs, combined = [], []
    for bar in starts:
        exit_bar = bar + 28 * DAY
        entry = bar + pd.Timedelta(hours=1, seconds=1)
        exit_at = exit_bar + pd.Timedelta(hours=1, seconds=1)
        book = {"entry": entry.isoformat(), "exit": exit_at.isoformat(), "entry_year": entry.year}
        by_coin = []
        for coin, (spot, perp, fund) in loaded.items():
            row = {**book, "coin": coin}
            reasons = []
            for frame, label in ((spot, "spot"), (perp, "perpetual")):
                if bar not in frame.index:
                    reasons.append(f"missing_{label}_entry_bar")
                if exit_bar not in frame.index:
                    reasons.append(f"missing_{label}_exit_bar")
            previous, previous_reasons = funding_window(fund, entry - 7 * DAY, entry)
            actual, future_reasons = funding_window(fund, entry, exit_at)
            reasons.extend("trailing_" + r for r in previous_reasons)
            reasons.extend("holding_" + r for r in future_reasons)
            if reasons:
                row.update(status="unavailable", reasons=reasons)
                legs.append(row)
                by_coin.append(row)
                continue
            s0, s1 = float(spot.at[bar, "close"]), float(spot.at[exit_bar, "close"])
            f0, f1 = float(perp.at[bar, "close"]), float(perp.at[exit_bar, "close"])
            q = 1 / (s0 + f0)
            price = q * ((s1 - s0) - (f1 - f0))
            funding = q * float((actual["funding_rate"] * actual["mark_price"]).sum())
            positive_funding = q * float((actual["funding_rate"].clip(lower=0) * actual["mark_price"]).sum())
            negative_funding = q * float((actual["funding_rate"].clip(upper=0) * actual["mark_price"]).sum())
            cost = q * (.0015 * (s0 + s1) + .0010 * (f0 + f1))
            forecast = 4 * float(previous["funding_rate"].sum()) * f0
            estimate_cost = .003 * s0 + .002 * f0
            selected = forecast > estimate_cost
            net = 100 * (price + funding - cost)
            stressed = 100 * (price + funding - 2 * cost)
            half_positive = 100 * (price + .5 * positive_funding + negative_funding - cost)
            high = float(perp.loc[(perp.index > bar) & (perp.index <= exit_bar), "high"].max()) / f0
            row.update(status="available", reasons=[], spot_entry=s0, spot_exit=s1,
                       perp_entry=f0, perp_exit=f1, quantity_per_capital=q,
                       trailing_rate_pct=100 * float(previous["funding_rate"].sum()),
                       forecast_cash_per_unit_spot=forecast / s0,
                       estimated_cost_cash_per_unit_spot=estimate_cost / s0,
                       selected=selected, funding_settlements=len(actual),
                       basis_pct=100 * price, funding_pct=100 * funding, cost_pct=100 * cost,
                       net_pct=net, double_cost_pct=stressed, half_positive_funding_pct=half_positive,
                       candidate_pct=net if selected else 0.,
                       candidate_double_cost_pct=stressed if selected else 0.,
                       candidate_half_positive_funding_pct=half_positive if selected else 0.,
                       max_perp_high_to_entry=high,
                       short_upside_near_initial_collateral=high >= 1.8)
            assert abs(net - (row["basis_pct"] + row["funding_pct"] - row["cost_pct"])) < 1e-9
            assert stressed <= net and half_positive <= net + 1e-10
            assert abs(q * (s0 + f0) - 1) < 1e-12
            legs.append(row)
            by_coin.append(row)
        if all(r["status"] == "available" for r in by_coin):
            for field in ("net_pct", "double_cost_pct", "half_positive_funding_pct", "candidate_pct",
                          "candidate_double_cost_pct", "candidate_half_positive_funding_pct",
                          "basis_pct", "funding_pct", "cost_pct"):
                book[field] = sum(r[field] for r in by_coin) / 2
            book["selected_allocations"] = sum(r["selected"] for r in by_coin)
            book["candidate_minus_control_pp"] = book["candidate_pct"] - book["net_pct"]
            for rate in (2, 5):
                book[f"candidate_less_{rate}pct_capital_cost_pct"] = book["candidate_pct"] - rate * 28 / 365
            book["status"] = "available"
        else:
            book["status"] = "unavailable"
        combined.append(book)
    valid_books = [b for b in combined if b["status"] == "available"]
    fields = ("net_pct", "candidate_pct", "candidate_minus_control_pp", "double_cost_pct",
              "candidate_double_cost_pct", "half_positive_funding_pct", "candidate_half_positive_funding_pct",
              "candidate_less_2pct_capital_cost_pct", "candidate_less_5pct_capital_cost_pct")
    summary = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "hypotheses": 1, "control_policies": 1, "parameter_search": False,
               "unit": "percent_per_28_day_book_of_spot_cost_plus_full_short_collateral",
               "planned_calendar_books": len(combined), "available_books": len(valid_books),
               "unavailable_books": len(combined) - len(valid_books),
               "available_start": valid_books[0]["entry"] if valid_books else None,
               "available_last_exit": valid_books[-1]["exit"] if valid_books else None,
               "full_history_performance_claim": False,
               "available_subsample_only": {k: stats(valid_books, k) for k in fields},
               "by_entry_year": {str(y): {k: stats([b for b in valid_books if b["entry_year"] == y], k)
                                          for k in fields} for y in sorted({b["entry_year"] for b in combined})},
               "selected_allocations": sum(b.get("selected_allocations", 0) for b in valid_books),
               "available_allocations": 2 * len(valid_books),
               "high_upside_margin_flags": sum(r.get("short_upside_near_initial_collateral", False) for r in legs),
               "metadata": metadata,
               "limitations": ["Only accounting on covered history; not real fills, margin transfers or liquidation replay.",
                               "Historical data and protocol choices are exploratory; no untouched holdout or forward confirmation.",
                               "Missing marks exclude a substantial early period, not zero funding; covered-period statistics are conditional.",
                               "Fixed BTC/ETH allocation and one 28-day phase do not establish general carry profitability.",
                               "Both legs bear exchange/custody risk; no guarantee of price-neutral execution or funding persistence."]}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "results.json").write_text(json.dumps({"summary": summary, "books": combined, "legs": legs},
                                                 ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "source_manifest.json").write_text(json.dumps(SOURCES, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(legs).to_csv(OUT / "legs.csv", index=False, encoding="utf-8-sig")
    print(json.dumps(summary, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
