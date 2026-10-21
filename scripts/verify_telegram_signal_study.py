"""Independently recompute the primary study from raw parquet and saved trade IDs."""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/research/telegram_signal_study_20261020"


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


@lru_cache(maxsize=None)
def frame(symbol, kind):
    directory = ROOT / "data/historical/binance_futures" / (symbol[:-4] + "_USDT")
    files = [p for p in [directory / (kind + ".parquet"), *sorted((directory / (kind + "_parts")).glob("*.parquet"))] if p.exists()]
    data = pd.concat([pd.read_parquet(path) for path in files]).sort_index(kind="stable")
    data.index = pd.to_datetime(data.index, utc=True)
    return data[~data.index.duplicated(keep="last")]


def main():
    summary = json.loads((OUT / "summary.json").read_text(encoding="utf-8"))
    events = read_jsonl(OUT / "event_outcomes.jsonl")
    signals = json.loads((OUT / "frozen_signals.json").read_text(encoding="utf-8"))
    assert len(signals) == len({s["source_id"] for s in signals})
    assert any(s["source"] == "cryptoquant_alert" and s["direction"] == 1 for s in signals)
    source_checks = {}
    for name in ("alastar", "cryptoquant_alert", "cryptoquant_official"):
        root = ROOT / f"data/research/{name}_20261020"
        rows = read_jsonl(root / "messages.jsonl")
        manifest = json.loads((root / "collection_manifest.json").read_text(encoding="utf-8"))
        assert len(rows) == manifest["parsed_message_count"]
        assert len(rows) == len({r["source_id"] for r in rows})
        assert hashlib.sha256((root / "messages.jsonl").read_bytes()).hexdigest() == manifest["messages_sha256"]
        source_checks[name] = {"messages":len(rows),"unique":True,"manifest_sha_matches":True,
                               "edited_posts":sum(r["payload"]["telegram_edited"] for r in rows)}
    results = {}
    primary = [r for r in events if r["holding_hours"] == 24 and r["status"] == "ok"]
    for source, data in summary.items():
        results[source] = {}
        for phase in ("development", "holdout"):
            headline = data["horizons"]["24"][phase]
            ids = set(headline["trade_ids"])
            rows = [r for r in primary if r["source"] == source and r["phase"] == phase and r["source_id"] in ids]
            assert len(rows) == len(ids) == headline["funding_covered_matched"]["n"]
            recomputed = []
            for row in rows:
                start, end = pd.Timestamp(row["entry_at"]), pd.Timestamp(row["exit_at"])
                expected_start = (pd.Timestamp(row["published_at"]) + pd.Timedelta(seconds=60)).ceil("h")
                assert start == expected_start and start > pd.Timestamp(row["published_at"])
                prices = frame(row["symbol"], "1h")
                p0 = float(prices.loc[start, "open"])
                if row["liquidated_1x_approx"]:
                    gross = -1.
                else:
                    gross = row["direction"] * (float(prices.loc[end, "open"]) / p0 - 1)
                funding = frame(row["symbol"], "funding_rate")
                paid = funding[(funding.index > start) & (funding.index <= end)]
                fund_return = -row["direction"] * float((paid["funding_rate"] * paid["mark_price"] / p0).sum())
                value = gross - .002 + fund_return
                assert abs(value - row["net_return"]) < 1e-10
                recomputed.append(value)
            values = np.array(recomputed)
            assert abs(float(values.mean()) - headline["funding_covered_matched"]["mean_trade_return"]) < 1e-10
            assert abs(.2 * float(values.sum()) - headline["signal_portfolio"]["return"]) < 1e-10
            results[source][phase] = {
                "trades_recomputed":len(values),"all_entry_prices_and_funding_match":True,
                "mean_net":float(values.mean()),"portfolio_return":.2*float(values.sum()),
                "best_trade_share_of_total_net":float(values.max()/values.sum()) if values.sum()>0 else None,
                "leave_best_out_mean":float(np.delete(values,values.argmax()).mean()) if len(values)>1 else None,
            }
    for filename, field in (("official_price_anchors.json","utc_exact_match"),("official_funding_anchors.json","exact_match")):
        anchors=json.loads((OUT/filename).read_text(encoding="utf-8"))
        assert all(a[field] for a in anchors)
    receipt={"assessment":"share_with_caveats_not_ready_for_live_strategy", "source_checks":source_checks,
             "primary_recomputation":results,"official_price_anchors_matched":4,"official_funding_anchors_matched":2,
             "limitations":["Current preview versions and deleted-message survivorship are not point-in-time data.",
                            "Historical holdouts are not untouched prospective out-of-sample tests.",
                            "PumpGlass private bot alert history was not obtained.",
                            "The 100-page CryptoQuant archive is bounded, not complete channel history.",
                            "Portfolio drawdown is hourly marked, liquidation is an OHLC approximation."]}
    (OUT/"validation.json").write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(receipt,ensure_ascii=True,indent=2))


if __name__ == "__main__":
    main()
