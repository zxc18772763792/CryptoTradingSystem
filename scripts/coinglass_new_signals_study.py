"""Do the Coinglass datasets we have not used yet predict alt returns beyond the 8 tested signals? (2026-10-12)

WHY THIS EXISTS:
After the 10/12 audit the user asked to test the Coinglass data with alt history that no study has
used (docs/COINGLASS_DATA_CATALOG.md section 6). Looked at first and left out, with reasons:
  * coin-margined open interest: empty for alts (the 10/11 probe got no rows for WLD), and for alts
    stablecoin-margined OI is nearly all of the aggregated OI already tested;
  * futures / spot coin "netflow": a snapshot with no history, and it is taker buy minus taker sell,
    the data behind the tested taker signal.
CVD needs no new download: it is the running sum of the aggregated taker buy - sell already held.

PRE-REGISTERED before any of the new data was downloaded (do not tune after seeing results):
  Universe: the wide universe of scripts/coinglass_wide_study.py (the 131 study coins plus the
    held-out coins among the 200 most-traded perps), on their Binance USDT perps. A coin counts on
    each day it has the signal and the label; days need >= 30 coins.
  New data (2024-01-01 .. 2026-10-10 UTC, one request per coin and dataset, the 10/11 downloader's
    pacing; a coin the relay refuses for a dataset simply lacks that signal):
    net_position  /api/futures/v2/net-position/history, Binance pair, daily: net_long_change, net_short_change
    spot_taker    /api/spot/aggregated-taker-buy-sell-volume/history, Binance+OKX+Bybit spot, daily
    depth         /api/futures/orderbook/aggregated-ask-bids-history, Binance+OKX+Bybit, range 1 (within
                  +/-1% of price), 4h bars (history starts 2024-10-21): aggregated_bids_usd, aggregated_asks_usd
    hl_ratio      /api/hyperliquid/global-long-short-account-ratio/history, daily (starts 2025-07-02):
                  long and short account percent
  Timing: flows and depth of day D are known at D+1 00:00 (daily bars stamped D; the six 4h depth
    bars stamped D 00:00 .. 20:00). The Hyperliquid ratio is used from its bar stamped D for day D:
    safe whichever end of the day that bar describes (Binance's daily ratio is the snapshot at its
    stamp; this one was not checked, so it is never used early).
  Signals (7), all known at D+1 00:00:
    net_position_z      z of the 3-day sum of (net long change - net short change) vs its own prior 30 days
    taker_imbalance_14d (buy - sell) / (buy + sell) over D-13 .. D, futures aggregated: the CVD trend
                        (the 3-day version is one of the 8 tested signals)
    spot_imbalance_3d   the same over D-2 .. D on spot taker volume
    spot_minus_perp_3d  spot_imbalance_3d - taker_imbalance_3d (spot-led buying)
    depth_imbalance_1d  mean over day D's bars of (bids - asks) / (bids + asks) within +/-1%
    depth_change_7d     log mean total depth of day D - log mean total depth over D-7 .. D-1
    hl_retail_z         z of log(Hyperliquid long % / short %) vs its own prior 7 days
  Horizons 1, 3 and 7 days, log returns from the D+1 00:00 open (as on 10/11).
  Pass rule of the 10/11 study (all must hold): |IC| >= 0.02 with the 90% CI (week-block bootstrap)
    excluding 0; whole-coin permutation p < 0.05/21; an incremental IC of the same sign with its CI
    excluding 0 after residualizing the signal's ranks on the past 1-day and 7-day returns, the 14-day
    volatility AND the 10/11 7-day composite score (which carries the 8 tested signals); the
    top-minus-bottom quintile in the IC's direction net of 0.12% per round trip > 0; the same IC sign
    in both halves of the days the signal has (split at their middle day).
  Reported, never the verdict: entries 4h later; the last 12 months.
  Next step fixed now: a passing signal is not added to any running tracker. It would get its own
    pre-registered forward test or combination study.

    python scripts/coinglass_new_signals_study.py --fetch   # ~4 requests per coin, ~3h at 5 req/min (resumable)
    python scripts/coinglass_new_signals_study.py           # evaluate
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.research import coinglass_composite_tracker as ct  # noqa: E402
from scripts import coinglass_combination_study as combo  # noqa: E402
from scripts import coinglass_positioning_study as single  # noqa: E402
from scripts import coinglass_wide_study as wide  # noqa: E402
from scripts.lsr_positioning_study import residualize  # noqa: E402

RAW_NEW = wide.WIDE_DIR / "raw_new"
RESULTS_PATH = wide.WIDE_DIR / "new_signals_results.json"
EXCHANGES = single.EXCHANGES
# dataset -> (path, symbol kind, extra params, first day with data, request limit)
NEW: Dict[str, Tuple[str, str, Dict[str, Any], datetime, int]] = {
    "net_position": ("/api/futures/v2/net-position/history", "pair", {"interval": "1d"}, single.START, 4500),
    "spot_taker": ("/api/spot/aggregated-taker-buy-sell-volume/history", "base",
                   {"interval": "1d", "exchange_list": EXCHANGES}, single.START, 4500),
    "depth_4h": ("/api/futures/orderbook/aggregated-ask-bids-history", "base",
                 {"interval": "4h", "exchange_list": EXCHANGES, "range": "1"}, datetime(2024, 10, 21, tzinfo=timezone.utc), 4500),
    "hl_ratio": ("/api/hyperliquid/global-long-short-account-ratio/history", "base", {"interval": "1d"},
                 datetime(2025, 7, 2, tzinfo=timezone.utc), 1000),
}
SIGNALS = ("net_position_z", "taker_imbalance_14d", "spot_imbalance_3d", "spot_minus_perp_3d",
           "depth_imbalance_1d", "depth_change_7d", "hl_retail_z")
ALPHA = 0.05 / (len(SIGNALS) * len(single.HORIZONS))


# ----------------------------------------------------------------------------------------- fetch
async def _series(client, name: str, coin: str, pair: str, last_call: List[float]) -> List[Dict[str, Any]]:
    """All rows of one dataset from its first day to the study's end; a page that fills the limit is
    followed by a request for the older part it left out."""
    path, kind, extra, first_day, limit = NEW[name]
    params: Dict[str, Any] = {**extra, "limit": limit}
    params.update({"symbol": pair, "exchange": "Binance"} if kind == "pair" else {"symbol": coin})
    lo, hi = single._ms(first_day), single._ms(single.END) - 1
    by_ts: Dict[int, Dict[str, Any]] = {}
    for _ in range(6):
        page = await single._coinglass(client, path, {**params, "start_time": lo, "end_time": hi}, last_call)
        stamps = [int(r["time"]) for r in page if r.get("time") is not None]
        by_ts.update({int(r["time"]): r for r in page if r.get("time") is not None})
        if len(stamps) < min(limit, single.PAGE_CAP) or min(stamps) <= lo + 2 * single.DAY_MS:
            break
        hi = min(stamps) - 1
    return [by_ts[t] for t in sorted(by_ts) if t < single._ms(single.END)]


async def fetch_all(coins: Dict[str, str]) -> None:
    """Resumable: a coin's file keeps every dataset already answered (a refusal is an answer)."""
    from core.data.coinglass_client import CoinglassClient

    RAW_NEW.mkdir(parents=True, exist_ok=True)
    last_call = [0.0]
    started, asked = time.monotonic(), 0
    async with CoinglassClient() as client:
        for i, (coin, pair) in enumerate(sorted(coins.items())):
            path = RAW_NEW / f"{pair}.json"
            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"pair": pair, "coin": coin}
            for name in NEW:
                if name in payload:
                    continue
                try:
                    payload[name] = await _series(client, name, coin, pair, last_call)
                    payload.get("errors", {}).pop(name, None)
                except RuntimeError as exc:
                    if "business code" in str(exc):  # the relay has no such series for this coin: final
                        payload[name] = []
                    payload.setdefault("errors", {})[name] = str(exc)[:160]
                except Exception as exc:  # noqa: BLE001 - a timeout: asked again on the next run
                    payload.setdefault("errors", {})[name] = f"{type(exc).__name__}: {str(exc)[:120]}"
                asked += 1
                path.write_text(json.dumps(payload), encoding="utf-8")
            rate = asked / max(1.0, (time.monotonic() - started) / 60)
            print(f"[{i + 1}/{len(coins)}] {pair}: " + ", ".join(f"{n}={len(payload.get(n) or [])}" for n in NEW)
                  + f" | {rate:.1f} req/min", flush=True)


def wide_coins(universe: Dict[str, Any]) -> Dict[str, str]:
    return {**universe["known"], **universe["held_out"]}


# -------------------------------------------------------------------------------------- signals
def _day_frame(rows_by_pair: Dict[str, List[Dict[str, Any]]], field: str, grid: pd.DatetimeIndex) -> pd.DataFrame:
    cols = {}
    for pair, rows in rows_by_pair.items():
        values = {pd.Timestamp(int(r["time"]), unit="ms", tz="UTC"): pd.to_numeric(r.get(field), errors="coerce")
                  for r in rows or [] if r.get("time") is not None}
        if values:
            cols[pair] = pd.Series(values, dtype=float).sort_index()
    return pd.DataFrame(cols).reindex(grid).sort_index(axis=1).astype(float) if cols else pd.DataFrame(index=grid)


def _depth_frames(rows_by_pair: Dict[str, List[Dict[str, Any]]], grid: pd.DatetimeIndex) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Per day D: the mean imbalance and the mean total depth of the 4h bars stamped D 00:00 .. 20:00."""
    imb, tot = {}, {}
    for pair, rows in rows_by_pair.items():
        if not rows:
            continue
        stamps = pd.to_datetime([int(r["time"]) for r in rows], unit="ms", utc=True)
        bids = pd.to_numeric(pd.Series([r.get("aggregated_bids_usd") for r in rows], index=stamps), errors="coerce")
        asks = pd.to_numeric(pd.Series([r.get("aggregated_asks_usd") for r in rows], index=stamps), errors="coerce")
        total = (bids + asks).where(lambda s: s > 0)
        day = stamps.floor("D")
        imb[pair] = ((bids - asks) / total).groupby(day).mean()
        tot[pair] = total.groupby(day).mean()
    return (pd.DataFrame(imb).reindex(grid).sort_index(axis=1).astype(float),
            pd.DataFrame(tot).reindex(grid).sort_index(axis=1).astype(float))


def _imbalance(buy: pd.DataFrame, sell: pd.DataFrame, days: int) -> pd.DataFrame:
    b, s = buy.rolling(days).sum(), sell.rolling(days).sum()
    return (b - s) / (b + s).where(lambda x: x > 0)


def new_signals(frames: Dict[str, pd.DataFrame], raw: Dict[str, Dict[str, Any]], index: pd.DatetimeIndex) -> Dict[str, pd.DataFrame]:
    grid = frames["oi"].index
    pick = lambda name: {pair: payload.get(name) or [] for pair, payload in raw.items()}  # noqa: E731
    net_long = _day_frame(pick("net_position"), "net_long_change", grid)
    net_short = _day_frame(pick("net_position"), "net_short_change", grid)
    spot_buy = _day_frame(pick("spot_taker"), "aggregated_buy_volume_usd", grid)
    spot_sell = _day_frame(pick("spot_taker"), "aggregated_sell_volume_usd", grid)
    hl_long = _day_frame(pick("hl_ratio"), "global_account_long_percent", grid)
    hl_short = _day_frame(pick("hl_ratio"), "global_account_short_percent", grid)
    depth_imb, depth_total = _depth_frames(pick("depth_4h"), grid)
    net3 = (net_long - net_short).rolling(3).sum()
    perp3 = _imbalance(frames["buy"], frames["sell"], 3)
    spot3 = _imbalance(spot_buy, spot_sell, 3)
    log_depth = np.log(depth_total)
    out = {
        "net_position_z": single._prior_z(net3, 30),
        "taker_imbalance_14d": _imbalance(frames["buy"], frames["sell"], 14),
        "spot_imbalance_3d": spot3,
        "spot_minus_perp_3d": spot3 - perp3.reindex(columns=spot3.columns),
        "depth_imbalance_1d": depth_imb,
        "depth_change_7d": log_depth - np.log(depth_total.shift(1).rolling(7, min_periods=5).mean()),
        "hl_retail_z": single._prior_z(np.log((hl_long / hl_short).where(lambda x: x > 0)), 7),
    }
    return {k: v.reindex(index=index) for k, v in out.items()}


# -------------------------------------------------------------------------------------- evaluate
def _halves(signal: pd.DataFrame, label: pd.DataFrame) -> Tuple[float, float]:
    ic = single.row_ic(signal, label, single.MIN_COINS).dropna()
    if len(ic) < 20:
        return float("nan"), float("nan")
    middle = ic.index[len(ic) // 2]
    return float(ic[ic.index < middle].mean()), float(ic[ic.index >= middle].mean())


def evaluate(panel: Dict[str, pd.DataFrame], signals: Dict[str, pd.DataFrame], score: pd.DataFrame,
             reps: int = single.N_PERM) -> List[Dict[str, Any]]:
    results = []
    for name in SIGNALS:
        signal = signals[name].reindex(columns=panel["fwd_1d"].columns)
        controls = [panel["past_1d"], panel["past_7d"], panel["vol_14d"], score]
        residual = residualize(signal, controls)
        for h in single.HORIZONS:
            label = panel[f"fwd_{h}d"]
            ic, ci, n_days = single._ic_ci(signal, label)
            if n_days == 0:
                results.append({"signal": name, "horizon": h, "days": 0, "passes": False})
                continue
            perm = single.coin_permutation(signal, label, reps=reps)
            inc, inc_ci, _ = single._ic_ci(residual, label)
            first, second = _halves(signal, label)
            spread = single.quintile_spread(signal, label, direction=1.0 if ic >= 0 else -1.0, horizon=h)
            coins = signal.notna().sum(axis=1)
            stat = abs(ic) >= single.IC_MIN and (ci[0] > 0 or ci[1] < 0) and perm["p"] < ALPHA
            incr = np.sign(inc) == np.sign(ic) and (inc_ci[0] > 0 or inc_ci[1] < 0)
            stable = np.sign(first) == np.sign(ic) and np.sign(second) == np.sign(ic)
            results.append({"signal": name, "horizon": h, "days": n_days, "coins_median": float(coins[coins >= single.MIN_COINS].median()),
                            "first_day": str(signal.dropna(how="all").index.min().date()) if signal.notna().any().any() else None,
                            "ic": ic, "ci": ci, "perm_p": perm["p"], "inc_ic": inc, "inc_ci": inc_ci,
                            "first_half": first, "second_half": second, "spread": spread,
                            "passes": bool(stat and incr and spread["net_per_day"] > 0 and stable)})
    return results


def robustness(frames, raw, results) -> List[str]:
    late = wide.wide_panel(frames, entry="open_04")
    late_signals = new_signals(frames, raw, late["fwd_1d"].index)
    base = wide.wide_panel(frames)
    base_signals = new_signals(frames, raw, base["fwd_1d"].index)
    recent = base["fwd_1d"].index >= base["fwd_1d"].index.max() - pd.Timedelta(days=365)
    lines = ["robustness (reported, not the verdict): entries 4h later | last 12 months"]
    for r in results:
        if not r.get("days") or not (r["ci"][0] > 0 or r["ci"][1] < 0):
            continue
        name, h = r["signal"], r["horizon"]
        ic_late, ci_late, _ = single._ic_ci(late_signals[name], late[f"fwd_{h}d"])
        ic_recent, ci_recent, _ = single._ic_ci(base_signals[name][recent], base[f"fwd_{h}d"][recent])
        lines.append(f"  {name:20s} {h}d  4h later {ic_late:+.4f} [{ci_late[0]:+.4f}, {ci_late[1]:+.4f}]"
                     f" | last 12m {ic_recent:+.4f} [{ci_recent[0]:+.4f}, {ci_recent[1]:+.4f}]")
    if len(lines) == 1:
        lines.append("  no signal has a CI excluding 0")
    return lines


def report(results: List[Dict[str, Any]]) -> str:
    lines = [f"pass rule: |IC|>={single.IC_MIN}, CI excl. 0, perm p<{ALPHA:.4f}, incremental (past returns, vol, composite), "
             "net quintile > 0, same sign in both halves"]
    for r in results:
        if not r.get("days"):
            lines.append(f"  {r['signal']:20s} {r['horizon']}d  no data")
            continue
        lines.append(f"  {r['signal']:20s} {r['horizon']}d  days {r['days']:3d} coins~{r['coins_median']:.0f} from {r['first_day']} "
                     f"IC {r['ic']:+.4f} [{r['ci'][0]:+.4f}, {r['ci'][1]:+.4f}] p {r['perm_p']:.3f} "
                     f"inc {r['inc_ic']:+.4f} [{r['inc_ci'][0]:+.4f}, {r['inc_ci'][1]:+.4f}] "
                     f"halves {r['first_half']:+.4f}/{r['second_half']:+.4f} "
                     f"q-spread net {r['spread']['net_per_day'] * 100:+.3f}%/d | {'PASS' if r['passes'] else 'fail'}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fetch", action="store_true", help="download the new datasets for the wide universe (resumable)")
    args = parser.parse_args(argv)
    universe = wide.build_universe()
    if args.fetch:
        asyncio.run(fetch_all(wide_coins(universe)))
        return 0
    raw = {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted(RAW_NEW.glob("*.json"))}
    frames = wide.wide_frames()
    panel = wide.wide_panel(frames)
    perps = sorted(set(wide_coins(universe).values()) & set(panel["fwd_1d"].columns))
    panel = {k: v.reindex(columns=perps) for k, v in panel.items()}
    score = combo.composite(panel, ct.WEIGHTS)
    signals = new_signals(frames, raw, panel["fwd_1d"].index)
    results = evaluate(panel, signals, score)
    RESULTS_PATH.write_text(json.dumps(results, indent=1, default=str), encoding="utf-8")
    print(report(results))
    print("\n".join(robustness(frames, raw, results)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
