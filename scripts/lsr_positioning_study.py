"""Do Binance long/short positioning ratios predict cross-sectional returns? (2026-10-09)

WHY THIS EXISTS:
The Coinglass relay (vip2) sells "smart money"/whale long-short structure (LSR) for ~739 perps,
live only, no history. Binance publishes the same idea for free with 30 days of history: the
top-trader position ratio and the all-account ratio. If those carry no cross-sectional edge here,
the relay's version is unlikely to; if they do, archive the relay's richer fields forward.

PRE-REGISTERED before the first run (do not tune after seeing results):
  Universe: data/research/xs_reversal/universe.json (131 bases with a TRADING USDT perp).
  Data: 1h topLongShortPositionRatio, 1h globalLongShortAccountRatio, 1h perp klines, last 30 days.
  Decision hours t use ratio points stamped <= t - 1h (one bar of publication lag).
  Signals (z-scores against the coin's own trailing 72 hours):
    S1 retail_z         z of log(global account L/S)          - crowding of all accounts
    S2 top_z            z of log(top-trader position L/S)     - crowding of the largest accounts
    S3 top_flow_24h     log(top position L/S) change over 24h  - where the largest accounts are moving
    S4 smart_vs_retail  z of log(top position L/S) - log(global account L/S)
  Labels: forward 4h and 24h log returns from the close at t (cross-sectional rank IC is
    unaffected by the market move).
  Statistics: mean hourly Spearman IC; 90% CI from a day-block bootstrap; null from permuting
    whole coins (each coin's signal series assigned to another coin), 500 reps.
  Incremental: IC after residualizing the signal's ranks on past-24h return and 24h realized vol.
  Economics (24h only): top-minus-bottom quintile in the IC's direction, daily rebalances at each
    hour of day (averaged), net of 0.12% per round trip on the replaced share of both legs.
  Verdict "worth archiving the relay's LSR forward" only if some signal has, at a horizon:
    |IC| >= 0.02, bootstrap CI excluding 0, permutation p < 0.05/8 (4 signals x 2 horizons),
    an incremental IC of the same sign with CI excluding 0, and (24h) a positive net spread.

    python scripts/lsr_positioning_study.py            # fetch (cached) and evaluate
    python scripts/lsr_positioning_study.py --offline  # cached data only
    python scripts/lsr_positioning_study.py --offline --robustness   # post-hoc checks, not the verdict
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.research.xs_reversal_tracker import perp_symbols  # noqa: E402
from scripts.agent_call_edge import day_bootstrap_ci  # noqa: E402

FAPI = "https://fapi.binance.com"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "lsr_positioning"
UNIVERSE = PROJECT_ROOT / "data" / "research" / "xs_reversal" / "universe.json"
RATIO_ENDPOINTS = {
    "top_position": "/futures/data/topLongShortPositionRatio",
    "global_account": "/futures/data/globalLongShortAccountRatio",
}
HOUR = pd.Timedelta(hours=1)
Z_WINDOW_H = 72
MIN_COINS = 30
HORIZONS = {"4h": 4, "24h": 24}
SIGNALS = ("retail_z", "top_z", "top_flow_24h", "smart_vs_retail")
COST_ROUND_TRIP = 0.0012
IC_MIN = 0.02
N_PERM = 500
ALPHA = 0.05 / (len(SIGNALS) * len(HORIZONS))


# ----------------------------------------------------------------------------------------- fetch
async def _get_json(client, path: str, params: Dict[str, Any]) -> Any:
    for attempt in range(4):
        try:
            resp = await client.get(FAPI + path, params=params)
            if resp.status_code == 429:
                await asyncio.sleep(10 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception:  # noqa: BLE001 - the proxy drops connections now and then
            if attempt == 3:
                raise
            await asyncio.sleep(2 * (attempt + 1))
    return None


async def _ratio_history(client, path: str, symbol: str, start_ms: int, end_ms: int,
                         rows: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Ratio points in [start_ms, end_ms], paging BACKWARD with endTime.

    Given startTime and endTime over more than `limit` points, the endpoint returns the LATEST 500,
    so paging forward from startTime silently stopped at ~21 of the 30 days (fixed 2026-10-09).
    Existing `rows` are kept and only the older gap is fetched.
    """
    by_ts = {int(r["timestamp"]): r for r in rows or []}
    end = (min(by_ts) - 1) if by_ts else end_ms
    while end > start_ms:
        page = await _get_json(client, path, {"symbol": symbol, "period": "1h", "limit": 500, "endTime": end})
        if not page:
            break
        stamps = [int(r["timestamp"]) for r in page]
        new = [s for s in stamps if s not in by_ts]
        by_ts.update({int(r["timestamp"]): r for r in page})
        if len(page) < 500 or not new or min(stamps) <= start_ms:
            break
        end = min(stamps) - 1
    return [by_ts[t] for t in sorted(by_ts) if start_ms <= t <= end_ms]


async def _fetch_symbol(client, symbol: str, start_ms: int, end_ms: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {"symbol": symbol}
    for name, path in RATIO_ENDPOINTS.items():
        out[name] = await _ratio_history(client, path, symbol, start_ms, end_ms)
    out["klines"] = await _get_json(client, "/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 1000,
                                                                 "startTime": start_ms - 2 * 86_400_000})
    return out


async def fetch_all(bases: List[str], out_dir: Path = OUT_DIR, refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)  # fapi.binance.com needs the proxy from here
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_ms = int(now.timestamp() * 1000)
    start_ms = int((now - timedelta(days=30) + timedelta(hours=1)).timestamp() * 1000)
    data: Dict[str, Dict[str, Any]] = {}
    async with httpx.AsyncClient(timeout=30, trust_env=True) as client:
        mapping_file = out_dir / "perp_symbols.json"
        if mapping_file.exists() and not refresh:
            perps = json.loads(mapping_file.read_text(encoding="utf-8"))
        else:
            perps = perp_symbols(await _get_json(client, "/fapi/v1/exchangeInfo", {}))
            mapping_file.write_text(json.dumps(perps), encoding="utf-8")
        sem = asyncio.Semaphore(4)

        async def one(base: str) -> None:
            symbol = perps.get(base)
            if not symbol:
                return
            cache = raw_dir / f"{symbol}.json"
            if cache.exists() and not refresh:
                payload = json.loads(cache.read_text(encoding="utf-8"))
                stale = [n for n in RATIO_ENDPOINTS
                         if not payload.get(n) or min(int(r["timestamp"]) for r in payload[n]) > start_ms + 6 * 3_600_000]
                if stale:  # caches written before the paging fix lack the oldest ~9 days
                    async with sem:
                        try:
                            for n in stale:
                                payload[n] = await _ratio_history(client, RATIO_ENDPOINTS[n], symbol, start_ms,
                                                                  end_ms, rows=payload.get(n))
                        except Exception as exc:  # noqa: BLE001
                            print(f"  backfill skipped {symbol}: {type(exc).__name__}")
                    cache.write_text(json.dumps(payload), encoding="utf-8")
                data[symbol] = payload
                return
            async with sem:
                try:
                    payload = await _fetch_symbol(client, symbol, start_ms, end_ms)
                except Exception as exc:  # noqa: BLE001 - one coin never stops the study
                    print(f"  skip {symbol}: {type(exc).__name__}")
                    return
            cache.write_text(json.dumps(payload), encoding="utf-8")
            data[symbol] = payload

        await asyncio.gather(*(one(base) for base in bases))
    return data


def load_cached(out_dir: Path = OUT_DIR) -> Dict[str, Dict[str, Any]]:
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted((out_dir / "raw").glob("*.json"))}


# ----------------------------------------------------------------------------------------- panel
def build_panel(data: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """Hour-indexed frames (columns = symbols): close at each hour, and the two raw ratios by stamp."""
    closes, top, glob = {}, {}, {}
    for symbol, payload in data.items():
        k = payload.get("klines") or []
        if k:
            # bar opened at T closes at T+1h: that close is the price "at" T+1h
            closes[symbol] = pd.Series([float(r[4]) for r in k],
                                       index=pd.to_datetime([int(r[0]) for r in k], unit="ms", utc=True) + HOUR)
        for name, store in (("top_position", top), ("global_account", glob)):
            rows = payload.get(name) or []
            if rows:
                store[symbol] = pd.Series([float(r["longShortRatio"]) for r in rows],
                                          index=pd.to_datetime([int(r["timestamp"]) for r in rows], unit="ms", utc=True))
    frames = {"close": pd.DataFrame(closes), "top": pd.DataFrame(top), "global": pd.DataFrame(glob)}
    for name in frames:
        frame = frames[name]
        # sorted columns: the permutation null must not depend on download order
        frames[name] = frame[~frame.index.duplicated(keep="last")].sort_index().sort_index(axis=1).astype(float)
    return frames


def _own_z(frame: pd.DataFrame, window: int = Z_WINDOW_H) -> pd.DataFrame:
    mean = frame.rolling(window, min_periods=window // 2).mean()
    std = frame.rolling(window, min_periods=window // 2).std()
    return (frame - mean) / std.where(std > 1e-9)


def build_signals(frames: Dict[str, pd.DataFrame], lag_hours: int = 1) -> Dict[str, pd.DataFrame]:
    """Signals, labels and controls on one hourly grid; ratios lagged `lag_hours` (publication)."""
    grid = pd.date_range(frames["close"].index.min(), frames["close"].index.max(), freq="1h")
    close = frames["close"].reindex(grid)
    cols = [c for c in close.columns if c in frames["top"].columns and c in frames["global"].columns]
    close = close[cols]
    # the point stamped T is used from T + lag on (pre-registered: 1h)
    log_top = np.log(frames["top"][cols].where(frames["top"][cols] > 0)).reindex(grid).shift(lag_hours)
    log_glob = np.log(frames["global"][cols].where(frames["global"][cols] > 0)).reindex(grid).shift(lag_hours)
    log_close = np.log(close)
    hourly = log_close.diff()
    out = {
        "retail_z": _own_z(log_glob),
        "top_z": _own_z(log_top),
        "top_flow_24h": log_top - log_top.shift(24),
        "smart_vs_retail": _own_z(log_top - log_glob),
        "past_24h": log_close - log_close.shift(24),
        "past_72h": log_close - log_close.shift(72),
        "past_168h": log_close - log_close.shift(168),
        "vol_24h": hourly.rolling(24, min_periods=20).std(),
    }
    for name, h in HORIZONS.items():
        out[f"fwd_{name}"] = log_close.shift(-h) - log_close
    return out


# ------------------------------------------------------------------------------------ statistics
def row_ic(signal: pd.DataFrame, label: pd.DataFrame, min_coins: int = MIN_COINS) -> pd.Series:
    """Spearman IC per row over the coins where both are present (elementwise ops only: no BLAS)."""
    mask = signal.notna() & label.notna()
    a = signal.where(mask).rank(axis=1)
    b = label.where(mask).rank(axis=1)
    a = a.sub(a.mean(axis=1), axis=0)
    b = b.sub(b.mean(axis=1), axis=0)
    den = np.sqrt((a * a).sum(axis=1) * (b * b).sum(axis=1))
    ic = (a * b).sum(axis=1) / den.where(den > 0)
    return ic.where(mask.sum(axis=1) >= min_coins)


def residualize(target: pd.DataFrame, controls: List[pd.DataFrame]) -> pd.DataFrame:
    """Per-row ranks of `target` with the ranks of `controls` regressed out (Gram-Schmidt)."""
    mask = target.notna()
    for control in controls:
        mask &= control.notna()
    y = target.where(mask).rank(axis=1)
    y = y.sub(y.mean(axis=1), axis=0)
    basis: List[pd.DataFrame] = []
    for control in controls:
        x = control.where(mask).rank(axis=1)
        x = x.sub(x.mean(axis=1), axis=0)
        for b in basis:
            x = x - b.mul((x * b).sum(axis=1) / (b * b).sum(axis=1), axis=0)
        basis.append(x)
        y = y - x.mul((y * x).sum(axis=1) / (x * x).sum(axis=1).where(lambda s: s > 0), axis=0)
    return y.where(mask)


def coin_permutation(signal: pd.DataFrame, label: pd.DataFrame, reps: int = N_PERM, seed: int = 0) -> Dict[str, Any]:
    """Mean IC on the coins complete over the window, and its null with whole coins permuted."""
    rows = signal.index[signal.notna().sum(axis=1) >= MIN_COINS].intersection(label.dropna(how="all").index)
    sig, lab = signal.loc[rows], label.loc[rows]
    cols = [c for c in sig.columns if sig[c].notna().all() and lab[c].notna().all()]
    if len(cols) < MIN_COINS or len(rows) == 0:
        return {"coins": len(cols), "ic": float("nan"), "p": float("nan")}
    s = sig[cols].rank(axis=1).to_numpy(float)
    l_ = lab[cols].rank(axis=1).to_numpy(float)
    s = s - s.mean(axis=1, keepdims=True)
    l_ = l_ - l_.mean(axis=1, keepdims=True)
    s = s / np.sqrt((s * s).sum(axis=1, keepdims=True))
    l_ = l_ / np.sqrt((l_ * l_).sum(axis=1, keepdims=True))
    observed = float((s * l_).sum(axis=1).mean())
    rng = np.random.default_rng(seed)
    null = np.array([(s[:, rng.permutation(len(cols))] * l_).sum(axis=1).mean() for _ in range(reps)])
    p = float((np.sum(np.abs(null) >= abs(observed)) + 1) / (reps + 1))
    return {"coins": len(cols), "ic": observed, "p": p}


def quintile_spread(signal: pd.DataFrame, fwd: pd.DataFrame, direction: float,
                    cost_round_trip: float = COST_ROUND_TRIP) -> Dict[str, float]:
    """Daily top-minus-bottom quintile at each hour of day, averaged over the 24 start hours."""
    gross, net, turnover = [], [], []
    for hour in range(24):
        prev: Optional[Tuple[set, set]] = None
        for t in signal.index[signal.index.hour == hour]:
            s, f = signal.loc[t], fwd.loc[t]
            ok = s.notna() & f.notna()
            if ok.sum() < MIN_COINS:
                continue
            ranked = (s[ok] * direction).sort_values(kind="mergesort")
            k = len(ranked) // 5
            short, long_ = set(ranked.index[:k]), set(ranked.index[-k:])
            spread = float(f[list(long_)].mean() - f[list(short)].mean())
            turn = 1.0 if prev is None else 0.5 * (len(long_ - prev[0]) + len(short - prev[1])) / k
            gross.append(spread)
            net.append(spread - 2 * turn * cost_round_trip)
            turnover.append(turn)
            prev = (long_, short)
    if not gross:
        return {"days": 0, "gross": float("nan"), "net": float("nan"), "turnover": float("nan")}
    return {"days": len(gross) / 24, "gross": float(np.mean(gross)), "net": float(np.mean(net)),
            "turnover": float(np.mean(turnover))}


def evaluate(panel: Dict[str, pd.DataFrame], reps: int = N_PERM) -> List[Dict[str, Any]]:
    results = []
    controls = [panel["past_24h"], panel["vol_24h"]]
    for name in SIGNALS:
        signal = panel[name]
        for horizon in HORIZONS:
            label = panel[f"fwd_{horizon}"]
            ic = row_ic(signal, label).dropna()
            days = pd.Series(ic.index.date, index=ic.index)
            lo, hi = day_bootstrap_ci(ic, days) if len(ic) else (np.nan, np.nan)
            perm = coin_permutation(signal, label, reps=reps)
            inc = row_ic(residualize(signal, controls), label).dropna()
            ilo, ihi = day_bootstrap_ci(inc, pd.Series(inc.index.date, index=inc.index)) if len(inc) else (np.nan, np.nan)
            row = {"signal": name, "horizon": horizon, "hours": int(len(ic)), "days": int(days.nunique()),
                   "ic": float(ic.mean()) if len(ic) else np.nan, "ci": (lo, hi),
                   "perm_ic": perm["ic"], "perm_p": perm["p"], "perm_coins": perm["coins"],
                   "inc_ic": float(inc.mean()) if len(inc) else np.nan, "inc_ci": (ilo, ihi)}
            if horizon == "24h":
                row["spread"] = quintile_spread(signal, label, direction=1.0 if row["ic"] >= 0 else -1.0)
            stat = abs(row["ic"]) >= IC_MIN and (lo > 0 or hi < 0) and row["perm_p"] < ALPHA
            incr = np.sign(row["inc_ic"]) == np.sign(row["ic"]) and (ilo > 0 or ihi < 0)
            econ = horizon != "24h" or row["spread"]["net"] > 0
            row["passes"] = bool(stat and incr and econ)
            results.append(row)
    return results


def _fmt(x: float, digits: int = 4) -> str:
    return "  n/a " if x != x else f"{x:+.{digits}f}"


def report(results: List[Dict[str, Any]], coins: int) -> str:
    lines = [f"coins {coins} | pass rule: |IC|>={IC_MIN}, CI excl. 0, coin-permutation p<{ALPHA:.4f}, "
             f"incremental IC same sign with CI excl. 0, 24h net quintile spread > 0"]
    lines.append(f"{'signal':16s} {'h':4s} {'IC':>8s} {'90% CI':>20s} {'perm p':>7s} {'incr IC':>8s} {'incr CI':>20s} "
                 f"{'24h gross/net per day':>24s} pass")
    for r in results:
        spread = r.get("spread")
        sp = f"{_fmt(spread['gross'] * 100, 3)}%/{_fmt(spread['net'] * 100, 3)}%" if spread else ""
        lines.append(f"{r['signal']:16s} {r['horizon']:4s} {_fmt(r['ic'])} [{_fmt(r['ci'][0])}, {_fmt(r['ci'][1])}] "
                     f"{r['perm_p']:7.3f} {_fmt(r['inc_ic'])} [{_fmt(r['inc_ci'][0])}, {_fmt(r['inc_ci'][1])}] "
                     f"{sp:>24s} {'YES' if r['passes'] else 'no'}")
    verdict = ("worth archiving the relay's LSR forward" if any(r["passes"] for r in results)
               else "no positioning signal passes: do not collect the relay's LSR for direction")
    lines.append(f"verdict (pre-registered): {verdict}")
    return "\n".join(lines)


def robustness(frames: Dict[str, pd.DataFrame]) -> str:
    """Post-hoc checks on the signals that passed; reported next to, never instead of, the verdict."""
    def ic_line(label: str, signal: pd.DataFrame, target: pd.DataFrame) -> str:
        ic = row_ic(signal, target).dropna()
        lo, hi = day_bootstrap_ci(ic, pd.Series(ic.index.date, index=ic.index)) if len(ic) else (np.nan, np.nan)
        return f"  {label:58s} IC {_fmt(float(ic.mean()) if len(ic) else np.nan)} [{_fmt(lo)}, {_fmt(hi)}]"

    base = build_signals(frames, lag_hours=1)
    lag2 = build_signals(frames, lag_hours=2)
    lines = ["post-hoc robustness (24h horizon unless noted; not part of the pre-registered verdict):"]
    for name in ("retail_z", "smart_vs_retail"):
        label = base["fwd_24h"]
        lines.append(f"{name}:")
        lines.append(ic_line("pre-registered (lag 1h)", base[name], label))
        lines.append(ic_line("ratios lagged 2h", lag2[name], lag2["fwd_24h"]))
        lines.append(ic_line("residual on past 24h/72h/168h return + 24h vol",
                             residualize(base[name], [base["past_24h"], base["past_72h"], base["past_168h"], base["vol_24h"]]), label))
        mid = base[name].dropna(how="all").index
        mid = mid[len(mid) // 2] if len(mid) else None
        if mid is not None:
            lines.append(ic_line("first half of the window", base[name][base[name].index < mid], label[label.index < mid]))
            lines.append(ic_line("second half of the window", base[name][base[name].index >= mid], label[label.index >= mid]))
    lines.append("does the top-trader leg add anything beyond the free retail ratio?")
    lines.append(ic_line("smart_vs_retail residual on retail_z", residualize(base["smart_vs_retail"], [base["retail_z"]]), base["fwd_24h"]))
    lines.append(ic_line("top_z residual on retail_z", residualize(base["top_z"], [base["retail_z"]]), base["fwd_24h"]))
    lines.append(ic_line("smart_vs_retail residual on retail_z (4h)", residualize(base["smart_vs_retail"], [base["retail_z"]]), base["fwd_4h"]))
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--offline", action="store_true", help="use cached raw data only")
    parser.add_argument("--refresh", action="store_true", help="re-download even when cached")
    parser.add_argument("--reps", type=int, default=N_PERM)
    parser.add_argument("--robustness", action="store_true", help="also print the post-hoc checks")
    args = parser.parse_args(argv)

    bases = json.loads(UNIVERSE.read_text(encoding="utf-8"))["bases"]
    data = load_cached() if args.offline else asyncio.run(fetch_all(bases, refresh=args.refresh))
    frames = build_panel(data)
    panel = build_signals(frames)
    coins = int(panel["top_z"].notna().any().sum())
    results = evaluate(panel, reps=args.reps)
    text = report(results, coins)
    print(text)
    if args.robustness:
        print(robustness(frames))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    serializable = [{k: (list(v) if isinstance(v, tuple) else v) for k, v in r.items()} for r in results]
    (OUT_DIR / "results.json").write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                                                      "coins": coins, "results": serializable}, indent=1, default=float),
                                          encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
