"""Do Coinglass derivatives-positioning histories predict the next days' alt returns? (2026-10-11)

WHY THIS EXISTS:
The vip2 relay's Coinglass "standard" plan serves daily history back to 2024-01 for open interest,
OI-weighted funding, liquidations, taker volume and Binance long/short ratios on alt perps - far
beyond the 30 days Binance keeps (probed 2026-10-11). The user asked whether this data predicts the
next few days, instead of waiting months for a forward archive. The 10/9 study
(scripts/lsr_positioning_study.py) had 30 days only; its one near-miss, retail crowding, is
re-tested here on ~2.7 years of daily data.

PRE-REGISTERED before the first run (do not tune after seeing results):
  Universe: data/research/xs_reversal/universe.json (131 bases) on their Binance USDT perps. A coin
    counts on each day it has the signal and the label; days need >= 30 coins.
  Data: daily bars 2024-01-01 .. 2026-10-10 (UTC), one request per coin and dataset:
    Coinglass aggregated: open-interest close (USD); OI-weighted funding close; long and short
      liquidations (Binance+OKX+Bybit); taker buy and sell volume (Binance+OKX+Bybit).
    Coinglass Binance pair: global account long/short ratio; top-trader position long/short ratio.
    Binance perp 4h klines for prices.
  Timing: the bar stamped D covers day D and is used from D+1 00:00 UTC. Entry at the D+1 00:00
    open, exit at the D+1+h 00:00 open. Exception, found before any run: a finished daily
    long/short bar is the snapshot AT its stamp (BTC 10/6-10/10: daily = hourly value at 00:00 of
    the same day), so the bar stamped D+1 is the one that closes day D and is used for day D.
    L/S = long % / short % (two decimals of a percent) rather than the ratio field (rounded to
    0.01), also decided before any run.
  Signals (8), all known at D+1 00:00:
    liq_imbalance_1d    (long liq - short liq) / (long + short) on day D     [share of forced selling]
    liq_shock_z         z of log(1 + long + short liq) vs the coin's own prior 30 days
    oi_change_3d        log OI(D) / OI(D-3)
    funding_z           z of OI-weighted funding vs the coin's own prior 30 days
    retail_z            z of log(global account L/S) vs own prior 7 days   [10/9 near-miss, daily]
    top_z               z of log(top position L/S) vs own prior 7 days
    smart_vs_retail     z of log(top L/S) - log(global L/S) vs own prior 7 days
    taker_imbalance_3d  (buy - sell) / (buy + sell) over days D-2..D
  ("prior" windows end at D-1, so the z compares day D with what came before it.)
  Horizons: 1, 3 and 7 days, log returns.
  Statistics (as on 10/9): mean daily Spearman IC; 90% CI from a week-block bootstrap; null from
    permuting whole coins (each coin's signal series reassigned to another coin, gaps kept),
    1000 reps; incremental IC after residualizing the signal's ranks on the past 1d and 7d returns
    and 14d volatility; top-minus-bottom quintile in the IC's direction, rebalanced every h days
    (averaged over the h start days), net of 0.12% per round trip on the replaced share of both legs.
  Pass (all): |IC| >= 0.02 with the CI excluding 0; permutation p < 0.05/24; incremental IC of the
    same sign with its CI excluding 0; net spread > 0; the same IC sign in both halves (before /
    from 2025-05-22).
  Confirmatory replication (single test): retail_z at 1d has a negative IC with permutation p < 0.05.
  Robustness, reported next to the verdict, never instead of it: entries 4h later (D+1 04:00 open
    to D+1+h 04:00); the last 12 months alone.

    python scripts/coinglass_positioning_study.py --fetch     # download (resumable, ~2.5h at 5 req/min)
    python scripts/coinglass_positioning_study.py             # evaluate the cached data
    python scripts/coinglass_positioning_study.py --timing    # which hour a daily L/S bar describes
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

from scripts.agent_call_edge import day_bootstrap_ci  # noqa: E402
from scripts.lsr_positioning_study import residualize, row_ic  # noqa: E402

OUT_DIR = PROJECT_ROOT / "data" / "research" / "coinglass_positioning"
UNIVERSE = PROJECT_ROOT / "data" / "research" / "xs_reversal" / "universe.json"
PERP_SYMBOLS = PROJECT_ROOT / "data" / "research" / "lsr_positioning" / "perp_symbols.json"
FAPI = "https://fapi.binance.com"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 10, 11, tzinfo=timezone.utc)  # exclusive: the last full day is 10-10
SPLIT = pd.Timestamp("2025-05-22", tz="UTC")
DAY = pd.Timedelta(days=1)
EXCHANGES = "Binance,OKX,Bybit"
# dataset -> (path, base or pair symbol, fields kept)
DATASETS: Dict[str, Tuple[str, str, Tuple[str, ...]]] = {
    "oi": ("/api/futures/open-interest/aggregated-history", "base", ("close",)),
    "funding": ("/api/futures/funding-rate/oi-weight-history", "base", ("close",)),
    "liquidation": ("/api/futures/liquidation/aggregated-history", "base",
                    ("aggregated_long_liquidation_usd", "aggregated_short_liquidation_usd")),
    "taker": ("/api/futures/aggregated-taker-buy-sell-volume/history", "base",
              ("aggregated_buy_volume_usd", "aggregated_sell_volume_usd")),
    "global_ratio": ("/api/futures/global-long-short-account-ratio/history", "pair", ("global_account_long_short_ratio",)),
    "top_ratio": ("/api/futures/top-long-short-position-ratio/history", "pair", ("top_position_long_short_ratio",)),
}
SNAPSHOT_AT_STAMP = {"global_ratio", "top_ratio"}  # the D+1 00:00 snapshot is the end of day D
SIGNALS = ("liq_imbalance_1d", "liq_shock_z", "oi_change_3d", "funding_z",
           "retail_z", "top_z", "smart_vs_retail", "taker_imbalance_3d")
HORIZONS = (1, 3, 7)
MIN_COINS = 30
COST_ROUND_TRIP = 0.0012
IC_MIN = 0.02
N_PERM = 1000
ALPHA = 0.05 / (len(SIGNALS) * len(HORIZONS))
REQUEST_SPACING_SEC = 12.0  # ~5/min: the relay allows 11 per rolling minute, shared with the collector


# ----------------------------------------------------------------------------------------- fetch
def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _rows(payload: Any) -> List[Dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else payload
    return [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []


async def _coinglass(client, path: str, params: Dict[str, Any], last_call: List[float]) -> List[Dict[str, Any]]:
    """One relay request through the project's budget ledger, paced and retried on 429."""
    from core.data.coinglass_client import CoinglassBudgetExceeded, CoinglassError

    for attempt in range(8):
        wait = REQUEST_SPACING_SEC - (time.monotonic() - last_call[0])
        if wait > 0:
            await asyncio.sleep(wait)
        last_call[0] = time.monotonic()
        try:
            resp = await client.request_json(path, params=params, manual=False)
        except CoinglassBudgetExceeded:
            await asyncio.sleep(15)
            continue
        except CoinglassError as exc:
            if "429" in str(exc):
                await asyncio.sleep(75)  # the client also pauses itself for 60s
                continue
            raise
        payload = resp.get("payload")
        code = str(payload.get("code")) if isinstance(payload, dict) else "0"
        if code not in {"0", "None"}:
            raise RuntimeError(f"business code {code}: {str(payload.get('msg'))[:80]}")
        return _rows(payload)
    raise RuntimeError("relay kept refusing")


PAGE_CAP = 1000  # a page this long may have been cut short; shorter pages are complete
DAY_MS = 86_400_000


async def _history(client, name: str, base: str, pair: str, last_call: List[float]) -> List[Dict[str, Any]]:
    """All daily rows in [START, END). One request normally covers the range; a page that hits the
    cap is followed by requests for whichever end it left out (either end may be the one cut)."""
    path, kind, _ = DATASETS[name]
    params: Dict[str, Any] = {"interval": "1d", "limit": 4500}
    if kind == "base":
        params["symbol"] = base
        if name in {"liquidation", "taker"}:
            params["exchange_list"] = EXCHANGES
    else:
        params.update(symbol=pair, exchange="Binance")
    by_ts: Dict[int, Dict[str, Any]] = {}
    todo = [(_ms(START), _ms(END) - 1)]
    for _ in range(6):
        if not todo:
            break
        lo, hi = todo.pop()
        page = await _coinglass(client, path, {**params, "start_time": lo, "end_time": hi}, last_call)
        stamps = [int(r["time"]) for r in page if r.get("time") is not None]
        by_ts.update({int(r["time"]): r for r in page if r.get("time") is not None})
        if len(stamps) >= PAGE_CAP:
            if min(stamps) > lo + 2 * DAY_MS:
                todo.append((lo, min(stamps) - 1))
            if max(stamps) < hi - 2 * DAY_MS:
                todo.append((max(stamps) + 1, hi))
    return [by_ts[t] for t in sorted(by_ts) if _ms(START) <= t < _ms(END)]


async def _klines(http, pair: str) -> List[List[Any]]:
    out: List[List[Any]] = []
    start = _ms(START) - 40 * 86_400_000  # warm-up for the 30-day windows
    while start < _ms(END):
        for attempt in range(4):
            try:
                resp = await http.get(f"{FAPI}/fapi/v1/klines",
                                      params={"symbol": pair, "interval": "4h", "startTime": start, "limit": 1500})
                resp.raise_for_status()
                page = resp.json()
                break
            except Exception:  # noqa: BLE001 - the proxy drops connections now and then
                if attempt == 3:
                    raise
                await asyncio.sleep(3 * (attempt + 1))
        if not page:
            break
        out += page
        start = int(page[-1][0]) + 4 * 3_600_000
        if len(page) < 1500:
            break
    return [k for k in out if int(k[0]) < _ms(END) + 8 * 86_400_000]


async def fetch_all(bases: List[str], out_dir: Path = OUT_DIR) -> None:
    """Resumable download: each coin's file keeps whatever datasets are already complete."""
    import httpx

    from config.settings import settings
    from core.data.coinglass_client import CoinglassClient
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)  # fapi.binance.com needs the proxy; the relay is on the bypass list
    raw = out_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    perps: Dict[str, str] = json.loads(PERP_SYMBOLS.read_text(encoding="utf-8"))
    last_call = [0.0]
    started = time.monotonic()
    done_requests = 0
    async with CoinglassClient() as client, httpx.AsyncClient(timeout=30, trust_env=True) as http:
        for i, base in enumerate(bases):
            pair = perps.get(base)
            if not pair:
                print(f"[{i + 1}/{len(bases)}] {base}: no live perp, skipped", flush=True)
                continue
            cg_base = base[4:] if base.startswith("1000") and len(base) > 4 else base
            path = raw / f"{pair}.json"
            payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"pair": pair, "base": cg_base}
            for name in DATASETS:
                if name in payload:
                    continue
                try:
                    payload[name] = await _history(client, name, cg_base, pair, last_call)
                    done_requests += 1
                except Exception as exc:  # noqa: BLE001 - recorded, retried on the next run
                    payload.setdefault("errors", {})[name] = str(exc)[:160]
                    print(f"[{i + 1}/{len(bases)}] {pair} {name}: {exc}", flush=True)
                path.write_text(json.dumps(payload), encoding="utf-8")
            if "klines_4h" not in payload:
                try:
                    payload["klines_4h"] = await _klines(http, pair)
                except Exception as exc:  # noqa: BLE001
                    payload.setdefault("errors", {})["klines_4h"] = str(exc)[:160]
                path.write_text(json.dumps(payload), encoding="utf-8")
            rate = done_requests / max(1.0, (time.monotonic() - started) / 60)
            print(f"[{i + 1}/{len(bases)}] {pair}: " + ", ".join(
                f"{n}={len(payload.get(n) or [])}" for n in (*DATASETS, "klines_4h")) + f" | {rate:.1f} req/min", flush=True)


def load_cached(out_dir: Path = OUT_DIR) -> Dict[str, Dict[str, Any]]:
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted((out_dir / "raw").glob("*.json"))}


# ----------------------------------------------------------------------------------------- panel
# ratio field -> (long %, short %) fields carrying more precision than the ratio's two decimals
RATIO_FROM_PERCENTS = {
    "global_account_long_short_ratio": ("global_account_long_percent", "global_account_short_percent"),
    "top_position_long_short_ratio": ("top_position_long_percent", "top_position_short_percent"),
}


def _value(row: Dict[str, Any], field: str) -> float:
    if field in RATIO_FROM_PERCENTS:
        long_pct, short_pct = (pd.to_numeric(row.get(f), errors="coerce") for f in RATIO_FROM_PERCENTS[field])
        if long_pct == long_pct and short_pct == short_pct and short_pct > 0:
            return float(long_pct) / float(short_pct)
    return float(pd.to_numeric(row.get(field), errors="coerce"))


def _series(rows: List[Dict[str, Any]], field: str) -> pd.Series:
    values = {pd.Timestamp(int(r["time"]), unit="ms", tz="UTC"): _value(r, field)
              for r in rows or [] if r.get("time") is not None}
    return pd.Series(values, dtype=float).sort_index()


def build_panel(data: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """Day-indexed frames (columns = perp symbols). Coinglass values sit on their bar's day D;
    prices are the 4h opens at 00:00 and 04:00 of each day."""
    fields = {"oi": ("oi", "close"), "funding": ("funding", "close"),
              "liq_long": ("liquidation", "aggregated_long_liquidation_usd"),
              "liq_short": ("liquidation", "aggregated_short_liquidation_usd"),
              "buy": ("taker", "aggregated_buy_volume_usd"), "sell": ("taker", "aggregated_sell_volume_usd"),
              "global": ("global_ratio", "global_account_long_short_ratio"),
              "top": ("top_ratio", "top_position_long_short_ratio")}
    cols: Dict[str, Dict[str, pd.Series]] = {k: {} for k in (*fields, "open_00", "open_04")}
    for symbol, payload in data.items():
        for key, (dataset, field) in fields.items():
            if payload.get(dataset):
                series = _series(payload[dataset], field)
                if dataset in SNAPSHOT_AT_STAMP:
                    series.index = series.index - DAY  # the snapshot at D+1 00:00 describes the end of day D
                cols[key][symbol] = series
        k = payload.get("klines_4h") or []
        if k:
            opens = pd.Series([float(r[1]) for r in k], index=pd.to_datetime([int(r[0]) for r in k], unit="ms", utc=True))
            opens = opens[~opens.index.duplicated(keep="last")]
            cols["open_00"][symbol] = opens[opens.index.hour == 0]
            cols["open_04"][symbol] = opens[opens.index.hour == 4].set_axis(opens[opens.index.hour == 4].index.floor("D"))
    grid = pd.date_range(START - pd.Timedelta(days=40), END + pd.Timedelta(days=8), freq="1D", tz="UTC")
    frames = {}
    for key, store in cols.items():
        frame = pd.DataFrame(store).reindex(grid) if store else pd.DataFrame(index=grid)
        # sorted columns: the permutation null must not depend on download order
        frames[key] = frame.sort_index(axis=1).astype(float)
    return frames


def _prior_z(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    """z of day D against days D-window .. D-1."""
    prior = frame.shift(1).rolling(window, min_periods=max(5, window * 2 // 3))
    std = prior.std()
    return (frame - prior.mean()) / std.where(std > 1e-12)


def build_signals(frames: Dict[str, pd.DataFrame], entry: str = "open_00") -> Dict[str, pd.DataFrame]:
    """Signals on day D (bar D's data) and labels from the D+1 entry; one daily grid."""
    log_glob = np.log(frames["global"].where(frames["global"] > 0))
    log_top = np.log(frames["top"].where(frames["top"] > 0))
    liq_total = frames["liq_long"] + frames["liq_short"]
    buy3, sell3 = frames["buy"].rolling(3).sum(), frames["sell"].rolling(3).sum()
    oi = frames["oi"].where(frames["oi"] > 0)
    out = {
        "liq_imbalance_1d": ((frames["liq_long"] - frames["liq_short"]) / liq_total.where(liq_total > 0)),
        "liq_shock_z": _prior_z(np.log1p(liq_total.clip(lower=0)), 30),
        "oi_change_3d": np.log(oi) - np.log(oi.shift(3)),
        "funding_z": _prior_z(frames["funding"], 30),
        "retail_z": _prior_z(log_glob, 7),
        "top_z": _prior_z(log_top, 7),
        "smart_vs_retail": _prior_z(log_top - log_glob, 7),
        "taker_imbalance_3d": (buy3 - sell3) / (buy3 + sell3).where((buy3 + sell3) > 0),
    }
    # prices: p0[D] = entry price on day D+1 (the first tradable moment after bar D closes)
    price = np.log(frames[entry].where(frames[entry] > 0))
    p0 = price.shift(-1)
    daily = frames["open_00"].where(frames["open_00"] > 0)
    log_00 = np.log(daily)
    out["past_1d"] = log_00.shift(-1) - log_00            # day D's move, known at D+1 00:00
    out["past_7d"] = log_00.shift(-1) - log_00.shift(6)
    out["vol_14d"] = (log_00.shift(-1) - log_00).rolling(14, min_periods=10).std()
    for h in HORIZONS:
        out[f"fwd_{h}d"] = price.shift(-1 - h) - p0
    window = (out["fwd_1d"].index >= START) & (out["fwd_1d"].index < END)
    return {k: v.loc[window] for k, v in out.items()}


# ------------------------------------------------------------------------------------ statistics
def week_keys(index: pd.Index) -> pd.Series:
    iso = pd.DatetimeIndex(index).isocalendar()
    return pd.Series((iso["year"] * 100 + iso["week"]).to_numpy(), index=index)


def coin_permutation(signal: pd.DataFrame, label: pd.DataFrame, reps: int = N_PERM, seed: int = 0) -> Dict[str, Any]:
    """Mean daily IC and its null with whole coins permuted (each coin's signal series, gaps
    included, assigned to another coin). Persistent coin traits survive the shuffle; the pairing
    of a coin's signal with its own future does not."""
    observed = float(row_ic(signal, label).mean())
    rng = np.random.default_rng(seed)
    cols = list(signal.columns)
    null = []
    for _ in range(reps):
        shuffled = signal.copy()
        shuffled.columns = [cols[i] for i in rng.permutation(len(cols))]
        null.append(float(row_ic(shuffled[cols], label).mean()))
    null_arr = np.array([v for v in null if v == v])
    p = float((np.sum(np.abs(null_arr) >= abs(observed)) + 1) / (len(null_arr) + 1)) if len(null_arr) else float("nan")
    return {"ic": observed, "p": p, "null_sd": float(null_arr.std()) if len(null_arr) else float("nan")}


def quintile_spread(signal: pd.DataFrame, fwd: pd.DataFrame, direction: float, horizon: int,
                    cost_round_trip: float = COST_ROUND_TRIP) -> Dict[str, float]:
    """Top-minus-bottom quintile held h days, rebalanced every h days, averaged over the h offsets."""
    gross, net, turnover = [], [], []
    days = list(signal.index)
    for offset in range(horizon):
        prev: Optional[Tuple[set, set]] = None
        for t in days[offset::horizon]:
            s, f = signal.loc[t], fwd.loc[t]
            ok = s.notna() & f.notna()
            if ok.sum() < MIN_COINS:
                prev = None
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
        return {"periods": 0, "gross_per_day": float("nan"), "net_per_day": float("nan"), "turnover": float("nan")}
    return {"periods": len(gross) / horizon, "gross_per_day": float(np.mean(gross)) / horizon,
            "net_per_day": float(np.mean(net)) / horizon, "turnover": float(np.mean(turnover))}


def _ic_ci(signal: pd.DataFrame, label: pd.DataFrame) -> Tuple[float, Tuple[float, float], int]:
    ic = row_ic(signal, label, MIN_COINS).dropna()
    if not len(ic):
        return float("nan"), (float("nan"), float("nan")), 0
    return float(ic.mean()), day_bootstrap_ci(ic, week_keys(ic.index)), int(len(ic))


def evaluate(panel: Dict[str, pd.DataFrame], reps: int = N_PERM) -> List[Dict[str, Any]]:
    results = []
    controls = [panel["past_1d"], panel["past_7d"], panel["vol_14d"]]
    for name in SIGNALS:
        signal = panel[name]
        residual = residualize(signal, controls)
        for h in HORIZONS:
            label = panel[f"fwd_{h}d"]
            ic, ci, n_days = _ic_ci(signal, label)
            perm = coin_permutation(signal, label, reps=reps)
            inc, inc_ci, _ = _ic_ci(residual, label)
            first, _, _ = _ic_ci(signal[signal.index < SPLIT], label[label.index < SPLIT])
            second, _, _ = _ic_ci(signal[signal.index >= SPLIT], label[label.index >= SPLIT])
            spread = quintile_spread(signal, label, direction=1.0 if ic >= 0 else -1.0, horizon=h)
            stat = abs(ic) >= IC_MIN and (ci[0] > 0 or ci[1] < 0) and perm["p"] < ALPHA
            incr = np.sign(inc) == np.sign(ic) and (inc_ci[0] > 0 or inc_ci[1] < 0)
            stable = np.sign(first) == np.sign(ic) and np.sign(second) == np.sign(ic)
            results.append({"signal": name, "horizon": h, "days": n_days, "ic": ic, "ci": ci,
                            "perm_p": perm["p"], "null_sd": perm["null_sd"], "inc_ic": inc, "inc_ci": inc_ci,
                            "first_half": first, "second_half": second, "spread": spread,
                            "passes": bool(stat and incr and spread["net_per_day"] > 0 and stable)})
    return results


def robustness(frames: Dict[str, pd.DataFrame], results: List[Dict[str, Any]]) -> List[str]:
    """Delayed entries and the last 12 months, for every signal/horizon with a CI excluding 0."""
    late = build_signals(frames, entry="open_04")
    base = build_signals(frames)
    recent = base["fwd_1d"].index >= base["fwd_1d"].index.max() - pd.Timedelta(days=365)
    lines = ["robustness (reported, not the verdict): entries 4h later | last 12 months"]
    for r in results:
        if not (r["ci"][0] > 0 or r["ci"][1] < 0):
            continue
        name, h = r["signal"], r["horizon"]
        ic_late, ci_late, _ = _ic_ci(late[name], late[f"fwd_{h}d"])
        ic_recent, ci_recent, _ = _ic_ci(base[name][recent], base[f"fwd_{h}d"][recent])
        lines.append(f"  {name:18s} {h}d  4h later {ic_late:+.4f} [{ci_late[0]:+.4f}, {ci_late[1]:+.4f}]"
                     f" | last 12m {ic_recent:+.4f} [{ci_recent[0]:+.4f}, {ci_recent[1]:+.4f}]")
    if len(lines) == 1:
        lines.append("  no signal has a CI excluding 0")
    return lines


def report(results: List[Dict[str, Any]], coins: int) -> str:
    def f(x: float, d: int = 4) -> str:
        return "   n/a" if x != x else f"{x:+.{d}f}"

    lines = [f"coins {coins} | pass: |IC|>={IC_MIN}, CI excl. 0, permutation p<{ALPHA:.4f}, incremental IC same "
             f"sign with CI excl. 0, net quintile spread > 0, same sign in both halves"]
    lines.append(f"{'signal':18s} {'h':>2s} {'days':>5s} {'IC':>8s} {'90% CI':>19s} {'perm p':>7s} {'incr IC':>8s} "
                 f"{'incr CI':>19s} {'1st/2nd half':>16s} {'net/day':>8s} pass")
    for r in results:
        s = r["spread"]
        lines.append(f"{r['signal']:18s} {r['horizon']:>2d} {r['days']:>5d} {f(r['ic'])} [{f(r['ci'][0])}, {f(r['ci'][1])}] "
                     f"{r['perm_p']:7.4f} {f(r['inc_ic'])} [{f(r['inc_ci'][0])}, {f(r['inc_ci'][1])}] "
                     f"{f(r['first_half'], 3)}/{f(r['second_half'], 3)} {f(s['net_per_day'] * 100, 3)}% "
                     f"{'YES' if r['passes'] else 'no'}")
    replication = next(r for r in results if r["signal"] == "retail_z" and r["horizon"] == 1)
    replicated = replication["ic"] < 0 and replication["perm_p"] < 0.05
    lines.append(f"replication (retail_z 1d, negative IC, p<0.05): {'REPLICATED' if replicated else 'not replicated'}"
                 f" (IC {f(replication['ic'])}, p {replication['perm_p']:.4f})")
    passed = [f"{r['signal']} {r['horizon']}d" for r in results if r["passes"]]
    lines.append("verdict (pre-registered): " + (f"passes: {', '.join(passed)}" if passed else "no signal passes"))
    return "\n".join(lines)


async def timing_check(pair: str = "BTCUSDT") -> None:
    """Which moment does a daily L/S bar describe? Print it next to the hourly bars of its day."""
    from core.data.coinglass_client import CoinglassClient

    last_call = [0.0]
    path = DATASETS["global_ratio"][0]
    async with CoinglassClient() as client:
        hourly = await _coinglass(client, path, {"exchange": "Binance", "symbol": pair, "interval": "1h", "limit": 240}, last_call)
        daily = await _coinglass(client, path, {"exchange": "Binance", "symbol": pair, "interval": "1d", "limit": 10}, last_call)
    by_hour = {int(r["time"]): r["global_account_long_short_ratio"] for r in hourly}
    for row in daily[-7:]:
        t = int(row["time"])
        print(pd.Timestamp(t, unit="ms", tz="UTC").date(), "daily", row["global_account_long_short_ratio"],
              "| hourly at 00:00", by_hour.get(t), "12:00", by_hour.get(t + 12 * 3_600_000),
              "23:00", by_hour.get(t + 23 * 3_600_000), "next 00:00", by_hour.get(t + 24 * 3_600_000))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fetch", action="store_true", help="download missing data (resumable)")
    parser.add_argument("--timing", action="store_true", help="check which hour a daily L/S bar describes")
    parser.add_argument("--reps", type=int, default=N_PERM)
    args = parser.parse_args(argv)
    if args.timing:
        asyncio.run(timing_check())
        return 0
    bases = json.loads(UNIVERSE.read_text(encoding="utf-8"))["bases"]
    if args.fetch:
        asyncio.run(fetch_all(bases))
        return 0
    frames = build_panel(load_cached())
    panel = build_signals(frames)
    coins = int(panel["fwd_1d"].notna().any().sum())
    results = evaluate(panel, reps=args.reps)
    text = report(results, coins) + "\n" + "\n".join(robustness(frames, results))
    print(text)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "results.json").write_text(json.dumps(
        {"generated_at": datetime.now(timezone.utc).isoformat(), "coins": coins, "report": text,
         "results": [{k: (list(v) if isinstance(v, tuple) else v) for k, v in r.items()} for r in results]},
        indent=1, default=float), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
