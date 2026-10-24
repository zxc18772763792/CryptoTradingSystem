"""Study 2 (replay): run the radar's own scorer (build_altcoin_rows) point-in-time on
local 4h perp klines and store per-coin scores for later forward-return tests.

Degraded-mode replay: no derivatives / community / whale / snapshot inputs -- exactly
what the live page runs on whenever the CoinGlass snapshot is unavailable (which is
most of the time, see the page audit). Cross-sectional percentiles use the whole
replay universe at each timestamp.
"""
from __future__ import annotations
import json, sys, time, warnings
from pathlib import Path
import pandas as pd

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
OUT = Path(sys.argv[1])
START = pd.Timestamp(sys.argv[2]) if len(sys.argv) > 2 else pd.Timestamp("2026-02-01")
END_TS = pd.Timestamp(sys.argv[3]) if len(sys.argv) > 3 else pd.Timestamp("2026-10-13")
STEP_H = int(sys.argv[4]) if len(sys.argv) > 4 else 24
MAXN = int(sys.argv[5]) if len(sys.argv) > 5 else 10**9

import core.research.altcoin_radar as ar  # noqa: E402

# silence in-memory event/rank history side effects
for name in ("record_ignition_cross_up", "record_rank_jump_event", "record_crowding_spike", "record_narrative_heat_spike"):
    setattr(ar, name, lambda *a, **k: None)
ar.bulk_update_ranks = lambda *a, **k: None
ar.get_rank_history = lambda *a, **k: []
ar.compute_rank_jump_score = lambda *a, **k: 0.0

lst = json.load(open(ROOT / "data/research/pump_watchlist/watchlist_2026-10-19.json", encoding="utf-8"))
bases = [x["base"] for x in lst["full_ranking"]]
FUT = ROOT / "data/historical/binance_futures"
frames_full = {}
for b in bases:
    for cand in (b, "1000" + b):
        p = FUT / f"{cand}_USDT" / "4h.parquet"
        if p.exists():
            df = pd.read_parquet(p, columns=["open", "high", "low", "close", "volume"])
            df = df[~df.index.duplicated(keep="last")].sort_index()
            frames_full[f"{b}/USDT"] = df
            break
print("universe coins with 4h perp:", len(frames_full), flush=True)

rows = []
ts_list = list(pd.date_range(START, END_TS, freq=f"{STEP_H}h"))[:MAXN]
t0 = time.time()
for k, ts in enumerate(ts_list):
    fr = {}
    for sym, df in frames_full.items():
        # complete bars only: open_time + 4h <= ts
        i = df.index.searchsorted(ts - pd.Timedelta(hours=4), side="right")
        sub = df.iloc[max(0, i - 200) : i]
        if len(sub) < 60:
            continue
        if sub.index[-1] < ts - pd.Timedelta(hours=8):  # coin not trading now
            continue
        fr[sym] = sub
    if len(fr) < 30:
        continue
    now = ts.to_pydatetime().replace(tzinfo=__import__("datetime").timezone.utc)
    try:
        out = ar.build_altcoin_rows(market_frames=fr, timeframe="4h", now=now)
    except Exception as exc:  # noqa: BLE001
        print("ERR", ts, repr(exc)[:200], flush=True)
        continue
    for r in out:
        sym = r["symbol"]
        df = frames_full[sym]
        rows.append({
            "ts": ts, "symbol": sym,
            **{k2: r.get(k2) for k2 in ("upside_score", "layout_score", "alert_score", "anomaly_score", "accumulation_score",
                                          "control_score", "ignition_score", "continuation_score", "risk_penalty",
                                          "liquidity_trap_score", "priority_score", "narrative_heat_score", "meme_rotation_score",
                                          "signal_state")},
        })
    if k % 10 == 0:
        print(f"{k}/{len(ts_list)} {ts} coins={len(fr)} rows={len(rows)} elapsed={time.time()-t0:.0f}s", flush=True)
D = pd.DataFrame(rows)
D.to_parquet(OUT / "study2_scores.parquet")
print("saved", D.shape, "elapsed", round(time.time() - t0), "s")
