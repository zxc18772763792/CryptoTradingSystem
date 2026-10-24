"""Study 3 replay: same radar scorer, but on a point-in-time, survivorship-free universe:
each day the 150 most liquid (trailing-30d USD volume) non-major USDT perps among ALL 654
local perps, delisted ones included. Output: per (ts, symbol) scores."""
from __future__ import annotations
import sys, time, warnings, glob
from datetime import timezone
from pathlib import Path
import pandas as pd

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
OUT = Path(sys.argv[1])
START = pd.Timestamp(sys.argv[2]); END_TS = pd.Timestamp(sys.argv[3]); STEP_H = int(sys.argv[4])
TOPN = 150

import core.research.altcoin_radar as ar  # noqa: E402
for name in ("record_ignition_cross_up", "record_rank_jump_event", "record_crowding_spike", "record_narrative_heat_spike"):
    setattr(ar, name, lambda *a, **k: None)
ar.bulk_update_ranks = lambda *a, **k: None
ar.get_rank_history = lambda *a, **k: []
ar.compute_rank_jump_score = lambda *a, **k: 0.0

MAJORS = {"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "TON", "USDC", "FDUSD", "BUSD", "TUSD", "USDE", "USDS", "DAI", "PAXG", "XAUT", "BTCDOM"}
FUT = ROOT / "data/historical/binance_futures"
frames, dvs = {}, {}
for d in sorted(FUT.glob("*_USDT")):
    p = d / "4h.parquet"
    if not p.exists():
        continue
    base = d.name[:-5]
    key = base
    stripped = base[4:] if base.startswith("1000") else base
    if stripped.upper() in MAJORS:
        continue
    df = pd.read_parquet(p, columns=["open", "high", "low", "close", "volume"])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    if len(df) < 120:
        continue
    frames[f"{key}/USDT"] = df
    dvs[f"{key}/USDT"] = (df["close"] * df["volume"]).resample("1D").sum()
print("perps loaded:", len(frames), flush=True)
DV = pd.DataFrame(dvs).sort_index()
DV30 = DV.rolling(30, min_periods=20).sum()

rows = []; t0 = time.time()
ts_list = list(pd.date_range(START, END_TS, freq=f"{STEP_H}h"))
for k, ts in enumerate(ts_list):
    day = (ts - pd.Timedelta(days=1)).normalize()
    if day not in DV30.index:
        continue
    liq = DV30.loc[day].dropna()
    liq = liq[liq > 0].nlargest(TOPN)
    fr = {}
    for sym in liq.index:
        df = frames[sym]
        i = df.index.searchsorted(ts - pd.Timedelta(hours=4), side="right")
        sub = df.iloc[max(0, i - 200): i]
        if len(sub) < 60 or sub.index[-1] < ts - pd.Timedelta(hours=8):
            continue
        fr[sym] = sub
    if len(fr) < 50:
        continue
    now = ts.to_pydatetime().replace(tzinfo=timezone.utc)
    try:
        out = ar.build_altcoin_rows(market_frames=fr, timeframe="4h", now=now)
    except Exception as exc:  # noqa: BLE001
        print("ERR", ts, repr(exc)[:160], flush=True); continue
    for r in out:
        rows.append({"ts": ts, "symbol": r["symbol"], "dv30": float(liq.get(r["symbol"], float("nan"))),
                     **{k2: r.get(k2) for k2 in ("upside_score", "layout_score", "alert_score", "anomaly_score", "accumulation_score",
                                                   "control_score", "ignition_score", "continuation_score", "risk_penalty",
                                                   "liquidity_trap_score", "narrative_heat_score", "signal_state")}})
    if k % 10 == 0:
        print(f"{k}/{len(ts_list)} {ts} coins={len(fr)} rows={len(rows)} elapsed={time.time()-t0:.0f}s", flush=True)
pd.DataFrame(rows).to_parquet(OUT / "study3_scores.parquet")
print("saved", len(rows), "elapsed", round(time.time() - t0), flush=True)
