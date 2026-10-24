"""PumpGlass public alert log (pump-catcher, 30D) against Binance prices and a same-coin null.

Input is the alert log as rendered on https://pumpglass.com/pump-catcher (every row
expanded in a browser; the site's /api/ is robots-disallowed and is not called).
Each alert is treated as a long entry because the scanner flags pumps.

Checks, in this order:
1. Is the alert price the market price at the alert timestamp (1m bar range)?
2. Does the advertised "rise after alert" equal the maximum high from the alert
   until now (an open-ended, hindsight peak)?
3. Executable outcome: entry at the first 5m open >= alert + 60s, fixed holds of
   1/4/24/72/168h, 20bps (and 80bps) round trip, funding ignored on both sides.
4. Null: the same coin, random hourly starts in the 30 days before the alert,
   same hold, same cost. Permutation p and a week-block bootstrap of the excess.

Only Binance public USD-M kline endpoints are read. No orders, no AI calls.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT = ROOT / "data/research/pumpglass_20261024"
REPORT = ROOT / "reports/pumpglass_alerts_20261024"
KLINES = "https://fapi.binance.com/fapi/v1/klines"
HOURS = (1, 4, 24, 72, 168)
COSTS = (0.002, 0.008)
DRAWS = 5000


def parse_rows(path: Path) -> pd.DataFrame:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        sym, ts, price, rise, to_peak, dip, status = line.split("|")
        rows.append({"symbol": sym, "ts": int(ts), "alert_price": float(price),
                     "claimed_peak_pct": float(rise.rstrip("%")), "time_to_peak": to_peak or None,
                     "claimed_dip_pct": float(dip.rstrip("%")), "status": status})
    frame = pd.DataFrame(rows)
    frame["alert_at"] = pd.to_datetime(frame["ts"], unit="s", utc=True)
    return frame


class Binance:
    def __init__(self):
        import requests
        from config.settings import settings
        from core.utils.proxy_env import ensure_proxy_env

        ensure_proxy_env(settings)
        self.session = requests.Session()
        self.calls = 0
        self.cache = OUT / "klines"
        self.cache.mkdir(parents=True, exist_ok=True)

    def klines(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        key = self.cache / f"{symbol.encode('utf-8').hex()}_{interval}_{start_ms}_{end_ms}.json"
        if key.exists():
            rows = json.loads(key.read_text(encoding="utf-8"))
        else:
            rows, cursor = [], start_ms
            while cursor <= end_ms:
                for attempt in range(3):
                    r = self.session.get(KLINES, params={"symbol": symbol, "interval": interval, "startTime": cursor,
                                                         "endTime": end_ms, "limit": 1500}, timeout=20)
                    self.calls += 1
                    if r.status_code == 200:
                        break
                    time.sleep(2 * (attempt + 1))
                if r.status_code != 200:
                    raise RuntimeError(f"{symbol} {interval} HTTP {r.status_code}: {r.text[:120]}")
                page = r.json()
                time.sleep(0.15)
                if not page:
                    break
                rows.extend(page)
                if len(page) < 1500:
                    break
                cursor = page[-1][0] + 1
            key.write_text(json.dumps(rows), encoding="utf-8")
        frame = pd.DataFrame(rows, columns=list(range(12)))
        if frame.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close"])
        frame = frame.rename(columns={0: "t", 1: "open", 2: "high", 3: "low", 4: "close"})[["t", "open", "high", "low", "close"]]
        frame.index = pd.to_datetime(frame.pop("t"), unit="ms", utc=True)
        return frame.astype(float)[~frame.index.duplicated()]


def evaluate(alerts: pd.DataFrame, api: Binance, now: pd.Timestamp) -> list[dict]:
    out = []
    for a in alerts.itertuples():
        rec = {"symbol": a.symbol, "alert_at": a.alert_at, "alert_price": a.alert_price, "claimed_peak_pct": a.claimed_peak_pct,
               "claimed_dip_pct": a.claimed_dip_pct, "status": a.status}
        t_ms = a.ts * 1000
        try:
            m1 = api.klines(a.symbol, "1m", t_ms - 120_000, t_ms + 120_000)
            h1 = api.klines(a.symbol, "1h", t_ms - 31 * 86400_000, int(min(now.timestamp() * 1000, t_ms + 30 * 86400_000)))
            m5 = api.klines(a.symbol, "5m", t_ms, int(min(now.timestamp() * 1000, t_ms + 7 * 86400_000 + 600_000)))
        except Exception as exc:  # delisted or renamed contracts
            rec["error"] = str(exc)[:120]
            out.append(rec)
            continue
        bar = m1[m1.index <= a.alert_at].tail(1)
        if not bar.empty:
            lo, hi = float(bar["low"].iloc[0]), float(bar["high"].iloc[0])
            rec["alert_minute_low"], rec["alert_minute_high"] = lo, hi
            rec["alert_price_in_minute_range"] = lo * 0.999 <= a.alert_price <= hi * 1.001
            rec["alert_vs_minute_close_pct"] = (a.alert_price / float(bar["close"].iloc[0]) - 1) * 100
        after = h1[h1.index >= a.alert_at.floor("h")]
        if not after.empty:
            rec["max_high_until_now_pct"] = (after["high"].max() / a.alert_price - 1) * 100
            rec["last_close_vs_alert_pct"] = (float(after["close"].iloc[-1]) / a.alert_price - 1) * 100
        entry_at = (a.alert_at + pd.Timedelta(seconds=60)).ceil("5min")
        if entry_at in m5.index:
            p0 = float(m5.at[entry_at, "open"])
            rec["entry_at"], rec["entry_price"] = entry_at, p0
            rec["entry_vs_alert_pct"] = (p0 / a.alert_price - 1) * 100
            for h in HOURS:
                exit_at = entry_at + pd.Timedelta(hours=h)
                if exit_at in m5.index:
                    rec[f"gross_{h}h"] = max(-1.0, float(m5.at[exit_at, "open"]) / p0 - 1)
        # Null: hourly opens in the 30 days strictly before the alert, same hold length.
        o = h1["open"]
        before = o[(o.index < a.alert_at.floor("h") - pd.Timedelta(hours=1)) & (o.index >= a.alert_at - pd.Timedelta(days=30))]
        for h in HOURS:
            starts = before.index[before.index + pd.Timedelta(hours=h) < a.alert_at.floor("h")]
            exits = o.reindex(starts + pd.Timedelta(hours=h)).to_numpy()
            vals = exits / o.reindex(starts).to_numpy() - 1
            vals = vals[np.isfinite(vals)]
            rec[f"null_{h}h"] = vals.tolist() if len(vals) >= 100 else None
        out.append(rec)
    return out


def week_block_ci(values: np.ndarray, stamps: pd.Series, draws: int = 5000, seed: int = 1):
    week = (stamps - pd.to_timedelta(stamps.dt.weekday, unit="D")).dt.strftime("%Y-%m-%d").values
    weeks = pd.Series(values, index=week).groupby(level=0).agg(["sum", "count"])
    if len(weeks) < 3:
        return None
    pick = np.random.default_rng(seed).integers(0, len(weeks), size=(draws, len(weeks)))
    boot = weeks["sum"].to_numpy()[pick].sum(1) / weeks["count"].to_numpy()[pick].sum(1)
    return [float(np.quantile(boot, .025)), float(np.quantile(boot, .975))]


def summarise(rows: list[dict]) -> pd.DataFrame:
    rng = np.random.default_rng(20261024)
    cells = []
    for h in HOURS:
        use = [r for r in rows if r.get(f"gross_{h}h") is not None and r.get(f"null_{h}h")]
        if len(use) < 5:
            continue
        gross = np.array([r[f"gross_{h}h"] for r in use])
        nulls = [np.array(r[f"null_{h}h"]) for r in use]
        null_means = np.array([n.mean() for n in nulls])
        draws = np.zeros(DRAWS)
        for n in nulls:
            draws += n[rng.integers(0, len(n), size=DRAWS)]
        draws /= len(nulls)
        for cost in COSTS:
            net = np.maximum(-1, gross - cost)
            excess = net - (null_means - cost)
            cells.append({"hours": h, "cost_bps": int(cost * 10000), "n": len(use), "hit_rate": float((net > 0).mean()),
                          "mean_net": float(net.mean()), "median_net": float(np.median(net)),
                          "mean_without_best": float(np.delete(net, net.argmax()).mean()),
                          "null_mean_net": float((null_means - cost).mean()), "mean_excess_vs_null": float(excess.mean()),
                          "p_greater_than_null": float((1 + ((draws - cost) >= net.mean()).sum()) / (DRAWS + 1)),
                          "excess_week_block_95": week_block_ci(excess, pd.Series([r["alert_at"] for r in use]))})
    return pd.DataFrame(cells)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.mkdir(parents=True, exist_ok=True)
    alerts = parse_rows(OUT / "alert_log_rows.txt")
    now = pd.Timestamp.now(tz="UTC")
    api = Binance()
    rows = evaluate(alerts, api, now)
    flat = pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("null_")} for r in rows])
    flat.to_csv(OUT / "alert_outcomes.csv", index=False, encoding="utf-8-sig")
    summary = summarise(rows)
    summary.to_csv(REPORT / "pumpglass_null_test.csv", index=False, encoding="utf-8-sig")
    notes = {"alerts": len(alerts), "evaluated": int(flat.get("entry_price", pd.Series(dtype=float)).notna().sum()),
             "errors": flat["error"].dropna().tolist() if "error" in flat else [], "api_calls": api.calls,
             "span_utc": [str(alerts["alert_at"].min()), str(alerts["alert_at"].max())], "computed_at_utc": str(now)}
    (OUT / "run_notes.json").write_text(json.dumps(notes, indent=1, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(notes, indent=1, ensure_ascii=False))
    print(summary.round(4).to_string(index=False))


if __name__ == "__main__":
    main()
