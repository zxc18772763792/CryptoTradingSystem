"""INIT after the Korean caution (Oct 2026): who drove the squeeze, from exchange data only.

The native Initia chain has been halted since 2026-10-23 15:41 UTC, so there is no on-chain flow to trace; this
collects cross-venue prices, Binance positioning series, funding and Binance's official daily aggTrades archives
for the pump window, then analyses lead-lag, open interest vs price, and large/repeated prints.

Raw responses stay in ~/.crypto_trading_system/research/init_squeeze_20261031/ (not in the repo).
Commands: collect | analyze
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.audit_official_timezone_reference import session  # noqa: E402

OUT = Path.home() / ".crypto_trading_system/research/init_squeeze_20261031"
RAW = OUT / "raw"
START = pd.Timestamp("2026-10-20 00:00", tz="UTC")
END = pd.Timestamp("2026-10-31 06:00", tz="UTC")
FINE_START = pd.Timestamp("2026-10-26 12:00", tz="UTC")
FINE_END = pd.Timestamp("2026-10-29 12:00", tz="UTC")
ARCHIVE_DAYS = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29"]


def ms(t: pd.Timestamp) -> int:
    return int(t.timestamp() * 1000)


def get(url: str, params: dict | None = None, pause: float = 0.35):
    for attempt in range(3):
        time.sleep(pause)
        try:
            r = session().get(url, params=params, timeout=(8, 40))
        except Exception:
            if attempt == 2:
                raise
            time.sleep(3)
            continue
        if r.status_code in (418, 429):
            raise RuntimeError(f"rate limited {r.status_code}: stop")
        return r
    raise RuntimeError("unreachable")


def save(name: str, rows) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    (RAW / f"{name}.json").write_text(json.dumps({"received_utc": datetime.now(timezone.utc).isoformat(),
                                                   "rows": rows}), encoding="utf-8")


def binance_klines(base: str, path: str, symbol: str, interval: str, start, end, step_ms: int) -> list:
    rows, cursor = [], ms(start)
    while cursor < ms(end):
        page = get(base + path, {"symbol": symbol, "interval": interval, "startTime": cursor,
                                 "endTime": ms(end) - 1, "limit": 1000}).json()
        if not page:
            break
        rows += page
        cursor = int(page[-1][0]) + step_ms
        if len(page) < 1000:
            break
    return rows


def futures_data(path: str, start, end) -> list:
    rows, cursor = [], ms(start)
    while cursor < ms(end):
        page = get("https://fapi.binance.com/futures/data/" + path,
                   {"symbol": "INITUSDT", "period": "5m", "startTime": cursor,
                    "endTime": min(ms(end), cursor + 500 * 300_000) - 1, "limit": 500}).json()
        if isinstance(page, dict):
            raise RuntimeError(f"{path}: {page}")
        rows += page
        cursor = cursor + 500 * 300_000
    return rows


def korean_candles(base: str, market: str, unit: int, start, end) -> list:
    """Upbit-compatible candles paged backwards with `to` (UTC)."""
    rows, to = [], end
    while to > start:
        page = get(f"{base}/v1/candles/minutes/{unit}",
                   {"market": market, "count": 200, "to": to.strftime("%Y-%m-%dT%H:%M:%S")}).json()
        if not isinstance(page, list) or not page:
            break
        rows += page
        to = pd.Timestamp(page[-1]["candle_date_time_utc"], tz="UTC")
    return rows


def cmd_collect(_a) -> None:
    fapi, spot = "https://fapi.binance.com", "https://api.binance.com"
    save("perp_5m", binance_klines(fapi, "/fapi/v1/klines", "INITUSDT", "5m", START, END, 300_000))
    save("spot_5m", binance_klines(spot, "/api/v3/klines", "INITUSDT", "5m", START, END, 300_000))
    save("spot_try_5m", binance_klines(spot, "/api/v3/klines", "INITTRY", "5m", START, END, 300_000))
    save("perp_1m", binance_klines(fapi, "/fapi/v1/klines", "INITUSDT", "1m", FINE_START, FINE_END, 60_000))
    save("spot_1m", binance_klines(spot, "/api/v3/klines", "INITUSDT", "1m", FINE_START, FINE_END, 60_000))
    save("try_1m", binance_klines(spot, "/api/v3/klines", "INITTRY", "1m", FINE_START, FINE_END, 60_000))
    save("usdttry_1m", binance_klines(spot, "/api/v3/klines", "USDTTRY", "1m", FINE_START, FINE_END, 60_000))
    print("binance klines done", flush=True)
    for path in ("openInterestHist", "topLongShortPositionRatio", "topLongShortAccountRatio",
                 "globalLongShortAccountRatio", "takerlongshortRatio"):
        save("fut_" + path, futures_data(path, START, END))
    save("funding", get(fapi + "/fapi/v1/fundingRate", {"symbol": "INITUSDT", "limit": 1000}).json())
    print("binance futures data done", flush=True)
    by = []
    cursor = ms(START)
    while cursor < ms(END):
        page = get("https://api.bybit.com/v5/market/kline", {"category": "linear", "symbol": "INITUSDT",
                                                             "interval": "5", "start": cursor,
                                                             "end": min(ms(END), cursor + 1000 * 300_000) - 1,
                                                             "limit": 1000}).json()["result"]["list"]
        by += page
        cursor += 1000 * 300_000
    save("bybit_5m", by)
    save("bithumb_5m", korean_candles("https://api.bithumb.com", "KRW-INIT", 5, START, END))
    save("bithumb_1m", korean_candles("https://api.bithumb.com", "KRW-INIT", 1, FINE_START, FINE_END))
    save("upbit_usdtkrw_5m", korean_candles("https://api.upbit.com", "KRW-USDT", 5, START, END))
    save("upbit_btcinit_5m", korean_candles("https://api.upbit.com", "BTC-INIT", 5, START, END))
    save("upbit_krwbtc_5m", korean_candles("https://api.upbit.com", "KRW-BTC", 5, START, END))
    print("bybit / korea done", flush=True)
    for kind, base in (("perp", "https://data.binance.vision/data/futures/um/daily/aggTrades/INITUSDT/"),
                       ("spot", "https://data.binance.vision/data/spot/daily/aggTrades/INITUSDT/")):
        for day in ARCHIVE_DAYS:
            path = RAW / f"aggtrades_{kind}_{day}.zip"
            if path.exists():
                continue
            r = get(base + f"INITUSDT-aggTrades-{day}.zip", pause=1.0)
            if r.status_code == 200:
                path.write_bytes(r.content)
                print(kind, day, len(r.content), "bytes", flush=True)
            else:
                print(kind, day, "missing", r.status_code, flush=True)


# ---------------------------------------------------------------- analysis

def load(name: str):
    return json.loads((RAW / f"{name}.json").read_text(encoding="utf-8"))["rows"]


def kl(name: str) -> pd.DataFrame:
    rows = load(name)
    f = pd.DataFrame([[float(x) for x in r[1:6]] + [float(r[9]) if len(r) > 9 else np.nan] for r in rows],
                     columns=["open", "high", "low", "close", "vol", "taker_buy"],
                     index=pd.to_datetime([int(r[0]) for r in rows], unit="ms", utc=True))
    return f[~f.index.duplicated()].sort_index()


def korean(name: str) -> pd.DataFrame:
    rows = load(name)
    f = pd.DataFrame({"open": [r["opening_price"] for r in rows], "high": [r["high_price"] for r in rows],
                      "low": [r["low_price"] for r in rows], "close": [r["trade_price"] for r in rows],
                      "vol": [r["candle_acc_trade_volume"] for r in rows],
                      "value": [r["candle_acc_trade_price"] for r in rows]},
                     index=pd.to_datetime([r["candle_date_time_utc"] for r in rows], utc=True))
    return f[~f.index.duplicated()].sort_index()


def _trades_as_agg(path: Path) -> pd.DataFrame:
    """Raw-trades archive (used when Binance published no aggTrades file for a day), folded like aggTrades:
    fills of one taker order share the millisecond, side and price."""
    with zipfile.ZipFile(path) as z:
        f = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])))
    f.columns = [c.strip().lower() for c in f.columns]
    f = f.rename(columns={"is_buyer_maker": "buyer_maker", "time": "ts"})
    f["buyer_maker"] = f["buyer_maker"].astype(str).str.lower().isin(["true", "1"])
    g = (f["ts"] != f["ts"].shift()) | (f["buyer_maker"] != f["buyer_maker"].shift()) | (f["price"] != f["price"].shift())
    f["grp"] = g.cumsum()
    a = f.groupby("grp").agg(price=("price", "first"), qty=("qty", "sum"), ts=("ts", "first"),
                             buyer_maker=("buyer_maker", "first")).reset_index(drop=True)
    unit = "us" if a["ts"].iloc[0] > 1e14 else "ms"
    a["time"] = pd.to_datetime(a["ts"], unit=unit, utc=True)
    return a


def aggtrades(kind: str) -> pd.DataFrame:
    frames = []
    for day in ARCHIVE_DAYS:
        path = RAW / f"aggtrades_{kind}_{day}.zip"
        if not path.exists():
            alt = RAW / f"trades_{kind}_{day}.zip"
            if alt.exists():
                frames.append(_trades_as_agg(alt))
            continue
        with zipfile.ZipFile(path) as z:
            raw = z.read(z.namelist()[0])
        f = pd.read_csv(io.BytesIO(raw), header=None)
        if not str(f.iloc[0, 0]).isdigit():  # some archives carry a header row
            f = f.iloc[1:]
        f = f.iloc[:, :7]
        f.columns = ["agg_id", "price", "qty", "first_id", "last_id", "ts", "buyer_maker"]
        f = f.astype({"price": float, "qty": float, "ts": "int64"})
        unit = "us" if f["ts"].iloc[0] > 1e14 else "ms"
        f["time"] = pd.to_datetime(f["ts"], unit=unit, utc=True)
        f["buyer_maker"] = f["buyer_maker"].astype(str).str.lower().isin(["true", "1"])
        frames.append(f)
    return pd.concat(frames).sort_values("time") if frames else pd.DataFrame()


def cmd_analyze(_a) -> None:
    out: dict = {}
    perp, spot, tr = kl("perp_5m"), kl("spot_5m"), kl("spot_try_5m")
    by_rows = load("bybit_5m")
    bybit = pd.DataFrame([[float(x) for x in r[1:6]] for r in by_rows], columns=["open", "high", "low", "close", "vol"],
                         index=pd.to_datetime([int(r[0]) for r in by_rows], unit="ms", utc=True)).sort_index()
    bit, fx, btc_init, krwbtc = korean("bithumb_5m"), korean("upbit_usdtkrw_5m"), korean("upbit_btcinit_5m"), korean("upbit_krwbtc_5m")
    df = pd.DataFrame({"perp": perp["close"], "spot": spot["close"], "bybit": bybit["close"],
                       "bithumb_krw": bit["close"], "usdtkrw": fx["close"], "upbit_btc": btc_init["close"],
                       "krwbtc": krwbtc["close"]}).ffill()
    df["bithumb_usd"] = df["bithumb_krw"] / df["usdtkrw"]
    df["upbit_usd"] = df["upbit_btc"] * df["krwbtc"] / df["usdtkrw"]
    df["prem_bithumb"] = df["bithumb_usd"] / df["spot"] - 1
    df["prem_upbit_btc"] = df["upbit_usd"] / df["spot"] - 1
    df["basis_perp"] = df["perp"] / df["spot"] - 1
    oi = pd.DataFrame(load("fut_openInterestHist"))
    oi.index = pd.to_datetime(oi["timestamp"].astype("int64"), unit="ms", utc=True)
    df["oi_contracts"] = oi["sumOpenInterest"].astype(float)
    for name, col in (("fut_topLongShortPositionRatio", "top_pos_long"), ("fut_topLongShortAccountRatio", "top_acc_long"),
                      ("fut_globalLongShortAccountRatio", "all_acc_long")):
        f = pd.DataFrame(load(name))
        f.index = pd.to_datetime(f["timestamp"].astype("int64"), unit="ms", utc=True)
        df[col] = f["longAccount"].astype(float)
    tk = pd.DataFrame(load("fut_takerlongshortRatio"))
    tk.index = pd.to_datetime(tk["timestamp"].astype("int64"), unit="ms", utc=True)
    df["taker_buy_sell"] = tk["buySellRatio"].astype(float)
    df["perp_vol_usd"] = perp["vol"] * perp["close"]
    df["perp_taker_buy_share"] = perp["taker_buy"] / perp["vol"]
    df["spot_vol_usd"] = spot["vol"] * spot["close"]
    df["spot_taker_buy_share"] = spot["taker_buy"] / spot["vol"]
    df["try_vol_usd"] = tr["vol"] * df["spot"]
    df["bithumb_vol_usd"] = bit["value"] / df["usdtkrw"]
    df["bybit_vol_usd"] = bybit["vol"] * bybit["close"]
    fund = pd.DataFrame(load("funding"))
    fund.index = pd.to_datetime(fund["fundingTime"].astype("int64"), unit="ms", utc=True)
    fund = fund[(fund.index >= START) & (fund.index <= END)]
    out["funding"] = {str(k): float(v) for k, v in fund["fundingRate"].astype(float).items()}
    df.to_csv(OUT / "timeline_5m.csv")
    h = df.resample("1h").agg({**{c: "last" for c in df.columns if not c.endswith("_vol_usd")},
                               **{c: "sum" for c in df.columns if c.endswith("_vol_usd")}})
    h.to_csv(OUT / "timeline_1h.csv")
    # venue volume shares by phase
    phases = {"pre_incident 10/20-10/23 15:40": (START, pd.Timestamp("2026-10-23 15:40", tz="UTC")),
              "halt_to_caution 10/23-10/26 04:00": (pd.Timestamp("2026-10-23 15:40", tz="UTC"), pd.Timestamp("2026-10-26 04:00", tz="UTC")),
              "caution_to_entry 10/26 04:00-10/27": (pd.Timestamp("2026-10-26 04:00", tz="UTC"), pd.Timestamp("2026-10-27 00:00", tz="UTC")),
              "squeeze 10/27-10/28 12:00": (pd.Timestamp("2026-10-27 00:00", tz="UTC"), pd.Timestamp("2026-10-28 12:00", tz="UTC")),
              "after 10/28 12:00-": (pd.Timestamp("2026-10-28 12:00", tz="UTC"), END)}
    vols = ["perp_vol_usd", "spot_vol_usd", "try_vol_usd", "bithumb_vol_usd", "bybit_vol_usd"]
    out["phases"] = {}
    for name, (a, b) in phases.items():
        g = df[(df.index >= a) & (df.index < b)]
        tot = g[vols].sum()
        out["phases"][name] = {
            "price_spot_first_last": [float(g["spot"].iloc[0]), float(g["spot"].iloc[-1])],
            "spot_high": float(g["spot"].max()), "oi_first_last": [float(g["oi_contracts"].dropna().iloc[0]), float(g["oi_contracts"].dropna().iloc[-1])],
            "venue_usd": {k: round(float(v)) for k, v in tot.items()},
            "perp_share_of_total": float(tot["perp_vol_usd"] / tot.sum()),
            "prem_bithumb_median": float(g["prem_bithumb"].median()), "prem_bithumb_max": float(g["prem_bithumb"].max()),
            "basis_perp_median": float(g["basis_perp"].median()),
            "top_pos_long_mean": float(g["top_pos_long"].mean()), "all_acc_long_mean": float(g["all_acc_long"].mean()),
            "perp_taker_buy_share": float((g["perp_taker_buy_share"] * g["perp_vol_usd"]).sum() / g["perp_vol_usd"].sum()),
            "spot_taker_buy_share": float((g["spot_taker_buy_share"] * g["spot_vol_usd"]).sum() / g["spot_vol_usd"].sum())}
    # squeeze legs: biggest 1h moves with OI change and taker share
    h["ret"] = h["spot"].pct_change()
    h["oi_chg"] = h["oi_contracts"].pct_change()
    legs = h[(h.index >= pd.Timestamp("2026-10-26", tz="UTC")) & (h.index < pd.Timestamp("2026-10-30", tz="UTC"))]
    top = legs.reindex(legs["ret"].abs().sort_values(ascending=False).index[:12])
    out["top_hours"] = [{"hour": str(i), "ret": float(r.ret), "oi_chg": float(r.oi_chg), "perp_usd": round(float(r.perp_vol_usd)),
                         "spot_usd": round(float(r.spot_vol_usd)), "bithumb_usd": round(float(r.bithumb_vol_usd)),
                         "try_usd": round(float(r.try_vol_usd)), "prem_bithumb": float(r.prem_bithumb),
                         "perp_taker_buy": float(r.perp_taker_buy_share), "top_pos_long": float(r.top_pos_long)}
                        for i, r in top.iterrows()]
    # minute lead-lag on the fine window: which venue's returns lead
    p1, s1 = kl("perp_1m"), kl("spot_1m")
    b1 = korean("bithumb_1m")
    cols = {"perp": p1["close"], "spot": s1["close"], "bithumb": b1["close"]}
    if (RAW / "try_1m.json").exists():
        t1, fx1 = kl("try_1m"), kl("usdttry_1m")
        cols["try_usd"] = t1["close"] / fx1["close"].reindex(t1.index).ffill()
        tv = pd.DataFrame({"try": t1["vol"] * cols["try_usd"], "spot": s1["vol"] * s1["close"], "perp": p1["vol"] * p1["close"],
                           "try_taker_buy": t1["taker_buy"] / t1["vol"]})
        sq = tv[(tv.index >= pd.Timestamp("2026-10-27", tz="UTC")) & (tv.index < pd.Timestamp("2026-10-29", tz="UTC"))]
        burst = sq.loc[sq["perp"].nlargest(30).index]
        out["try_market"] = {"try_share_of_spot_usd_squeeze": float(sq["try"].sum() / (sq["try"].sum() + sq["spot"].sum())),
                             "try_share_of_spot_usd_top30_perp_minutes": float(burst["try"].sum() / (burst["try"].sum() + burst["spot"].sum())),
                             "try_taker_buy_top30": float((burst["try_taker_buy"] * burst["try"]).sum() / burst["try"].sum()),
                             "try_premium_median": float((cols["try_usd"] / s1["close"] - 1).dropna().median())}
    m = pd.DataFrame(cols).ffill().dropna()
    r = np.log(m).diff().dropna()
    lead = {}
    pairs = [("perp", "spot"), ("perp", "bithumb"), ("spot", "bithumb")] + ([("try_usd", "spot"), ("try_usd", "perp")] if "try_usd" in m else [])
    for a, b in pairs:
        lead[f"{a}->{b}"] = {str(k): float(r[a].shift(k).corr(r[b])) for k in (-3, -2, -1, 0, 1, 2, 3)}
    out["lead_lag_1m"] = lead
    # aggTrades: large prints and repeated sizes during the squeeze
    for kind in ("perp", "spot"):
        t = aggtrades(kind)
        if t.empty:
            continue
        t["usd"] = t["price"] * t["qty"]
        t["side"] = np.where(t["buyer_maker"], "sell", "buy")
        w = t[(t["time"] >= pd.Timestamp("2026-10-27", tz="UTC")) & (t["time"] < pd.Timestamp("2026-10-28 12:00", tz="UTC"))]
        q = w["usd"].quantile([0.5, 0.9, 0.99, 0.999]).to_dict()
        big = w[w["usd"] >= 20_000]
        sizes = w.groupby(["side", "qty"]).agg(n=("qty", "size"), usd=("usd", "sum")).reset_index()
        rep = sizes[(sizes["n"] >= 15) & (sizes["qty"] >= 5_000)].sort_values("usd", ascending=False).head(15)
        burst = w.set_index("time").groupby("side")["usd"].resample("1min").sum().unstack(0).fillna(0)
        out[f"aggtrades_{kind}"] = {
            "trades_in_squeeze": int(len(w)), "usd_in_squeeze": round(float(w["usd"].sum())),
            "buy_share_usd": float(w.loc[w["side"] == "buy", "usd"].sum() / w["usd"].sum()),
            "size_quantiles_usd": {str(k): round(float(v), 2) for k, v in q.items()},
            "prints_over_20k": int(len(big)), "prints_over_20k_buy_share": float((big["side"] == "buy").mean()) if len(big) else None,
            "prints_over_20k_usd": round(float(big["usd"].sum())),
            "largest": [{"time": str(x.time), "side": x.side, "price": x.price, "usd": round(x.usd)}
                        for x in big.sort_values("usd", ascending=False).head(12).itertuples()],
            "repeated_sizes": [{"side": x.side, "qty": x.qty, "n": int(x.n), "usd": round(x.usd)} for x in rep.itertuples()],
            "top_buy_minutes": [{"minute": str(i), "buy_usd": round(float(v))}
                                for i, v in burst.get("buy", pd.Series(dtype=float)).sort_values(ascending=False).head(10).items()],
        }
    (OUT / "analysis.json").write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    print(json.dumps(out, indent=1, default=str)[:12000])


def cmd_screen(_a) -> None:
    """Coins in the INIT setup today: Korean caution flag + wallet freeze + concentrated big perp longs.

    A structural watchlist (squeeze risk for shorts), not a forecast of anyone's next trade.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap = OUT / "screen" / stamp
    snap.mkdir(parents=True, exist_ok=True)
    up = get("https://api.upbit.com/v1/market/all", {"isDetails": "true"}).json()
    bt = get("https://api.bithumb.com/v1/market/all", {"isDetails": "true"}).json()
    ws = get("https://api.bithumb.com/public/assetsstatus/ALL").json().get("data") or {}
    (snap / "korea.json").write_text(json.dumps({"upbit": up, "bithumb": bt, "bithumb_wallet": ws}), encoding="utf-8")
    flagged: dict = {}
    for m in up:
        ev = m.get("market_event") or {}
        tok = m["market"].split("-")[1]
        cautions = [k for k, v in (ev.get("caution") or {}).items() if v]
        if ev.get("warning") or cautions:
            f = flagged.setdefault(tok, {"upbit_warning": False, "upbit_caution": set(), "bithumb_warning": None})
            f["upbit_warning"] |= bool(ev.get("warning"))
            f["upbit_caution"] |= set(cautions)
    for m in bt:
        tok = m["market"].split("-")[1]
        if m.get("market_warning") and m["market_warning"] != "NONE":
            flagged.setdefault(tok, {"upbit_warning": False, "upbit_caution": set(), "bithumb_warning": None})["bithumb_warning"] = m["market_warning"]
    ex = get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
    perps = {x["baseAsset"]: x["symbol"] for x in ex["symbols"]
             if x["quoteAsset"] == "USDT" and x["status"] == "TRADING" and x["contractType"] == "PERPETUAL"}
    cg = []
    for page in (1, 2, 3, 4):
        cg += get("https://api.coingecko.com/api/v3/coins/markets", {"vs_currency": "usd", "per_page": 250, "page": page},
                  pause=2.5).json()
    float_of: dict = {}
    for c in sorted(cg, key=lambda c: -(c.get("market_cap") or 0)):
        sym = (c.get("symbol") or "").upper()
        tot = c.get("total_supply") or c.get("max_supply")
        if sym not in float_of and tot:
            float_of[sym] = {"float": (c.get("circulating_supply") or 0) / tot, "mcap": c.get("market_cap")}
    rows = []
    for tok, f in sorted(flagged.items()):
        sym = perps.get(tok) or perps.get("1000" + tok)
        w = ws.get(tok) or {}
        row = {"token": tok, "perp": sym, "upbit_warning": f["upbit_warning"], "upbit_caution": ",".join(sorted(f["upbit_caution"])),
               "bithumb_warning": f["bithumb_warning"], "bithumb_deposit": w.get("deposit_status"),
               "bithumb_withdraw": w.get("withdrawal_status"), **{k: v for k, v in (float_of.get(tok) or {}).items()}}
        if sym:
            def last(path, period="1h", limit=72):
                return get("https://fapi.binance.com/futures/data/" + path, {"symbol": sym, "period": period, "limit": limit}).json()
            pos, acc, glo = last("topLongShortPositionRatio"), last("topLongShortAccountRatio"), last("globalLongShortAccountRatio")
            oi = last("openInterestHist", limit=25)
            prem = get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": sym}).json()
            if isinstance(pos, list) and pos:
                row.update({"top_pos_long": float(pos[-1]["longAccount"]), "top_pos_long_3d": float(np.mean([float(x["longAccount"]) for x in pos])),
                            "top_acc_long": float(acc[-1]["longAccount"]) if acc else None,
                            "all_acc_long": float(glo[-1]["longAccount"]) if glo else None,
                            "oi_24h": float(oi[-1]["sumOpenInterest"]) / float(oi[0]["sumOpenInterest"]) - 1 if len(oi) > 1 else None,
                            "funding": float(prem.get("lastFundingRate") or 0)})
                row["concentration"] = row["top_pos_long"] - (row["top_acc_long"] or 0)
        rows.append(row)
    df = pd.DataFrame(rows)
    df["wallet_frozen_bithumb"] = (df["bithumb_deposit"] == 0) & (df["bithumb_withdraw"] == 0)
    df.to_csv(snap / "screen.csv", index=False)
    show = df[df["perp"].notna()].sort_values(["wallet_frozen_bithumb", "concentration"], ascending=False)
    pd.set_option("display.width", 250)
    print(show[["token", "perp", "upbit_warning", "upbit_caution", "bithumb_warning", "bithumb_deposit", "bithumb_withdraw",
                "float", "mcap", "top_pos_long", "top_pos_long_3d", "top_acc_long", "all_acc_long", "concentration",
                "oi_24h", "funding"]].round(3).to_string())
    print("flagged tokens", len(df), "with perp", int(df["perp"].notna().sum()), "snapshot", snap)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["collect", "analyze", "screen"])
    a = ap.parse_args()
    {"collect": cmd_collect, "analyze": cmd_analyze, "screen": cmd_screen}[a.cmd](a)


if __name__ == "__main__":
    main()
