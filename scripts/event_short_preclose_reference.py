"""Historical reference for the event_short_preclose_v1 paper ledger. Reads local data only; writes one csv and one json.

The rule's universe, as the forward ledger will see it:
  U: cliffs of at least 10% from the t-7 tracker's schedules (a coin with a perp), t-7 close to t-1 close.
  K: Upbit KRW listings with a perp and a Binance spot bar 30 days before the notice, D0 close to D0+7 close.
Cost lines as in the protocol: 0.20% + 0.10% round trip plus square-root impact for a declared $10,000 order per leg.
Funding: the position opens before the settlement at the entry close and closes before the one at the exit close.
The daily close stands in for the 23:55 UTC fill: local perp bars are hourly at best, so that gap is what the ledger measures.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research.unlock_events import cliff_events, entry_ticker, unlocked_series  # noqa: E402

RES = ROOT / "data/research"
FUTURES = ROOT / "data/historical/binance_futures"
SPOT = ROOT / "data/historical/binance"
PANEL = RES / "xs_daily_panel"
OUTPUT = RES / "event_short_preclose_v1"
DAY, HOUR = pd.Timedelta(days=1), pd.Timedelta(hours=1)
DATA_END = pd.Timestamp("2026-11-03")
DEV_END = pd.Timestamp("2024-12-31")
FORWARD_START = pd.Timestamp("2026-09-28")
MIN_UNLOCK_PCT = 10.0
STOP, SLIP = 0.40, 0.02
COIN_COST, BASKET_COST = 0.002, 0.001
ORDER_USD, IMPACT_Y = 10_000.0, 1.0
DEFAULT_SIGMA = 0.10
BASKET_SIZE, CAP = 30, 1.0
F = 0.05
STABLE = {"USDC", "FDUSD", "TUSD", "DAI", "USDP", "BUSD", "USDE", "USD1", "BFUSD", "EUR", "PYUSD", "RLUSD", "USDT", "PAXG", "XAUT"}
REJECT_MEAN = 2.0
SEED = 20261106


# ------------------------------------------------------------------------------------------- data
def naive(index) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(index)
    return index.tz_convert("UTC").tz_localize(None) if index.tz is not None else index


def bars(base: Path, name: str, tf: str) -> pd.DataFrame:
    folder = base / f"{name}_USDT"
    frames = []
    main = folder / f"{tf}.parquet"
    if main.exists():
        frames.append(pd.read_parquet(main))
    parts = folder / f"{tf}_parts"
    if parts.exists():
        frames += [pd.read_parquet(p) for p in sorted(parts.glob("*.parquet"))]
    if not frames:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    out = pd.concat(frames)
    out.index = naive(out.index)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out[["open", "high", "low", "close", "volume"]].astype(float)


class Market:
    def __init__(self) -> None:
        self.close = pd.read_parquet(PANEL / "col_close.parquet").astype("float64")
        self.volume = pd.read_parquet(PANEL / "col_volume.parquet").astype("float64")
        self.funding = pd.read_parquet(PANEL / "col_funding.parquet").astype("float64")
        self.sigma = self.close.pct_change(fill_method=None).rolling(30, min_periods=10).std()
        self.perps = pd.DataFrame(json.loads((RES / "coinglass_history/universe.json").read_text(encoding="utf-8"))["perps"]).set_index("perp")
        self._hourly: Dict[str, pd.DataFrame] = {}
        self._daily: Dict[str, pd.DataFrame] = {}
        self._fund: Dict[str, pd.DataFrame] = {}
        ret = self.close.pct_change(fill_method=None)
        rank = self.volume.shift(1).rank(axis=1, ascending=False)
        self.index_level = (1 + ret.where(rank <= BASKET_SIZE).mean(axis=1).fillna(0)).cumprod()

    def to_perp(self, ticker: str) -> Optional[str]:
        for name in (f"{ticker}USDT", f"1000{ticker}USDT"):
            if name in self.perps.index:
                return name
        return None

    def hourly(self, perp: str) -> pd.DataFrame:
        if perp not in self._hourly:
            self._hourly[perp] = bars(FUTURES, perp[:-4], "1h")
        return self._hourly[perp]

    def daily(self, perp: str) -> pd.DataFrame:
        if perp not in self._daily:
            self._daily[perp] = bars(FUTURES, perp[:-4], "1d")
        return self._daily[perp]

    def settlements(self, perp: str) -> pd.DataFrame:
        if perp not in self._fund:
            path = FUTURES / f"{perp[:-4]}_USDT" / "funding_rate.parquet"
            if path.exists():
                frame = pd.read_parquet(path)
                frame.index = naive(frame.index)
                self._fund[perp] = frame[~frame.index.duplicated(keep="last")].sort_index()
            else:
                self._fund[perp] = pd.DataFrame(columns=["funding_rate", "mark_price"])
        return self._fund[perp]

    def prior_move(self, day: pd.Timestamp, days: int = 60) -> float:
        level = self.index_level
        if day not in level.index:
            return float("nan")
        position = level.index.get_loc(day)
        return float(level.iloc[position] / level.iloc[position - days] - 1) if position >= days else float("nan")

    def basket(self, day: pd.Timestamp, exclude: str) -> List[str]:
        row = self.volume.loc[day].dropna().sort_values(ascending=False)
        picks = [c for c in row.index if c != exclude and c[:-4] not in STABLE and pd.notna(self.close.at[day, c])]
        return picks[:BASKET_SIZE]


# ----------------------------------------------------------------------------------------- events
def unlocked_from(path: Path) -> pd.Series:
    side = path.with_suffix(".unlocked.json")
    if side.exists() and side.stat().st_mtime >= path.stat().st_mtime:
        data = json.loads(side.read_text(encoding="utf-8"))
        return pd.Series(data["v"], index=pd.to_datetime(data["t"], unit="s", utc=True), dtype=float)
    return unlocked_series(json.loads(path.read_text(encoding="utf-8")))


def load_index(path: Path) -> List[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw["data"] if isinstance(raw, dict) else raw


def unlock_events(market: Market) -> pd.DataFrame:
    """The later cache's schedule where it exists, the study's where the token has since dropped out."""
    sources = [(RES / "unlock_short/cache", "tracker_cache"), (RES / "unlock_study", "study_cache")]
    seen, rows = set(), []
    for folder, label in sources:
        for entry in load_index(folder / "emissions_index.json"):
            slug, ticker = entry.get("protocolSlug"), entry_ticker(entry)
            path = folder / "emissions" / f"{slug}.json"
            if not ticker or ticker in seen or not path.exists():
                continue
            seen.add(ticker)
            perp = market.to_perp(ticker)
            if perp is None:
                continue
            for ev in cliff_events(entry, unlocked_from(path), 0.5):
                if ev["size_pct"] >= MIN_UNLOCK_PCT:
                    day = ev["date"].tz_localize(None)
                    rows.append({"stream": "U", "token": ticker, "perp": perp, "day": day, "size_pct": float(ev["size_pct"]),
                                 "source": label, "entry_day": day - 7 * DAY, "exit_day": day - DAY})
    return pd.DataFrame(rows)


def spot_bar_on(ticker: str, day: pd.Timestamp) -> bool:
    return day in bars(SPOT, ticker, "1d").index


def krw_events(market: Market) -> pd.DataFrame:
    from core.research.upbit_caution_tracker import krw_listing_tickers

    raw = []
    for notice in json.loads((RES / "upbit/notices.json").read_text(encoding="utf-8")):
        at = pd.Timestamp(notice["first_listed_at"]).tz_convert("UTC").tz_localize(None)
        for ticker in krw_listing_tickers(str(notice.get("title") or "")):
            raw.append({"token": ticker, "at": at, "symbol": None, "tracked": False})
    last = max(r["at"] for r in raw)
    for trade in json.loads((RES / "upbit_krw_listing/tracker.json").read_text(encoding="utf-8"))["trades"].values():
        at = pd.Timestamp(trade["notice_at"]).tz_convert("UTC").tz_localize(None)
        if at > last:
            raw.append({"token": trade["ticker"], "at": at, "symbol": trade.get("symbol"), "tracked": True})
    raw.sort(key=lambda r: r["at"])
    rows, claimed = [], {}
    for r in raw:
        day0 = r["at"].normalize()
        prior = claimed.get(r["token"])
        if prior is not None and abs((day0 - prior).days) < 30:
            continue
        claimed[r["token"]] = day0
        if r["tracked"]:
            perp = r["symbol"]
            if not perp or perp not in market.perps.index:
                continue
        else:
            perp = market.to_perp(r["token"])
            if perp is None or pd.Timestamp(market.perps.at[perp, "listed"]) > day0 or not spot_bar_on(r["token"], day0 - 30 * DAY):
                continue
        rows.append({"stream": "K", "token": r["token"], "perp": perp, "day": day0, "size_pct": np.nan,
                     "source": "tracker" if r["tracked"] else "notices", "entry_day": day0, "exit_day": day0 + 7 * DAY})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------- one trade
def coin_leg(market: Market, perp: str, entry_day: pd.Timestamp, exit_day: pd.Timestamp) -> Optional[dict]:
    daily = market.daily(perp)
    if entry_day not in daily.index or exit_day <= entry_day:
        return None
    entry_px = float(daily.at[entry_day, "close"])
    start, planned_end = entry_day + DAY, exit_day + DAY
    hourly = market.hourly(perp)
    path = hourly[(hourly.index >= start) & (hourly.index < planned_end)]
    if path.empty:
        return None
    level = entry_px * (1 + STOP)
    hit = np.flatnonzero(path["high"].to_numpy() >= level)
    if len(hit):
        i = int(hit[0])
        fill = max(level, float(path["open"].iloc[i])) * (1 + SLIP)
        exit_time, reason, held = path.index[i] + HOUR, "stop", path.iloc[: i + 1]
        funding_end = exit_time + pd.Timedelta(seconds=60)
    else:
        fill, exit_time, held = float(path["close"].iloc[-1]), path.index[-1] + HOUR, path
        reason = "time" if exit_time >= planned_end else "delisted"
        funding_end = exit_time - pd.Timedelta(seconds=90)  # closes before the settlement at the exit close
    closes = held["close"].groupby(held.index.floor("D")).last()
    closes.iloc[-1] = fill
    pnl = (closes.shift(1).fillna(entry_px) - closes) / entry_px
    fr = market.settlements(perp)
    window = fr[(fr.index > start - pd.Timedelta(seconds=90)) & (fr.index <= funding_end)]  # opens before the entry-close settlement
    fund = 0.0
    if len(window):
        mark = pd.to_numeric(window["mark_price"], errors="coerce").fillna(entry_px)
        amount = window["funding_rate"].astype(float) * mark / entry_px
        fund = float(amount.sum())
        by_day = amount.groupby(amount.index.floor("D")).sum()
        pnl = pnl.add(by_day.reindex(pnl.index).fillna(0.0))
        pnl.iloc[-1] += float(by_day[by_day.index > pnl.index[-1]].sum())
    pnl.iloc[0] -= COIN_COST / 2
    pnl.iloc[-1] -= COIN_COST / 2
    return {"entry_px": entry_px, "exit_px": fill, "reason": reason, "exit_time": exit_time, "price": (entry_px - fill) / entry_px,
            "funding": fund, "daily": pnl, "last_day": closes.index[-1]}


def side_impact(market: Market, perp: str, sigma_day: pd.Timestamp, volume_day: pd.Timestamp, order: float) -> float:
    if perp not in market.volume.columns or volume_day not in market.volume.index:
        return 0.0
    volume = market.volume.at[volume_day, perp]
    sigma = market.sigma.at[sigma_day, perp] if sigma_day in market.sigma.index else np.nan
    sigma = float(sigma) if np.isfinite(sigma) else DEFAULT_SIGMA
    return IMPACT_Y * sigma * float(np.sqrt(order / volume)) if np.isfinite(volume) and volume > 0 else 0.0


def basket_leg(market: Market, perp: str, entry_day: pd.Timestamp, days: pd.DatetimeIndex, cap: Optional[float]) -> pd.Series:
    members = market.basket(entry_day, perp)
    window = [entry_day] + [d for d in days if d in market.close.index]
    closes = market.close.loc[window, members].ffill()
    start = closes.iloc[0]
    if cap is not None:
        closes = closes.clip(upper=start * (1 + cap), axis=1)
    move = closes.diff().iloc[1:].div(start)
    rate = market.funding.reindex(index=closes.index[1:], columns=members).fillna(0.0)
    paid = rate.mul(closes.iloc[1:]).div(start)
    out = (move - paid).mean(axis=1).reindex(days).fillna(0.0)
    out.iloc[0] -= BASKET_COST / 2
    out.iloc[-1] -= BASKET_COST / 2
    return out


def build_trades(market: Market, events: pd.DataFrame) -> List[dict]:
    trades = []
    order = {"U": 0, "K": 1}
    events = events.assign(o=events.stream.map(order)).sort_values(["entry_day", "o", "perp"])
    busy: Dict[str, pd.Timestamp] = {}
    skipped = {"no_bar": 0, "same_coin": 0}
    for ev in events.itertuples(index=False):
        if ev.exit_day > DATA_END:
            continue
        if ev.perp in busy and busy[ev.perp] > ev.entry_day + DAY:
            skipped["same_coin"] += 1
            continue
        coin = coin_leg(market, ev.perp, ev.entry_day, ev.exit_day)
        if coin is None or ev.entry_day not in market.close.index:
            skipped["no_bar"] += 1
            continue
        busy[ev.perp] = coin["exit_time"]
        days = coin["daily"].index
        members = market.basket(ev.entry_day, ev.perp)
        impact_coin = side_impact(market, ev.perp, ev.entry_day, ev.entry_day, ORDER_USD) + side_impact(market, ev.perp, ev.entry_day, days[-1], ORDER_USD)
        member_order = ORDER_USD / max(len(members), 1)
        impact_basket = float(np.mean([side_impact(market, m, ev.entry_day, ev.entry_day, member_order) + side_impact(market, m, ev.entry_day, days[-1], member_order) for m in members])) if members else 0.0
        hedge = basket_leg(market, ev.perp, ev.entry_day, days, None)
        hedge_capped = basket_leg(market, ev.perp, ev.entry_day, days, CAP)
        daily = coin["daily"].copy()
        daily.iloc[0] -= (impact_coin + impact_basket) / 2
        daily.iloc[-1] -= (impact_coin + impact_basket) / 2
        coin_net = float(coin["price"] + coin["funding"] - COIN_COST - impact_coin)
        basket_net = float(hedge.sum() - impact_basket)
        capped_net = float(hedge_capped.sum() - impact_basket)
        trades.append({
            "stream": ev.stream, "token": ev.token, "perp": ev.perp, "day": ev.day, "size_pct": ev.size_pct, "source": ev.source,
            "entry_day": ev.entry_day, "exit_day": ev.exit_day, "reason": coin["reason"], "entry_px": coin["entry_px"], "exit_px": coin["exit_px"],
            "price": coin["price"], "funding": coin["funding"], "impact_coin": impact_coin, "impact_basket": impact_basket,
            "coin_net": coin_net, "basket_net": basket_net, "basket_capped_net": capped_net,
            "hedged": coin_net + basket_net, "hedged_capped": coin_net + capped_net,
            "hedged_no_impact": coin_net + basket_net + impact_coin + impact_basket,
            "prior60": market.prior_move(ev.entry_day),
            "sample": "dev" if ev.entry_day <= DEV_END else "holdout", "forward_era": ev.entry_day >= FORWARD_START,
            "daily": daily, "hedge_daily": hedge, "exit_time": coin["exit_time"]})
    print("skipped", skipped, "trades", len(trades), flush=True)
    return trades


# ------------------------------------------------------------------------------------------ account
def account(trades: List[dict], start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    ordered = sorted(trades, key=lambda x: (x["entry_day"], x["stream"], x["perp"]))
    days = pd.date_range(start, end, freq="D")
    book, equity, out, seq = [], 1.0, [], 0
    for day in days:
        while seq < len(ordered) and ordered[seq]["entry_day"] + DAY <= day:
            book.append({"notional": F * equity, "trade": ordered[seq]})
            seq += 1
        pnl, still = 0.0, []
        for pos in book:
            tr = pos["trade"]
            pnl += pos["notional"] * (float(tr["daily"].get(day, 0.0)) + float(tr["hedge_daily"].get(day, 0.0)))
            if day < tr["daily"].index.max():
                still.append(pos)
        ret = pnl / equity
        equity *= 1 + ret
        out.append(ret)
        book = still
    return pd.Series(out, index=days)


def perf(series: pd.Series) -> dict:
    curve = (1 + series).cumprod()
    return {"days": len(series), "sharpe": round(float(series.mean() / series.std() * np.sqrt(365)), 3),
            "cagr_pct": round(float(curve.iloc[-1] ** (365 / len(series)) - 1) * 100, 2),
            "max_drawdown_pct": round(float((curve / curve.cummax() - 1).min()) * 100, 2)}


def realized_drawdown(frame: pd.DataFrame) -> float:
    """The ledger's safety measure: 5% notional per trade, in exit order, open positions not marked."""
    ordered = frame.sort_values("exit_time")
    curve = (1 + F * ordered.hedged).cumprod()
    return round(float((curve / curve.cummax().clip(lower=1.0) - 1).min()) * 100, 2)


def batches(frame: pd.DataFrame, column: str = "hedged") -> pd.Series:
    return frame.groupby("entry_day")[column].mean().sort_index() * 100


def stats(frame: pd.DataFrame) -> dict:
    b, c = batches(frame), batches(frame, "hedged_capped")
    return {"trades": len(frame), "batches": len(b), "trade_mean_pct": round(frame.hedged.mean() * 100, 2),
            "trade_median_pct": round(frame.hedged.median() * 100, 2), "win_rate": round(float((frame.hedged > 0).mean()), 3),
            "batch_mean_pct": round(b.mean(), 2), "batch_sd_pct": round(b.std(), 2), "capped_batch_mean_pct": round(c.mean(), 2),
            "no_impact_batch_mean_pct": round(batches(frame, "hedged_no_impact").mean(), 2), "worst_trade_pct": round(frame.hedged.min() * 100, 2),
            "stops": int((frame.reason == "stop").sum()), "mean_impact_pp": round((frame.impact_coin + frame.impact_basket).mean() * 100, 3),
            "realized_drawdown_pct": realized_drawdown(frame)}


# ------------------------------------------------------------------------------- operating characteristics
def operating(frame: pd.DataFrame, n: int = 30, draws: int = 5000) -> dict:
    """Draw n batches without replacement, shifted to a target mean, and apply the rule's mean, capped-mean, significance and
    two-best-coins tests. The per-stream, entry-gap, schedule and rising-market tests are left out: the history cannot exercise them.
    Significance uses the batch t-statistic as a stand-in for the sign-flip test (t >= 1.699: one-sided 5%, 29 df)."""
    rng = np.random.default_rng(SEED)
    groups = [(g.hedged.mean() * 100, g.hedged_capped.mean() * 100, list(zip(g.perp, g.hedged * 100))) for _, g in frame.groupby("entry_day")]
    values = np.array([x[0] for x in groups])
    capped = np.array([x[1] for x in groups])
    base = values.mean()
    out = {}
    for label, target in (("as_reference", base), ("half", base / 2), ("zero", 0.0)):
        shift = target - base
        tally = {"rejected": 0, "passed": 0, "inconclusive": 0, "blocked_by_two_best_coins": 0}
        for _ in range(draws):
            picks = rng.choice(len(groups), n, replace=False)
            sample = values[picks] + shift
            mean, sd = sample.mean(), sample.std(ddof=1)
            if mean <= REJECT_MEAN or capped[picks].mean() + shift <= REJECT_MEAN:
                tally["rejected"] += 1
                continue
            if mean / (sd / np.sqrt(n)) < 1.699:
                tally["inconclusive"] += 1
                continue
            coins: Dict[str, List[float]] = {}
            for i in picks:
                for perp, value in groups[i][2]:
                    coins.setdefault(perp, []).append(value + shift)
            best = sorted(coins, key=lambda c: sum(coins[c]), reverse=True)[:2]
            others = [v for c, got in coins.items() if c not in best for v in got]
            if not others or np.mean(others) <= REJECT_MEAN:
                tally["inconclusive"] += 1
                tally["blocked_by_two_best_coins"] += 1
            else:
                tally["passed"] += 1
        out[label] = {"mean_pct": round(float(target), 2), **{k: round(v / draws, 3) for k, v in tally.items()}}
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(OUTPUT))
    args = ap.parse_args()
    output = Path(args.output)
    market = Market()
    print("panel last day", market.close.index.max().date(), flush=True)
    u, k = unlock_events(market), krw_events(market)
    print("events: U", len(u), "from", u.source.value_counts().to_dict(), "| K", len(k), "from", k.source.value_counts().to_dict(), flush=True)
    events = pd.concat([u, k], ignore_index=True)
    trades = build_trades(market, events)
    frame = pd.DataFrame([{key: value for key, value in x.items() if key not in ("daily", "hedge_daily")} for x in trades])
    first = pd.Timestamp("2022-11-01")
    full = frame[frame.entry_day >= first]
    report: dict = {"data_end": str(DATA_END.date()), "panel_last_day": str(market.close.index.max().date()), "events": {"U": len(u), "K": len(k)}}
    for label, part in (("all_from_2022_11", full), ("dev", full[full["sample"] == "dev"]), ("holdout", full[full["sample"] == "holdout"]), ("forward_era", full[full.forward_era])):
        report[label] = stats(part)
        report[label]["by_stream"] = {s: stats(g) for s, g in part.groupby("stream")}
    report["before_2022_11"] = stats(frame[frame.entry_day < first]) if (frame.entry_day < first).any() else None
    by_year = full.groupby(full.entry_day.dt.year)
    report["by_year_trade_mean_pct"] = {str(y): round(g.hedged.mean() * 100, 2) for y, g in by_year}
    report["account"] = {}
    pick = lambda mask: [x for x, keep in zip(trades, mask) if keep]
    for label, mask, start in (("holdout", frame["sample"] == "holdout", pd.Timestamp("2025-01-01")),
                               ("dev", (frame["sample"] == "dev") & (frame.entry_day >= first), first)):
        end = DATA_END if label == "holdout" else DEV_END + 8 * DAY
        report["account"][label] = perf(account(pick(mask.tolist()), start, end))
    regime = pd.cut(full.prior60, [-9, -0.15, 0.15, 99], labels=["down", "flat", "up"])
    report["by_regime"] = {f"{s}_{r}": {"n": len(g), "mean_pct": round(g.hedged.mean() * 100, 2)} for (s, r), g in full.groupby(["stream", regime], observed=True)}
    months = (full.entry_day.max() - full.entry_day.min()).days / 30.44
    recent = full[full.entry_day >= pd.Timestamp("2025-01-01")]
    report["pace"] = {"batches_per_month_all": round(full.entry_day.nunique() / months, 2),
                      "batches_per_month_since_2025": round(recent.entry_day.nunique() / ((recent.entry_day.max() - recent.entry_day.min()).days / 30.44), 2),
                      "up_regime_u10_share": round(float((full[full.stream == "U"].prior60 > 0.15).mean()), 3)}
    report["operating_characteristics_30_batches"] = {"all_history": operating(full), "since_2025": operating(recent)}
    output.mkdir(parents=True, exist_ok=True)
    frame.drop(columns=["exit_time"]).assign(exit_time=frame.exit_time.astype(str)).to_csv(output / "development_reference.csv", index=False, float_format="%.8g", lineterminator="\n")
    (output / "development_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report, ensure_ascii=False, indent=1, default=str), flush=True)


if __name__ == "__main__":
    main()
