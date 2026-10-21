"""Independent review of Telegram channel signals against a same-coin random-time null.

Re-uses the frozen, already-labelled signal sets (Alastar response events and the
rule-based CryptoQuant alert mapping). Never calls an AI service, Telegram, or an
order endpoint; reads only local Binance perpetual 1h bars and funding history.

For every signal the null distribution is the SAME coin, SAME side and SAME
holding period entered at random hourly opens within +/-30 days (excluding
windows overlapping the signal itself). Excess over that null measures timing
skill beyond "this coin happened to trend that month". All horizons are reported;
none is selected.
"""
from __future__ import annotations

import json
import math
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.research_alastar_responses import all_events, exclusion  # noqa: E402
from scripts.research_telegram_signal_strategy import LocalMarket, cryptoquant_signal, read_jsonl  # noqa: E402

DATA_OUT = ROOT / "data/research/telegram_signal_review_20261021"
REPORT_OUT = ROOT / "reports/telegram_signal_review_20261021"
HORIZONS = (1, 4, 24, 72, 168)
BASE_COST = .002          # previous studies: 10bps fee + 10bps slippage round trip
SMALLCAP_COST = .008      # listing_short_tracker: 30bps round trip + 50bps entry slippage
NULL_WINDOW_HOURS = 30 * 24
NULL_DRAWS = 5000
FIXED_CUTOFF = pd.Timestamp("2026-07-17T06:29:15Z")  # same boundary as the earlier studies
HOUR = pd.Timedelta(hours=1)


def entry_hour(published) -> pd.Timestamp:
    """Next full hourly open at least 60 seconds after publication."""
    stamp = pd.Timestamp(published)
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return (stamp + pd.Timedelta(seconds=60)).ceil("h")


def side_return(entry: float, exit_: float, side: int) -> float:
    """Long: (exit-entry)/entry. Short: (entry-exit)/entry. 1x collateral floor at -100%."""
    if side < 0:
        return max(-1.0, (entry - exit_) / entry)
    return max(-1.0, (exit_ - entry) / entry)


class SymbolPanel:
    """Hourly arrays on a gap-explicit grid, so a missing bar is never filled."""

    def __init__(self, bars: pd.DataFrame, funding: pd.DataFrame | None):
        if bars.empty:
            self.index = pd.DatetimeIndex([], tz="UTC")
            self.open = self.high = self.low = self.fund = np.array([])
            return
        self.index = pd.date_range(bars.index.min(), bars.index.max(), freq="h")
        frame = bars.reindex(self.index)
        bad = ~np.isfinite(frame[["open", "high", "low", "close"]].to_numpy(float)).all(axis=1)
        bad |= (frame[["open", "high", "low", "close"]] <= 0).any(axis=1).to_numpy()
        self.open = np.where(bad, np.nan, frame["open"].to_numpy(float))
        self.high = np.where(bad, np.nan, frame["high"].to_numpy(float))
        self.low = np.where(bad, np.nan, frame["low"].to_numpy(float))
        self.fund = np.zeros(len(self.index))
        self.funding_known = np.zeros(len(self.index), dtype=bool)
        if funding is not None and not funding.empty and "funding_rate" in funding:
            rates = funding["funding_rate"].astype(float)
            hours = rates.index.floor("h")
            # Settlement at hh:00:00.00x is paid by a position opened at that open.
            series = rates.groupby(hours).sum().reindex(self.index)
            self.fund = np.nan_to_num(series.to_numpy(float))
            first, last = rates.index.min().floor("h"), rates.index.max().floor("h")
            self.funding_known = (self.index >= first) & (self.index <= last)

    def position(self, at: pd.Timestamp) -> int | None:
        if len(self.index) == 0 or at < self.index[0] or at > self.index[-1]:
            return None
        return int((at - self.index[0]) / HOUR)

    def outcomes(self, hours: int, side: int) -> tuple[np.ndarray, np.ndarray]:
        """Gross side return (with 1x short-liquidation floor) and funding for every start hour."""
        n = len(self.index)
        gross = np.full(n, np.nan)
        funding = np.full(n, np.nan)
        if n <= hours:
            return gross, funding
        o = self.open
        entry, exit_ = o[:n - hours], o[hours:]
        window_ok = np.ones(n - hours, dtype=bool)
        worst_high = np.full(n - hours, -np.inf)
        cum_f = np.concatenate([[0.0], np.cumsum(self.fund)])
        for k in range(hours):  # hours <= 168, arrays small enough
            seg_h = self.high[k:n - hours + k]
            window_ok &= np.isfinite(seg_h) & np.isfinite(self.low[k:n - hours + k])
            worst_high = np.maximum(worst_high, np.nan_to_num(seg_h, nan=-np.inf))
        window_ok &= np.isfinite(entry) & np.isfinite(exit_)
        with np.errstate(invalid="ignore", divide="ignore"):
            raw = side * (exit_ / entry - 1.0)
            if side < 0:
                raw = np.where(worst_high >= 2.0 * entry, -1.0, raw)
        raw = np.maximum(-1.0, raw)
        gross[:n - hours] = np.where(window_ok, raw, np.nan)
        paid = cum_f[hours:n] - cum_f[:n - hours]
        known = self.funding_known[:n - hours] & self.funding_known[hours - 1:n - 1]
        funding[:n - hours] = np.where(window_ok & known, -side * paid, np.nan)
        return gross, funding


def null_candidates(pos: int, hours: int, valid: np.ndarray, window: int = NULL_WINDOW_HOURS) -> np.ndarray:
    """Valid start hours within +/-window that do not overlap the signal's own holding window."""
    lo, hi = max(0, pos - window), min(len(valid), pos + window + 1)
    idx = np.arange(lo, hi)
    idx = idx[np.abs(idx - pos) >= hours]
    return idx[valid[idx]]


def dedup_independent(rows: list[dict]) -> list[dict]:
    """Keep a signal only if no earlier kept position on the same coin is still open."""
    kept: list[dict] = []
    open_until: dict[str, pd.Timestamp] = {}
    for row in sorted(rows, key=lambda r: (r["entry_at"], r["signal_id"])):
        until = open_until.get(row["symbol"])
        if until is not None and row["entry_at"] < until:
            continue
        kept.append(row)
        open_until[row["symbol"]] = row["entry_at"] + row["hours"] * HOUR
    return kept


def bh_fdr(pvalues: list[float]) -> list[float]:
    """Benjamini-Hochberg adjusted q-values (same order as input)."""
    p = np.asarray(pvalues, float)
    m = len(p)
    if m == 0:
        return []
    order = np.argsort(p)
    ranked = p[order] * m / np.arange(1, m + 1)
    q = np.minimum.accumulate(ranked[::-1])[::-1]
    out = np.empty(m)
    out[order] = np.minimum(q, 1.0)
    return out.tolist()


def permutation_test(observed: np.ndarray, nulls: list[np.ndarray], draws: int = NULL_DRAWS,
                     seed: int = 20261021) -> dict:
    """Draw one null trade per signal, compare the observed mean with the null-mean distribution."""
    rng = np.random.default_rng(seed)
    means = np.zeros(draws)
    for values in nulls:
        means += values[rng.integers(0, len(values), size=draws)]
    means /= len(nulls)
    obs = float(observed.mean())
    greater = (1 + int((means >= obs).sum())) / (draws + 1)
    less = (1 + int((means <= obs).sum())) / (draws + 1)
    return {"null_mean": float(means.mean()), "null_p05": float(np.quantile(means, .05)),
            "null_p95": float(np.quantile(means, .95)), "p_greater": greater,
            "p_two_sided": min(1.0, 2 * min(greater, less))}


def week_bootstrap(values: np.ndarray, stamps: list[pd.Timestamp], draws: int = 2000) -> list[float] | None:
    weeks: dict[str, list[float]] = {}
    for value, stamp in zip(values, stamps):
        weeks.setdefault(str((stamp - pd.Timedelta(days=stamp.weekday())).date()), []).append(float(value))
    if len(weeks) < 8:
        return None
    sums = np.array([sum(v) for v in weeks.values()])
    counts = np.array([len(v) for v in weeks.values()])
    pick = np.random.default_rng(705).integers(0, len(weeks), size=(draws, len(weeks)))
    return np.quantile(sums[pick].sum(1) / counts[pick].sum(1), [.025, .975]).tolist()


def book_drawdown(rows: list[dict], panels: dict[str, SymbolPanel], cost: float) -> float | None:
    """20% initial notional per position, hourly mark-to-market at opens, fees half at entry/exit."""
    if not rows:
        return None
    start = min(r["entry_at"] for r in rows)
    end = max(r["entry_at"] + r["hours"] * HOUR for r in rows)
    grid = pd.date_range(start, end, freq="h")
    equity = np.ones(len(grid))
    for r in rows:
        panel = panels[r["symbol"]]
        p = panel.position(r["entry_at"])
        prices = panel.open[p:p + r["hours"] + 1]
        path = np.maximum(-1.0, r["side"] * (prices / prices[0] - 1.0)) - cost / 2
        path[-1] = r["net"]
        i = grid.searchsorted(r["entry_at"])
        equity[i:i + len(path)] += .2 * path
        equity[i + len(path):] += .2 * r["net"]
    return float((equity / np.maximum.accumulate(equity) - 1).min())


def load_signals() -> tuple[list[dict], dict]:
    notes: dict = {}
    signals: list[dict] = []
    events = all_events()
    reasons = Counter()
    groups: dict[tuple, set] = {}
    for e in events:
        groups.setdefault((e["source_id"], e["symbol"]), set()).add(e["direction"])
    for e in events:
        reason = exclusion(e)
        if reason is None and e.get("evidence_type"):
            reason = "image_only_interpretation"
        if reason is None and e["reaction_type"] == "price_condition":
            reason = "conditional_needs_trigger_model"
        if reason is None and len(groups[(e["source_id"], e["symbol"])]) > 1:
            reason = "opposed_directions_in_same_message"
        if reason:
            reasons[reason if len(reason) < 60 else "other_label_exclusion"] += 1
            continue
        if e["reaction_type"] == "immediate_view":
            kind = "fresh_call" if e["is_fresh_entry_view"] else "followup_view"
        else:
            kind = "context_opinion"
        signals.append({"signal_id": e["event_id"], "channel": "alastartrades", "kind": kind,
                        "symbol": e["symbol"], "side": int(e["direction"]), "published_at": e["published_at"]})
    notes["alastar_events_total"] = len(events)
    notes["alastar_excluded"] = dict(reasons)
    cq = 0
    for message in read_jsonl(ROOT / "data/research/cryptoquant_alert_20261020/messages.jsonl"):
        s = cryptoquant_signal(message)
        if s is None:
            continue
        cq += 1
        signals.append({"signal_id": s["source_id"], "channel": "cryptoquant_alert", "kind": s["kind"],
                        "symbol": s["symbol"], "side": int(s["direction"]), "published_at": s["published_at"]})
    notes["cryptoquant_signals"] = cq
    return signals, notes


def summarise(rows: list[dict], panels: dict[str, SymbolPanel]) -> dict:
    net = np.array([r["net"] for r in rows])
    net_sc = np.array([r["net_smallcap"] for r in rows])
    excess = np.array([r["net"] - r["null_mean"] for r in rows])
    perm = permutation_test(net, [r["null_values"] for r in rows])
    profit = Counter()
    for r in rows:
        profit[r["symbol"]] += r["net"]
    total_pos = sum(v for v in net if v > 0)
    sigma = float(excess.std(ddof=1)) if len(rows) > 1 else float("nan")
    return {
        "n_independent": len(rows), "hit_rate": float((net > 0).mean()),
        "null_hit_rate": float(np.mean([r["null_hit"] for r in rows])),
        "mean_net_20bps": float(net.mean()), "median_net_20bps": float(np.median(net)),
        "mean_net_80bps": float(net_sc.mean()),
        "mean_without_best": float(np.delete(net, net.argmax()).mean()) if len(net) > 1 else None,
        "null_mean_net_20bps": float(np.mean([r["null_mean"] for r in rows])),
        "mean_excess_vs_null": float(excess.mean()), "excess_sd": sigma,
        "excess_week_bootstrap_95": week_bootstrap(excess, [r["entry_at"] for r in rows]),
        "p_greater_than_null": perm["p_greater"], "p_two_sided": perm["p_two_sided"],
        "null_mean_p05": perm["null_p05"], "null_mean_p95": perm["null_p95"],
        "max_drawdown_20pct_book": book_drawdown(rows, panels, BASE_COST),
        "top_symbol": max(profit, key=profit.get) if profit else None,
        "top_symbol_share_of_gross_profit": (max(profit.values()) / total_pos) if total_pos > 0 and len(profit) > 1 else None,
        "funding_missing": int(sum(r["funding_missing"] for r in rows)),
        "liquidations_1x": int(sum(r["liquidated"] for r in rows)),
        "n_for_power80_at_1pct": math.ceil(((1.96 + 0.8416) * sigma / .01) ** 2) if sigma == sigma else None,
    }


def evaluate_signals(signals: list[dict], panels: dict[str, SymbolPanel],
                     window: int = NULL_WINDOW_HOURS) -> tuple[list[dict], Counter]:
    outcome_cache: dict[tuple, tuple] = {}
    evaluated: list[dict] = []
    status = Counter()
    for hours in HORIZONS:
        for s in signals:
            panel = panels[s["symbol"]]
            key = (s["symbol"], hours, s["side"])
            if key not in outcome_cache:
                outcome_cache[key] = panel.outcomes(hours, s["side"])
            gross, funding = outcome_cache[key]
            at = entry_hour(s["published_at"])
            pos = panel.position(at)
            if pos is None or not np.isfinite(gross[pos]):
                status[(s["channel"], hours, "no_complete_price_path")] += 1
                continue
            candidates = null_candidates(pos, hours, np.isfinite(gross), window)
            if len(candidates) < 50:
                status[(s["channel"], hours, "null_sample_below_50")] += 1
                continue
            fund = 0.0 if not np.isfinite(funding[pos]) else float(funding[pos])
            null_values = gross[candidates] - BASE_COST + np.nan_to_num(funding[candidates])
            evaluated.append({**s, "hours": hours, "entry_at": at, "gross": float(gross[pos]), "funding": fund,
                              "funding_missing": not np.isfinite(funding[pos]), "liquidated": bool(gross[pos] <= -1.0),
                              "net": max(-1.0, float(gross[pos]) - BASE_COST + fund),
                              "net_smallcap": max(-1.0, float(gross[pos]) - SMALLCAP_COST + fund),
                              "null_values": null_values, "null_mean": float(null_values.mean()),
                              "null_hit": float((null_values > 0).mean()), "null_n": int(len(candidates))})
            status[(s["channel"], hours, "ok")] += 1
    return evaluated, status


def five_minute_check(signals: list[dict], hours: int) -> dict | None:
    """Sensitivity: official 5m bars from the response study, entry at the next 5m open >= pub+60s.

    Those downloads only cover about [publication-1h, publication+194h], so the null
    here is the same coin/side at other 5m starts inside that window (mostly AFTER
    the post) -- weaker than the +/-30d hourly null. Funding is ignored (<= 1 settlement).
    """
    from scripts.research_alastar_responses import Market
    market = Market()
    rows = []
    step = pd.Timedelta(minutes=5)
    for s in signals:
        frame = market.prices_for(s["symbol"])
        if frame.empty:
            continue
        at = (pd.Timestamp(s["published_at"]).tz_convert("UTC") + pd.Timedelta(seconds=60)).ceil("5min")
        grid = pd.date_range(frame.index.min(), frame.index.max(), freq=step)
        o = frame["open"].reindex(grid).to_numpy(float)
        hi = frame["high"].reindex(grid).to_numpy(float)
        k = hours * 12
        if at not in grid:
            continue
        pos = grid.get_loc(at)

        def value(i: int) -> float:
            if i + k >= len(o) or not np.isfinite(o[i]) or not np.isfinite(o[i + k]) or not np.isfinite(hi[i:i + k]).all():
                return float("nan")
            if s["side"] < 0 and hi[i:i + k].max() >= 2 * o[i]:
                return -1.0 - BASE_COST
            return side_return(o[i], o[i + k], s["side"]) - BASE_COST

        v = value(pos)
        if not np.isfinite(v):
            continue
        lo, up = max(0, pos - 12), min(len(o) - k - 1, pos + 194 * 12)
        cand = [i for i in range(lo, up) if abs(i - pos) >= k]
        null = np.array([value(i) for i in cand])
        null = null[np.isfinite(null)]
        if len(null) >= 50:
            rows.append({**s, "hours": hours, "entry_at": at, "net": v, "null_values": null,
                         "null_mean": float(null.mean())})
    rows = dedup_independent(rows)
    if len(rows) < 2:
        return None
    net = np.array([r["net"] for r in rows])
    perm = permutation_test(net, [r["null_values"] for r in rows])
    return {"hours": hours, "n_independent": len(rows), "hit_rate": float((net > 0).mean()),
            "mean_net_20bps": float(net.mean()), "null_mean_net_20bps": float(np.mean([r["null_mean"] for r in rows])),
            "mean_excess_vs_null": float(np.mean([r["net"] - r["null_mean"] for r in rows])),
            "p_greater_than_null": perm["p_greater"], "p_two_sided": perm["p_two_sided"]}


def main() -> None:
    DATA_OUT.mkdir(parents=True, exist_ok=True)
    REPORT_OUT.mkdir(parents=True, exist_ok=True)
    signals, notes = load_signals()
    market = LocalMarket()
    panels = {s: SymbolPanel(market.frame(s), market.frame(s, "funding_rate")) for s in {x["symbol"] for x in signals}}
    evaluated, status = evaluate_signals(signals, panels)
    cells = []
    phases = {"all": lambda r: True, "before_cutoff": lambda r: r["entry_at"] < FIXED_CUTOFF,
              "after_cutoff": lambda r: r["entry_at"] >= FIXED_CUTOFF}
    for channel in ("alastartrades", "cryptoquant_alert"):
        kinds = sorted({r["kind"] for r in evaluated if r["channel"] == channel})
        groups = ["ALL_DIRECTIONAL"] + kinds
        for kind in groups:
            for side in ("all", "long", "short"):
                for hours in HORIZONS:
                    base = [r for r in evaluated if r["channel"] == channel and r["hours"] == hours
                            and (kind == "ALL_DIRECTIONAL" or r["kind"] == kind)
                            and (side == "all" or r["side"] == (1 if side == "long" else -1))]
                    for phase, keep in phases.items():
                        rows = dedup_independent([r for r in base if keep(r)])
                        cell = {"channel": channel, "kind": kind, "side": side, "hours": hours, "phase": phase,
                                "n_events": sum(1 for r in base if keep(r))}
                        if len(rows) >= 2:
                            cell.update(summarise(rows, panels))
                        else:
                            cell["n_independent"] = len(rows)
                        cells.append(cell)
    tested = [c for c in cells if c["phase"] == "all" and c.get("n_independent", 0) >= 10 and "p_two_sided" in c]
    for c, q in zip(tested, bh_fdr([c["p_two_sided"] for c in tested])):
        c["bh_q_two_sided"] = q
    sensitivity = []
    narrow, _ = evaluate_signals([x for x in signals if x["channel"] == "alastartrades"], panels, window=7 * 24)
    for kind in ("ALL_DIRECTIONAL", "fresh_call"):
        for hours in HORIZONS:
            rows = dedup_independent([r for r in narrow if r["hours"] == hours
                                      and (kind == "ALL_DIRECTIONAL" or r["kind"] == kind)])
            if len(rows) >= 2:
                sensitivity.append({"check": "null_window_7d", "kind": kind, "side": "all", "hours": hours,
                                    **{k: v for k, v in summarise(rows, panels).items()
                                       if k in ("n_independent", "hit_rate", "mean_net_20bps", "null_mean_net_20bps",
                                                "mean_excess_vs_null", "p_greater_than_null", "p_two_sided")}})
    fresh = [x for x in signals if x["channel"] == "alastartrades" and x["kind"] == "fresh_call"]
    for side_name, side in (("all", None), ("short", -1), ("long", 1)):
        for hours in (1, 4):
            result = five_minute_check([x for x in fresh if side is None or x["side"] == side], hours)
            if result:
                sensitivity.append({"check": "entry_5m_local_window_null", "kind": "fresh_call", "side": side_name, **result})
    pd.DataFrame(sensitivity).to_csv(REPORT_OUT / "sensitivity.csv", index=False, encoding="utf-8-sig")
    notes.update({"fdr_family_size": len(tested), "status": {"|".join(map(str, k)): v for k, v in status.items()},
                  "horizons": HORIZONS, "base_cost": BASE_COST, "smallcap_cost": SMALLCAP_COST,
                  "null": "same symbol, same side, same horizon, random hourly start within +/-30d, "
                          "excluding starts overlapping the signal window; funding included",
                  "entry": "next full 1h open >= publication + 60s", "cutoff": str(FIXED_CUTOFF)})
    frame = pd.DataFrame(cells)
    frame.to_csv(REPORT_OUT / "metrics_all_cells.csv", index=False, encoding="utf-8-sig")
    primary = frame[(frame["phase"] == "all") & frame["hit_rate"].notna()][[
        "channel", "kind", "side", "hours", "n_events", "n_independent", "hit_rate", "null_hit_rate",
        "mean_net_20bps", "mean_net_80bps", "mean_without_best", "null_mean_net_20bps", "mean_excess_vs_null",
        "excess_week_bootstrap_95", "p_greater_than_null", "p_two_sided", "bh_q_two_sided",
        "max_drawdown_20pct_book", "top_symbol", "top_symbol_share_of_gross_profit", "n_for_power80_at_1pct"]]
    primary.to_csv(REPORT_OUT / "primary_metrics.csv", index=False, encoding="utf-8-sig")
    (DATA_OUT / "run_notes.json").write_text(json.dumps(notes, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    trades = pd.DataFrame([{k: v for k, v in r.items() if k != "null_values"} for r in evaluated])
    trades.to_csv(DATA_OUT / "signal_outcomes.csv", index=False, encoding="utf-8-sig")
    print(json.dumps(notes, indent=1, ensure_ascii=False, default=str)[:3000])


if __name__ == "__main__":
    main()
