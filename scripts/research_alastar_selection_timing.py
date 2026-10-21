"""Round-4 Alastar checks: coin selection vs the market, family-wise horizon selection, event path.

The 10/21 review (scripts/review_telegram_signals.py) asked one question: given the
coin, was the author's *timing* better than random hours on that coin? This script
asks the questions it left open, on the same frozen signal set and local data:

1. Selection: at the SAME entry hour, same side and horizon, did the named coin beat
   an equal-weight alt index and volume-matched peers? And did the author's direction
   beat two mechanical rules on the same coin/hour (7d momentum, 48h reversal)?
2. Family-wise: the earlier "3-day" candidate was picked after looking at many
   horizons. A Westfall-Young max-z test over every kind x side x horizon cell,
   using pseudo-signals at random hours on the same coin, gives adjusted p-values.
3. Event path: mean side-signed abnormal return from 48h before to 168h after the
   post (hourly), plus a 5m delay-decay curve for fresh calls (official 5m bars).
4. Exact reconciliation: the response study's own broad_directional|d15|h72 trades
   (5m entry, real funding) against the same random-hour null.

Read-only: local parquet + the response study's downloaded 5m bars. No network,
no AI service, no orders.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.review_telegram_signals import (  # noqa: E402
    BASE_COST, FIXED_CUTOFF, HORIZONS, SymbolPanel, dedup_independent, entry_hour, load_signals,
)
from scripts.research_telegram_signal_strategy import LocalMarket  # noqa: E402

RESPONSE = ROOT / "data/research/alastar_response_20261021"
DATA_OUT = ROOT / "data/research/alastar_selection_timing_20261021"
REPORT_OUT = ROOT / "reports/alastar_selection_timing_20261021"
UNIVERSE_START = pd.Timestamp("2026-01-01", tz="UTC")
MIN_LISTED_HOURS = 14 * 24
VOLUME_BAND = 3.0          # matched peers: trailing 7d quote volume within [1/3x, 3x]
DRAWS = 5000
FW_DRAWS = 2000
PSEUDO_WINDOW = 30 * 24
PSEUDO_GAP = 168           # pseudo entry at least one max-horizon away from the real post
SEED = 20261021


def load_universe(market: LocalMarket) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Wide hourly opens and quote volumes for every local perpetual since UNIVERSE_START."""
    opens, qvol = {}, {}
    for directory in sorted((ROOT / "data/historical/binance_futures").iterdir()):
        if not directory.is_dir() or not directory.name.endswith("_USDT"):
            continue
        symbol = directory.name[:-5] + "USDT"
        frame = market.frame(symbol)
        if frame.empty:
            continue
        frame = frame[frame.index >= UNIVERSE_START - pd.Timedelta(days=8)]
        bad = (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
        frame = frame[~bad]
        if len(frame) < 48:
            continue
        opens[symbol] = frame["open"].astype(float)
        qvol[symbol] = (frame["volume"] * frame["close"]).astype(float)
    grid = pd.date_range(min(s.index.min() for s in opens.values()), max(s.index.max() for s in opens.values()), freq="h")
    return pd.DataFrame(opens).reindex(grid), pd.DataFrame(qvol).reindex(grid)


def forward_returns(opens: pd.DataFrame, hours: int) -> pd.DataFrame:
    """Open-to-open simple return from each start hour, NaN if either end is missing."""
    return opens.shift(-hours) / opens - 1.0


def alt_index(fwd: pd.DataFrame, exclude_majors: bool = True) -> pd.Series:
    """Equal-weight cross-sectional mean, winsorised at 1/99% per hour so one listing pump cannot dominate."""
    frame = fwd.drop(columns=[c for c in ("BTCUSDT", "ETHUSDT") if exclude_majors and c in fwd])
    lo, hi = frame.quantile(.01, axis=1), frame.quantile(.99, axis=1)
    return frame.clip(lo, hi, axis=0).mean(axis=1)


def selection_rows(signals: list[dict], opens: pd.DataFrame, qvol: pd.DataFrame) -> list[dict]:
    """Per signal and horizon: own net, peer means at the same hour, and two mechanical direction rules."""
    listed = opens.notna().cumsum()
    vol7 = qvol.rolling(7 * 24, min_periods=72).sum().shift(1)  # strictly trailing
    past48 = opens / opens.shift(48) - 1.0
    past168 = opens / opens.shift(168) - 1.0
    rows = []
    for hours in HORIZONS:
        fwd = forward_returns(opens, hours)
        alt = alt_index(fwd)
        btc = fwd["BTCUSDT"]
        for s in signals:
            at = entry_hour(s["published_at"])
            if at not in fwd.index or s["symbol"] not in fwd:
                continue
            own = fwd.at[at, s["symbol"]]
            if not np.isfinite(own):
                continue
            side = s["side"]
            peers = fwd.loc[at].drop(s["symbol"])
            peers = peers[np.isfinite(peers) & (listed.loc[at, peers.index] >= MIN_LISTED_HOURS)]
            own_vol = vol7.at[at, s["symbol"]]
            pv = vol7.loc[at, peers.index]
            matched = peers[(pv >= own_vol / VOLUME_BAND) & (pv <= own_vol * VOLUME_BAND)] if np.isfinite(own_vol) else peers.iloc[:0]

            def signed(r, d=side):
                return np.maximum(-1.0, d * np.asarray(r, float)) - BASE_COST

            mom = np.sign(past168.at[at, s["symbol"]])
            rev = -np.sign(past48.at[at, s["symbol"]])
            rows.append({**s, "hours": hours, "entry_at": at,
                         "net": float(signed(own)),
                         "abn_alt": float(side * (own - alt.at[at])),
                         "abn_btc": float(side * (own - btc.at[at])) if np.isfinite(btc.at[at]) else np.nan,
                         "peer_all": float(signed(peers.to_numpy()).mean()) if len(peers) >= 5 else np.nan,
                         "peer_matched": float(signed(matched.to_numpy()).mean()) if len(matched) >= 5 else np.nan,
                         "n_peer_matched": len(matched),
                         "net_mom7d": float(signed(own, mom)) if mom in (1, -1) else np.nan,
                         "net_rev48h": float(signed(own, rev)) if rev in (1, -1) else np.nan,
                         "agree_mom7d": bool(mom == side), "agree_rev48h": bool(rev == side)})
    return rows


def paired_test(diff: np.ndarray, stamps: list[pd.Timestamp], draws: int = DRAWS, seed: int = SEED) -> dict:
    """Mean of a per-signal difference with a week-block bootstrap (keeps same-week clustering).

    The difference uses the signal coin's OWN variance; drawing peers as the null would
    understate it, because the named coins are far more volatile than a random perpetual.
    """
    weeks: dict[str, list[float]] = {}
    for value, stamp in zip(diff, stamps):
        weeks.setdefault(str((stamp - pd.Timedelta(days=stamp.weekday())).date()), []).append(float(value))
    sums = np.array([sum(v) for v in weeks.values()])
    counts = np.array([len(v) for v in weeks.values()])
    pick = np.random.default_rng(seed).integers(0, len(weeks), size=(draws, len(weeks)))
    boot = sums[pick].sum(1) / counts[pick].sum(1)
    return {"mean": float(diff.mean()), "ci95_lo": float(np.quantile(boot, .025)), "ci95_hi": float(np.quantile(boot, .975)),
            "p_le_zero": float((1 + (boot <= 0).sum()) / (draws + 1)), "weeks": len(weeks)}


def summarise_selection(rows: list[dict]) -> list[dict]:
    out = []
    for kind in ("ALL_DIRECTIONAL", "fresh_call", "followup_view", "context_opinion"):
        for side in ("all", "long", "short"):
            for hours in HORIZONS:
                for phase in ("all", "after_cutoff"):
                    base = [r for r in rows if r["hours"] == hours and (kind == "ALL_DIRECTIONAL" or r["kind"] == kind)
                            and (side == "all" or r["side"] == (1 if side == "long" else -1))
                            and (phase == "all" or r["entry_at"] >= FIXED_CUTOFF)]
                    kept = dedup_independent(base)
                    if len(kept) < 5:
                        continue
                    net = np.array([r["net"] for r in kept])
                    cell = {"kind": kind, "side": side, "hours": hours, "phase": phase, "n": len(kept),
                            "mean_net": float(net.mean()),
                            "mean_abn_vs_btc": float(np.nanmean([r["abn_btc"] for r in kept])),
                            "agree_mom7d": float(np.mean([r["agree_mom7d"] for r in kept])),
                            "agree_rev48h": float(np.mean([r["agree_rev48h"] for r in kept]))}
                    for name in ("abn_alt", "peer_matched", "net_mom7d", "net_rev48h"):
                        use = [r for r in kept if np.isfinite(r[name])]
                        if len(use) < 5:
                            continue
                        if name == "abn_alt":
                            diff = np.array([r["abn_alt"] for r in use])
                        elif name == "peer_matched":
                            diff = np.array([r["net"] - r["peer_matched"] for r in use])
                        else:  # author's direction minus the mechanical rule's direction on the same coin/hour
                            diff = np.array([r["net"] - r[name] for r in use])
                            cell[f"mean_{name}"] = float(np.mean([r[name] for r in use]))
                        test = paired_test(diff, [r["entry_at"] for r in use])
                        label = {"abn_alt": "vs_alt_index", "peer_matched": "vs_matched_peers",
                                 "net_mom7d": "vs_mom7d_rule", "net_rev48h": "vs_rev48h_rule"}[name]
                        cell.update({f"excess_{label}": test["mean"], f"ci_lo_{label}": test["ci95_lo"],
                                     f"ci_hi_{label}": test["ci95_hi"], f"p_{label}": test["p_le_zero"]})
                    out.append(cell)
    return out


KINDS = ("ALL_DIRECTIONAL", "fresh_call", "followup_view", "context_opinion")


def familywise(signals: list[dict], panels: dict[str, SymbolPanel], kinds: tuple = KINDS) -> list[dict]:
    """Westfall-Young single-step max-z over all kind x side x horizon cells (no dedup, events as posted).

    Each pseudo replicate moves every signal to ONE random hour on its own coin (same
    hour for every horizon, so cross-horizon correlation is kept), at least PSEUDO_GAP
    hours from the real post and within +/-PSEUDO_WINDOW.
    """
    rng = np.random.default_rng(SEED + 1)
    # Common index of signals that have a complete path for every horizon at the real hour.
    table = {}
    for hours in HORIZONS:
        for side in (1, -1):
            for sym in {s["symbol"] for s in signals}:
                g, f = panels[sym].outcomes(hours, side)
                table[(sym, hours, side)] = g - BASE_COST + np.nan_to_num(f)
    usable, pseudo_pos = [], []
    for s in signals:
        panel = panels[s["symbol"]]
        pos = panel.position(entry_hour(s["published_at"]))
        if pos is None:
            continue
        vals = [table[(s["symbol"], h, s["side"])] for h in HORIZONS]
        if not all(np.isfinite(v[pos]) for v in vals):
            continue
        idx = np.arange(max(0, pos - PSEUDO_WINDOW), min(len(vals[0]), pos + PSEUDO_WINDOW + 1))
        idx = idx[np.abs(idx - pos) >= PSEUDO_GAP]
        ok = np.logical_and.reduce([np.isfinite(v[idx]) for v in vals])
        idx = idx[ok]
        if len(idx) < 100:
            continue
        usable.append((s, pos))
        pseudo_pos.append(idx[rng.integers(0, len(idx), size=FW_DRAWS)])
    cells = []
    for kind in kinds:
        for side in ("all", "long", "short"):
            members = [i for i, (s, _) in enumerate(usable) if (kind == "ALL_DIRECTIONAL" or s["kind"] == kind)
                       and (side == "all" or s["side"] == (1 if side == "long" else -1))]
            if len(members) >= 10:
                for hours in HORIZONS:
                    cells.append((kind, side, hours, members))
    obs = np.zeros(len(cells))
    pseudo = np.zeros((len(cells), FW_DRAWS))
    for c, (kind, side, hours, members) in enumerate(cells):
        real = [table[(usable[i][0]["symbol"], hours, usable[i][0]["side"])][usable[i][1]] for i in members]
        obs[c] = np.mean(real)
        pseudo[c] = np.mean([table[(usable[i][0]["symbol"], hours, usable[i][0]["side"])][pseudo_pos[i]] for i in members], axis=0)
    mu, sd = pseudo.mean(1), pseudo.std(1, ddof=1)
    z_obs = (obs - mu) / sd
    z_null_max = ((pseudo - mu[:, None]) / sd[:, None]).max(0)
    out = []
    for c, (kind, side, hours, members) in enumerate(cells):
        out.append({"kind": kind, "side": side, "hours": hours, "n_events": len(members),
                    "mean_net_events": float(obs[c]), "pseudo_mean": float(mu[c]), "excess": float(obs[c] - mu[c]),
                    "z": float(z_obs[c]), "p_raw_one_sided": float((1 + (pseudo[c] >= obs[c]).sum()) / (FW_DRAWS + 1)),
                    "p_familywise_maxz": float((1 + (z_null_max >= z_obs[c]).sum()) / (FW_DRAWS + 1))})
    return out


def event_path(signals: list[dict], opens: pd.DataFrame) -> pd.DataFrame:
    """Mean side-signed log path relative to the entry-hour open, minus the alt index path."""
    offsets = np.arange(-48, 169)
    alt_log = np.log(opens.drop(columns=["BTCUSDT", "ETHUSDT"], errors="ignore"))
    alt_step = alt_log.diff().clip(-.5, .5).mean(axis=1).fillna(0).cumsum()  # equal-weight hourly index
    rows = []
    for s in signals:
        at = entry_hour(s["published_at"])
        if s["symbol"] not in opens or at not in opens.index:
            continue
        i = opens.index.get_loc(at)
        if i + offsets[0] < 0 or i + offsets[-1] >= len(opens):
            continue
        p = np.log(opens[s["symbol"]].to_numpy(float)[i + offsets])
        a = alt_step.to_numpy(float)[i + offsets]
        if not np.isfinite(p[48]):
            continue
        rows.append({"kind": s["kind"], "side": s["side"],
                     "raw": s["side"] * (p - p[48]), "abn": s["side"] * ((p - p[48]) - (a - a[48]))})
    out = []
    for kind in ("ALL_DIRECTIONAL", "fresh_call"):
        sel = [r for r in rows if kind == "ALL_DIRECTIONAL" or r["kind"] == kind]
        raw = np.array([r["raw"] for r in sel])
        abn = np.array([r["abn"] for r in sel])
        for j, off in enumerate(offsets):
            col = abn[:, j][np.isfinite(abn[:, j])]
            out.append({"kind": kind, "offset_h": int(off), "n": len(col),
                        "mean_signed_log_raw": float(np.nanmean(raw[:, j])),
                        "mean_signed_log_abn": float(col.mean()) if len(col) else np.nan,
                        "se_abn": float(col.std(ddof=1) / np.sqrt(len(col))) if len(col) > 1 else np.nan})
    return pd.DataFrame(out)


def delay_decay(signals: list[dict]) -> pd.DataFrame:
    """Fresh calls on official 5m bars: 60-minute signed return starting at pub+delay, and the 60m before the post."""
    from scripts.research_alastar_responses import Market
    market = Market()
    delays = (1, 3, 5, 10, 15, 30, 60, 120, 240)
    rows = []
    for s in signals:
        frame = market.prices_for(s["symbol"])
        if frame.empty:
            continue
        o = frame["open"]
        pub = pd.Timestamp(s["published_at"]).tz_convert("UTC")
        rec = {"signal_id": s["signal_id"], "kind": s["kind"], "side": s["side"], "symbol": s["symbol"]}
        pre_a, pre_b = (pub - pd.Timedelta(minutes=60)).ceil("5min"), pub.floor("5min")
        if pre_a in o.index and pre_b in o.index:
            rec["pre_60m_signed"] = s["side"] * (o[pre_b] / o[pre_a] - 1)
        for d in delays:
            a = (pub + pd.Timedelta(minutes=d)).ceil("5min")
            b = a + pd.Timedelta(minutes=60)
            if a in o.index and b in o.index:
                rec[f"d{d}_60m_net"] = max(-1.0, s["side"] * (o[b] / o[a] - 1)) - BASE_COST
        rows.append(rec)
    return pd.DataFrame(rows)


def draw_p(observed: np.ndarray, pools: list[np.ndarray], draws: int = DRAWS, seed: int = SEED) -> tuple[float, float]:
    """Same-coin random-hour null: one draw per signal; (null mean, one-sided p that observed mean is larger)."""
    rng = np.random.default_rng(seed)
    means = np.zeros(draws)
    for pool in pools:
        means += pool[rng.integers(0, len(pool), size=draws)]
    means /= len(pools)
    return float(means.mean()), (1 + int((means >= observed.mean()).sum())) / (draws + 1)


def reconcile_response_candidate(panels: dict[str, SymbolPanel]) -> list[dict]:
    """The response study's own trades for broad_directional|d15|h72 (and |h24) vs a random-hour null on the same coin."""
    out = []
    trades = [json.loads(line) for line in (RESPONSE / "admitted_trades.jsonl").read_text(encoding="utf-8").splitlines()]
    for scenario in ("broad_directional|d15|h72", "broad_directional|d15|h24", "fresh_view|d15|h1", "fresh_view|d15|h72"):
        hours = int(float(scenario.split("|h")[1]))
        for phase in ("all", "later"):
            sel = [t for t in trades if t["scenario"] == scenario and t["status"] == "ok"
                   and (phase == "all" or t["analysis_phase"] == "later")]
            sel = list({t["event_id"]: t for t in sel}.values())
            for window in (7 * 24, 30 * 24, "pre_only"):
                obs, nulls = [], []
                for t in sel:
                    if t["symbol"] not in panels:
                        continue
                    panel = panels[t["symbol"]]
                    pos = panel.position(pd.Timestamp(t["entry_at"]).floor("h"))
                    if pos is None:
                        continue
                    g, f = panel.outcomes(hours, int(t["direction"]))
                    v = g - BASE_COST + np.nan_to_num(f)
                    if window == "pre_only":
                        idx = np.arange(max(0, pos - 30 * 24), max(0, pos - hours))
                    else:
                        idx = np.arange(max(0, pos - window), min(len(v), pos + window + 1))
                        idx = idx[np.abs(idx - pos) >= hours]
                    idx = idx[np.isfinite(v[idx])]
                    if len(idx) < 50:
                        continue
                    obs.append(t["net_return"])
                    nulls.append(v[idx])
                if len(obs) < 5:
                    continue
                null_mean, p = draw_p(np.array(obs), nulls)
                out.append({"scenario": scenario, "phase": phase, "null_window": str(window), "n": len(obs),
                            "mean_net_response_study": float(np.mean(obs)), "null_mean": null_mean,
                            "excess": float(np.mean(obs) - null_mean), "p_greater": p})
    return out


def main() -> None:
    DATA_OUT.mkdir(parents=True, exist_ok=True)
    REPORT_OUT.mkdir(parents=True, exist_ok=True)
    signals, _ = load_signals()
    signals = [s for s in signals if s["channel"] == "alastartrades"]
    market = LocalMarket()
    panels = {s: SymbolPanel(market.frame(s), market.frame(s, "funding_rate")) for s in {x["symbol"] for x in signals}}
    opens, qvol = load_universe(market)
    print("universe", opens.shape)

    sel = summarise_selection(selection_rows(signals, opens, qvol))
    pd.DataFrame(sel).to_csv(REPORT_OUT / "selection_vs_peers.csv", index=False, encoding="utf-8-sig")
    fw = familywise(signals, panels)
    pd.DataFrame(fw).to_csv(REPORT_OUT / "familywise_timing.csv", index=False, encoding="utf-8-sig")
    rec = reconcile_response_candidate(panels)
    pd.DataFrame(rec).to_csv(REPORT_OUT / "response_candidate_vs_null.csv", index=False, encoding="utf-8-sig")
    path_df = event_path(signals, opens)
    path_df.to_csv(DATA_OUT / "event_path.csv", index=False, encoding="utf-8-sig")
    decay = delay_decay([s for s in signals if s["kind"] in ("fresh_call", "followup_view", "context_opinion")])
    decay.to_csv(DATA_OUT / "delay_decay_5m.csv", index=False, encoding="utf-8-sig")
    notes = {"signals": len(signals), "universe_symbols": int(opens.shape[1]), "base_cost": BASE_COST,
             "volume_band": VOLUME_BAND, "min_listed_hours": MIN_LISTED_HOURS, "draws": DRAWS, "fw_draws": FW_DRAWS,
             "pseudo_window_h": PSEUDO_WINDOW, "pseudo_gap_h": PSEUDO_GAP, "local_data_last": str(opens.index.max())}
    (DATA_OUT / "run_notes.json").write_text(json.dumps(notes, indent=2), encoding="utf-8")
    print(json.dumps(notes, indent=1))


if __name__ == "__main__":
    main()
