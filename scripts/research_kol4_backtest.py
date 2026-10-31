"""Backtest of the hand-labelled calls from four Telegram channels (Wise Advice, Rich Trader, Zetoshi, Market Dealer).

Input is the per-message labels written by reading every message (labels/*.jsonl, chart_levels.jsonl); this
script never parses message text. Rules are declared in RULES and frozen with the trade list before any price
is read. Execution reuses the bracket engine of research_kol_execution (1m/5m official klines, 10 bps per
side, funding, conservative intrabar order).

Policies
  A  as written    entry and levels from the opening post (and brackets posted within 10 minutes)
  B  follow author A plus the author's later instructions (close / partial / break-even / new TP or SL)
  C  direction     every call with a symbol and a side, market entry at activation, fixed 24h and 7d exits

Commands: build | freeze | simulate | nulls | report
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_kol_execution as E  # noqa: E402
import research_kol4_integrated as K  # noqa: E402

OUT = K.OUT
LABELS = OUT / "labels"
BT = OUT / "backtest"

RULES = {
    "activation": "published_at + 60s; if the post was edited more than 600s after publication, edited_at + 60s "
                  "(edited_at is the last edit, so this is conservative)",
    "fresh_false": "posts identified as proof reposts (same-second 'TP hit', later forward of an old call) are a "
                   "separate group 'repost'; never in the main results",
    "bracket_merge": "a later post that only adds levels to the same trade is merged into the opening post when it "
                     "comes within 10 minutes; later it is an update (policy B only)",
    "entry": "'m' = market at the first bar opening at/after activation; price list = limit legs, equal weight, valid "
             "72h, cancelled if TP1 trades first; numeric breakout/breakdown trigger = entry at the next 1h open after "
             "a 1h close beyond the level within 7 days (immediately if already beyond at activation)",
    "levels": "tp absolute; tp_pct (Rich) = return on margin at the stated leverage, 10x when none is stated; "
              "tp_pct_price/sl_pct_price = price percent; tp_r = multiples of the stop distance; tp_abs/sl_abs = "
              "absolute price moves; tp_pct_pos/sl_pct_pos = position percent at the stated leverage; Market Dealer "
              "chart targets (red dashed lines) when the text gives none; no stop is invented",
    "weights": "equal split over targets; 'open' targets keep one extra equal share riding to the stop or expiry; "
               "explicit tp1_close_frac when stated",
    "policy_A_needs_levels": "policy A/B only for calls with at least one target or stop; calls without levels are "
                             "measured by policy C only",
    "horizon": "14 days; 'scalp' calls 24 hours",
    "updates_B": "applied at the update's activation time: close = exit all at the next bar open; partial = exit "
                 "50%; break-even = stop to average entry; new targets/stop replace the old ones",
    "policy_C": "market entry at activation for every call with a symbol and a side; exit at the 1h close 24h and "
                "7d later; costs 2 x 10 bps; no stops",
    "nulls": "same coin, same side, same relative plan on local 1h bars started every 12h over the 30 days before "
             "(pre) and the 30 days after (post, starting +1 day); excess = signal minus null mean",
    "stats": "means per channel x policy with week-block bootstrap CI and p (4000 reps); BH over all cells",
}

TRADE = {"signal", "signal?", "cond", "conditional", "position", "position?", "add"}
UPDATE = {"update_close", "update_partial", "update_be", "update_tp", "update_sl"}
MERGE_WINDOW = pd.Timedelta(minutes=10)
TRIGGER_WINDOW = pd.Timedelta(days=7)


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def read_labels(slug: str) -> list[dict]:
    rows = [json.loads(x) for x in (LABELS / f"{slug}.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    return [r for r in rows if "id" in r]


def chart_levels() -> dict:
    out = {}
    path = LABELS / "chart_levels.jsonl"
    if path.exists():
        for x in path.read_text(encoding="utf-8").splitlines():
            r = json.loads(x)
            if "ch" in r:
                out[(r["ch"], float(r["id"]))] = r
    return out


def activation_of(msg: dict) -> tuple[pd.Timestamp, bool]:
    pub = pd.Timestamp(msg["published_at"])
    ed = pd.Timestamp(msg["edited_at"]) if msg.get("edited_at") else None
    if ed is not None and (ed - pub).total_seconds() > 600:
        return ed + pd.Timedelta(seconds=60), False
    return pub + pd.Timedelta(seconds=60), True


def side_of(d) -> int | None:
    return {"L": 1, "S": -1}.get(d)


def _num_list(x) -> list[float]:
    if x is None:
        return []
    return [float(v) for v in (x if isinstance(x, list) else [x])]


def _trigger_level(r: dict, side: int):
    for key in ("trigger_long" if side > 0 else "trigger_short", "trigger"):
        v = r.get(key)
        if isinstance(v, (int, float)):
            return float(v)
    if isinstance(r.get("trigger"), str) and "break" in r["trigger"] and isinstance(r.get("e"), list):
        return float(r["e"][0])
    return None


def build_trades() -> dict:
    charts = chart_levels()
    trades, skipped = [], Counter()
    for slug in K.CHANNELS:
        msgs = {int(k): v for k, v in K.load(slug).items()}
        recs = read_labels(slug)
        by_id = {str(r["id"]): r for r in recs}
        own: dict[str, dict] = {}
        for r in sorted(recs, key=lambda r: float(str(r["id"]).rstrip("abcdefghijklmnopqrstuvwxyz") or 0)):
            if r["c"] not in TRADE:
                continue
            base_id = int(float(str(r["id"]).rstrip("abcdefghijklmnopqrstuvwxyz")))
            msg = msgs.get(base_id)
            if msg is None:
                skipped["no_message"] += 1
                continue
            act, pit = activation_of(msg)
            parent = by_id.get(str(r.get("ref"))) if r.get("ref") is not None else None
            if r["c"].startswith("position") and parent is not None and str(parent["id"]) in own:
                skipped["author_fill_of_earlier_call"] += 1
                continue
            d = r.get("d")
            if (d in (None, "?")) and parent is not None:
                d = parent.get("d")
            sides = [1, -1] if d == "both" else [side_of(d)]
            if sides == [None]:
                skipped["no_direction"] += 1
                continue
            syms = [s.strip().upper() for s in str(r.get("s", "?")).split(",") if s.strip()]
            chart = charts.get((slug, float(str(r["id"]).rstrip("abcdefghijklmnopqrstuvwxyz"))))
            if syms in (["?"], []) and chart and chart.get("s"):
                syms = [chart["s"].upper()]
            if syms in (["?"], []):
                skipped["ticker_unknown"] += 1
                continue
            # a later post of the same trade that only adds levels: merge when within 10 minutes
            if r["c"] == "signal" and parent is not None and parent.get("c") in TRADE and str(parent["id"]) in own \
                    and (parent.get("s") or "").upper() == (r.get("s") or "").upper():
                host = own[str(parent["id"])]
                if act - pd.Timestamp(host["activation"]) <= MERGE_WINDOW:
                    for k in ("tp", "sl", "tp_r", "tp_abs", "sl_abs", "tp_pct", "lev", "tp_open"):
                        if r.get(k) is not None and not host["lv"].get(k):
                            host["lv"][k] = r[k]
                    host["merged"].append(r["id"])
                    continue
                # later bracket: an update for policy B
                host["updates"].append({"id": r["id"], "kind": "levels", "at": str(act),
                                        **{k: r[k] for k in ("tp", "sl") if r.get(k) is not None}})
                continue
            for s in syms:
                for side in sides:
                    lv = {k: r.get(k) for k in ("e", "tp", "sl", "tp_r", "tp_abs", "sl_abs", "tp_pct", "tp_pct_price",
                                                "sl_pct_price", "tp_pct_pos", "sl_pct_pos", "lev", "tp_open",
                                                "sl_r_implied", "tp1_close_frac", "be_after_tp1", "e_legs")
                          if r.get(k) is not None}
                    if len(syms) > 1 and not r.get("levels_each"):  # several coins: the levels belong to none of them
                        lv = {"e": "m"}
                    if r["c"] == "add" and parent is not None:
                        lv = {"e": "m", **{k: parent[k] for k in ("tp", "sl", "lev") if parent.get(k) is not None}}
                    if not any(lv.get(k) for k in ("tp", "tp_r", "tp_abs", "tp_pct", "tp_pct_price", "tp_pct_pos")) \
                            and chart and chart.get("tp") and len(syms) == 1:
                        lv["tp"] = chart["tp"]
                        lv["tp_from_chart"] = True
                    trig = _trigger_level(r, side) if r["c"] in ("cond", "conditional") else None
                    kind = "conditional_numeric" if trig is not None else (
                        "conditional_text" if r["c"] in ("cond", "conditional") else r["c"].rstrip("?"))
                    fresh = r.get("fresh")
                    t = {"channel": slug, "id": r["id"], "symbol": s, "side": side, "kind": kind,
                         "published_at": msg["published_at"], "activation": str(act), "point_in_time": pit,
                         "edit_lag_s": (pd.Timestamp(msg["edited_at"]) - pd.Timestamp(msg["published_at"])).total_seconds()
                         if msg.get("edited_at") else 0.0,
                         "group": "repost" if fresh is False else "main", "fresh": fresh, "quality": r.get("q"),
                         "trigger": trig, "horizon_h": 24 if r.get("horizon") == "scalp" else 24 * 14,
                         "lv": lv, "merged": [], "updates": [], "parent": r.get("ref") if r["c"] == "add" else None}
                    trades.append(t)
                    if s == (r.get("s") or "").upper() and side == sides[0]:
                        own[str(r["id"])] = t
        # updates by reference, applied in policy B
        for r in recs:
            if r["c"] not in UPDATE or r.get("ref") is None:
                continue
            refs = r["ref"] if isinstance(r["ref"], list) else [r["ref"]]
            msg = msgs.get(int(float(str(r["id"]).rstrip("abcdefghijklmnopqrstuvwxyz"))))
            if msg is None:
                continue
            act, _ = activation_of(msg)
            for ref in refs:
                host = own.get(str(ref))
                if host is None:
                    skipped["update_without_trade"] += 1
                    continue
                kind = {"update_close": "close", "update_partial": "partial", "update_be": "be",
                        "update_tp": "levels", "update_sl": "levels"}[r["c"]]
                host["updates"].append({"id": r["id"], "kind": kind, "at": str(act),
                                        **{k: r[k] for k in ("tp", "sl") if r.get(k) is not None}})
        # an add-on entry follows its parent's later instructions
        for t in trades:
            if t["channel"] == slug and t["parent"] is not None and str(t["parent"]) in own:
                t["updates"] += [dict(u) for u in own[str(t["parent"])]["updates"] if u["at"] > t["activation"]]
    for t in trades:
        t["updates"].sort(key=lambda u: u["at"])
    return {"trades": trades, "skipped": dict(skipped)}


def cmd_build(_a) -> None:
    BT.mkdir(parents=True, exist_ok=True)
    data = build_trades()
    E.write_json(BT / "trades_pre_price.json", data)
    c = Counter((t["channel"], t["group"], t["kind"]) for t in data["trades"])
    for k in sorted(c):
        print(k, c[k])
    print("skipped", data["skipped"])


def cmd_freeze(_a) -> None:
    files = {p.name: sha(p.read_bytes()) for p in sorted(LABELS.glob("*.jsonl"))}
    freeze = {"frozen_at_utc": pd.Timestamp.now(tz="UTC").isoformat(), "rules": RULES, "label_files": files,
              "trades_sha256": sha((BT / "trades_pre_price.json").read_bytes()),
              "script_sha256": sha(Path(__file__).read_bytes())}
    E.write_json(BT / "freeze.json", freeze)
    print(json.dumps(freeze, indent=1)[:1500])


# ---------------------------------------------------------------- plans

def levels_to_plan(t: dict, ref: float, mult: float) -> tuple[E.Plan | None, str]:
    """Absolute plan in contract units; ref = the price the order is placed against (contract units)."""
    lv, side = t["lv"], t["side"]
    e = lv.get("e", "m")
    legs = [] if (e == "m" or isinstance(e, str) or t["kind"] == "conditional_numeric") else [x * mult for x in _num_list(e)]
    entry_ref = float(np.mean(legs)) if legs else ref
    lev = lv.get("lev") if isinstance(lv.get("lev"), (int, float)) else None
    stop = lv.get("sl") * mult if isinstance(lv.get("sl"), (int, float)) else None
    if stop is None and isinstance(lv.get("sl_abs"), (int, float)):
        stop = entry_ref - side * lv["sl_abs"] * mult
    if stop is None and lv.get("sl_pct_price"):
        stop = entry_ref * (1 - side * _num_list(lv["sl_pct_price"])[0] / 100)
    if stop is None and isinstance(lv.get("sl_pct_pos"), (int, float)) and lev:
        stop = entry_ref * (1 - side * lv["sl_pct_pos"] / 100 / lev)
    targets = [x * mult for x in _num_list(lv.get("tp"))]
    if not targets and lv.get("tp_abs"):
        targets = [entry_ref + side * x * mult for x in _num_list(lv["tp_abs"])]
    if not targets and lv.get("tp_r") and stop is not None:
        risk = abs(entry_ref - stop)
        targets = [entry_ref + side * risk * r for r in _num_list(lv["tp_r"])]
    if not targets and lv.get("tp_pct"):
        targets = [entry_ref * (1 + side * p / 100 / (lev or 10.0)) for p in _num_list(lv["tp_pct"])]
    if not targets and lv.get("tp_pct_price"):
        targets = [entry_ref * (1 + side * p / 100) for p in _num_list(lv["tp_pct_price"])]
    if not targets and lv.get("tp_pct_pos") and lev:
        targets = [entry_ref * (1 + side * p / 100 / lev) for p in _num_list(lv["tp_pct_pos"])]
    if stop is None and isinstance(lv.get("sl_r_implied"), dict):
        imp = lv["sl_r_implied"]
        stop = entry_ref - side * abs(entry_ref - imp["tp"] * mult) / imp["r"]
    if not targets and stop is None:
        return None, "no_levels"
    targets = sorted(targets, key=lambda x: side * x)
    market = not legs
    if any(side * (tp - entry_ref) <= 0 for tp in targets):
        return None, "stale_target_passed" if market else "target_on_wrong_side_of_entry"
    if stop is not None and side * (entry_ref - stop) <= 0:
        return None, "stale_stop_passed" if market else "stop_on_wrong_side_of_entry"
    # a limit already through the market fills at the market: judge the stop from there (post-freeze fix)
    if legs and any(side * (ref - x) < 0 for x in legs) and stop is not None and side * (ref - stop) <= 0:
        return None, "stale_stop_passed"
    if lv.get("tp1_only") and targets:  # improvement variant: the whole position exits at TP1
        targets = targets[:1]
    n = len(targets)
    if lv.get("tp1_only") and n:
        weights = [1.0]
    elif lv.get("tp1_close_frac") and n >= 1:
        f = float(lv["tp1_close_frac"])
        weights = [f] + [(1 - f) / (n - 1)] * (n - 1) if n > 1 else [1.0]
    elif lv.get("tp_open") and n:
        weights = [1.0 / (n + 1)] * n
    else:
        weights = [1.0 / n] * n if n else []
    return E.Plan(side, market, legs, targets, weights, stop, bool(lv.get("be_after_tp1")), liq_leverage=None), "ok"


def first_trigger(native: str, t: dict, act: pd.Timestamp, mult: float) -> pd.Timestamp | None:
    """Time of the next 1h open after the first 1h close beyond the trigger (or activation if already beyond)."""
    lvl, side = t["trigger"] * mult, t["side"]
    m5 = E.klines(native, "5m", act.floor("5min"), min(act + TRIGGER_WINDOW, E.DATA_END))
    if m5.empty:
        return None
    h = m5.resample("1h").agg({"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    first_open = float(h.iloc[0]["open"])
    if side * (first_open - lvl) > 0:
        return act
    for ts, row in h.iterrows():
        if ts < act.floor("h"):
            continue
        if side * (row["close"] - lvl) > 0:
            return ts + pd.Timedelta(hours=1)
    return None


# ---------------------------------------------------------------- simulation

def _dated(f: pd.DataFrame) -> pd.DataFrame:
    # an empty answer (contract not listed yet) still needs a time index for the comparisons below
    return f if isinstance(f.index, pd.DatetimeIndex) else f.set_index(pd.DatetimeIndex([]))


def bars_for(native: str, act: pd.Timestamp, hours: int):
    end = min(act + pd.Timedelta(hours=hours, minutes=5), E.DATA_END)
    a1 = act.floor("min")
    m1 = _dated(E.klines(native, "1m", a1, a1 + pd.Timedelta(hours=2)))
    if hours <= 2:
        return [(m1, pd.Timedelta(minutes=1))]
    m5 = _dated(E.klines(native, "5m", (a1 + pd.Timedelta(hours=2)).floor("5min"), end))
    return [(m1, pd.Timedelta(minutes=1)), (m5[m5.index >= a1 + pd.Timedelta(hours=2)], pd.Timedelta(minutes=5))]


def _apply_update(eng: E.Engine, u: dict, t: pd.Timestamp, price: float, mult: float) -> None:
    if eng.qty <= 0 and u["kind"] in ("close", "partial", "be"):
        if u["kind"] == "close" and eng.status == "pending":
            eng.status, eng.exit_reason = "cancelled", "author_close_before_fill"
        return
    if u["kind"] == "close":
        eng._exit(t, price, eng.qty, "author_close")
    elif u["kind"] == "partial":
        eng._exit(t, price, eng.qty * 0.5, "author_partial")
    elif u["kind"] == "be" and eng.avg:
        eng.stop = eng.avg
    elif u["kind"] == "levels":
        side = eng.p.side
        if isinstance(u.get("sl"), (int, float)):
            s = u["sl"] * mult
            ref = eng.avg or price
            if side * (ref - s) > 0:
                eng.stop = s
        tps = [x * mult for x in _num_list(u.get("tp")) if side * (x * mult - (eng.avg or price)) > 0]
        if tps:
            done = [tp for tp, d in zip(eng.p.targets, eng.tp_done) if d]
            eng.p.targets = done + sorted(tps, key=lambda x: side * x)
            eng.tp_done = [True] * len(done) + [False] * len(tps)
            rest = max(0.0, 1.0 - sum(eng.p.weights[:len(done)]))
            eng.p.weights = eng.p.weights[:len(done)] + [rest / len(tps)] * len(tps)


def simulate(t: dict, policy: str, leverage: float | None = None) -> dict:
    native, mult = E.native_symbol(t["symbol"])
    if native is None:
        return {"status": "no_contract"}
    if t["kind"] == "conditional_text":
        return {"status": "condition_not_executable"}
    act = pd.Timestamp(t["activation"]).tz_convert(None)
    hours = t["horizon_h"]
    if t["kind"] == "conditional_numeric":
        trig = first_trigger(native, t, act, mult)
        if trig is None:
            return {"status": "not_triggered", "native": native}
        act = trig
    if act + pd.Timedelta(hours=hours) > E.DATA_END:
        return {"status": "immature", "native": native}
    segments = bars_for(native, act, hours)
    first = segments[0][0]
    first = first[first.index >= act.ceil(segments[0][1])]
    if first.empty:
        return {"status": "no_market_data", "native": native}
    ref = float(first.iloc[0]["open"])
    plan, why = levels_to_plan(t, ref, mult)
    if plan is None:
        return {"status": "invalid_geometry" if why != "no_levels" else "no_levels", "reason": why, "native": native,
                "ref": ref}
    expiry = act + pd.Timedelta(hours=hours)
    plan.liq_leverage = leverage
    eng = E.Engine(plan, act, expiry, act + E.ENTRY_VALID, finer=E.make_finer(native),
                   funding_series=E.funding(native, act - pd.Timedelta(hours=8), expiry + pd.Timedelta(hours=1)))
    updates = [u for u in t["updates"] if policy == "B" and pd.Timestamp(u["at"]).tz_convert(None) > act]
    applied = []
    for bars, blen in segments:
        cut_points = [pd.Timestamp(u["at"]).tz_convert(None) for u in updates]
        start = 0
        idx = bars.index
        for u, cp in zip(list(updates), cut_points):
            if cp >= (idx[-1] + blen if len(idx) else cp):
                break
            pos = idx.searchsorted(cp.ceil(blen))
            eng.run(bars.iloc[start:pos], blen)
            if eng.status in ("cancelled", "stale", "closed", "expired_unfilled"):
                break
            if pos < len(bars):
                _apply_update(eng, u, idx[pos], float(bars.iloc[pos]["open"]), mult)
                applied.append(u["kind"])
            updates.remove(u)
            start = pos
        if eng.status not in ("cancelled", "stale", "closed", "expired_unfilled"):
            eng.run(bars.iloc[start:], blen)
        if eng.status in ("cancelled", "stale", "closed", "expired_unfilled"):
            break
    if eng.status in ("open", "pending") and eng.qty > 0:
        last = segments[-1][0]
        eng._exit(last.index[-1], float(last.iloc[-1]["close"]), eng.qty, "expiry")
    elif eng.status == "pending":
        eng.status = "expired_unfilled"
    out = eng.result()
    out.update({"native": native, "mult": mult, "ref": ref, "entry_at": str(act), "plan_check": why,
                "updates_applied": applied,
                "plan": {"legs": plan.legs, "targets": plan.targets, "weights": plan.weights, "stop": plan.stop,
                         "market": plan.entry_market, "be": plan.be_after_tp1}})
    return out


def direction_returns(t: dict) -> dict:
    """Policy C: market entry at activation, exits at +24h and +7d on official 5m bars (1m for the first 2h)."""
    if t["kind"].startswith("conditional"):
        return {"status": "conditional_excluded"}
    native, mult = E.native_symbol(t["symbol"])
    if native is None:
        return {"status": "no_contract"}
    act = pd.Timestamp(t["activation"]).tz_convert(None)
    out = {"native": native}
    segs = bars_for(native, act, 24 * 14)
    allbars = pd.concat([s[0] for s in segs if len(s[0])]).sort_index() if any(len(s[0]) for s in segs) else segs[0][0]
    allbars = allbars[allbars.index >= act.ceil("min")]
    if allbars.empty:
        return {"status": "no_market_data", "native": native}
    p0 = float(allbars.iloc[0]["open"])
    out["ref"] = p0
    for name, h in (("r24h", 24), ("r7d", 24 * 7)):
        end = act + pd.Timedelta(hours=h)
        if end > E.DATA_END:
            out[name] = None
            continue
        b = allbars[allbars.index < end]
        if b.empty or b.index[-1] < end - pd.Timedelta(minutes=10):
            out[name] = None
            continue
        out[name] = t["side"] * (float(b.iloc[-1]["close"]) / p0 - 1) - 2 * E.COST
    out["status"] = "ok"
    return out


def cmd_simulate(_a) -> None:
    data = E.read_json(BT / "trades_pre_price.json")
    freeze = E.read_json(BT / "freeze.json")
    assert sha((BT / "trades_pre_price.json").read_bytes()) == freeze["trades_sha256"], "trades changed after freeze"
    rows = []
    for i, t in enumerate(data["trades"]):
        rec = {k: t[k] for k in ("channel", "id", "symbol", "side", "kind", "group", "fresh", "quality",
                                 "published_at", "activation", "point_in_time", "horizon_h")}
        rec["n_updates"] = len(t["updates"])
        for pol in ("A", "B"):
            try:
                rec[pol] = simulate(t, pol) if (pol == "A" or t["updates"]) else None
            except Exception as exc:  # keep going; record the failure
                rec[pol] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        try:
            rec["C"] = direction_returns(t)
        except Exception as exc:
            rec["C"] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        rows.append(rec)
        if i % 25 == 0:
            print(i, len(data["trades"]), t["channel"], t["symbol"], (rec["A"] or {}).get("status"), flush=True)
            E.write_json(BT / "results_partial.json", rows)
    E.write_json(BT / "results.json", {"data_end": str(E.DATA_END), "rows": rows})
    print("done", Counter((r["channel"], (r["A"] or {}).get("status")) for r in rows))


# ---------------------------------------------------------------- nulls

def _h1(native: str) -> pd.DataFrame:
    h = E.local_frame(native, "1h")
    return h[["open", "high", "low", "close"]] if not h.empty else h


def null_direction(t: dict, rec: dict, h: pd.DataFrame) -> dict | None:
    act = pd.Timestamp(t["activation"]).tz_convert(None)
    side = t["side"]

    def ret(start, hours):
        a = h.index.searchsorted(start)
        b = h.index.searchsorted(start + pd.Timedelta(hours=hours)) - 1
        if a >= len(h) or b <= a or h.index[b] < start + pd.Timedelta(hours=hours - 2):
            return None
        return side * (float(h.iloc[b]["close"]) / float(h.iloc[a]["open"]) - 1) - 2 * E.COST

    out = {}
    for name, hours in (("24h", 24), ("7d", 168)):
        sig = ret(act.ceil("h"), hours)
        pre = [ret((act - pd.Timedelta(days=30) + pd.Timedelta(hours=12 * k)).ceil("h"), hours) for k in range(58)]
        post = [ret((act + pd.Timedelta(days=1) + pd.Timedelta(hours=12 * k)).ceil("h"), hours) for k in range(58)]
        pre = [x for x in pre if x is not None]
        post = [x for x in post if x is not None]
        out[name] = {"signal_1h": sig, "n_pre": len(pre), "n_post": len(post),
                     "excess_pre": sig - float(np.mean(pre)) if sig is not None and len(pre) >= 20 else None,
                     "excess_post": sig - float(np.mean(post)) if sig is not None and len(post) >= 20 else None}
    return out


def cmd_nulls(_a) -> None:
    data = E.read_json(BT / "trades_pre_price.json")
    res = E.read_json(BT / "results.json")
    trades = {(t["channel"], str(t["id"]), t["symbol"], t["side"]): t for t in data["trades"]}
    cache: dict = {}
    for rec in res["rows"]:
        t = trades[(rec["channel"], str(rec["id"]), rec["symbol"], rec["side"])]
        native = (rec.get("C") or {}).get("native")
        if not native:
            continue
        if native not in cache:
            cache[native] = _h1(native)
        h = cache[native]
        if h.empty:
            continue
        rec["C_null"] = null_direction(t, rec, h)
        a = rec.get("A") or {}
        if a.get("plan"):
            E.HOLD = pd.Timedelta(hours=t["horizon_h"])
            sig = {"activation": a.get("entry_at") + "+00:00" if a.get("entry_at") and "+" not in a["entry_at"]
                   else a.get("entry_at"), "side": t["side"]}
            try:
                rec["A_null"] = E.null_for(sig, a)
            except Exception as exc:
                rec["A_null"] = {"error": str(exc)}
            E.HOLD = pd.Timedelta(days=14)
    E.write_json(BT / "results.json", res)
    print("nulls done")


# ---------------------------------------------------------------- report

def _cell(name: str, vals: pd.Series, weeks: pd.Series) -> dict:
    vals = vals.astype(float).reset_index(drop=True)
    weeks = weeks.reset_index(drop=True)
    return {"cell": name, "n": int(len(vals)), "mean": float(vals.mean()), "median": float(vals.median()),
            "win": float((vals > 0).mean()), "ci95": E.block_ci(vals, weeks),
            "p": E.block_p(vals, weeks) if len(vals) >= 5 else None}


def cmd_report(_a) -> None:
    res = E.read_json(BT / "results.json")
    df = pd.DataFrame(res["rows"])
    df["week"] = pd.to_datetime(df["activation"]).dt.tz_convert(None).dt.to_period("W").astype(str)
    cells = []
    status = {}
    for (ch, grp), g in df.groupby(["channel", "group"]):
        status[f"{ch}|{grp}"] = {
            "calls": int(len(g)),
            "A_status": Counter((x or {}).get("status") for x in g["A"]),
            "A_reason": Counter((x or {}).get("reason") for x in g["A"] if (x or {}).get("reason")),
            "C_status": Counter((x or {}).get("status") for x in g["C"]),
            "kinds": Counter(g["kind"]),
        }
        def ret_of(x):
            return x.get("ret_on_filled") if isinstance(x, dict) else None
        a_vals = [ret_of(x) for x in g["A"]]
        # B equals A unless the call had later instructions (B is None then)
        b_vals = [ret_of(a) if b is None else ret_of(b) for a, b in zip(g["A"], g["B"])]
        for pol, vals in (("A", a_vals), ("B", b_vals)):
            keep = [v is not None for v in vals]
            if sum(keep) >= 3:
                c = _cell(f"{ch}|{grp}|{pol}", pd.Series([v for v in vals if v is not None]), g["week"][keep])
                if pol == "A":
                    c["exit_reasons"] = Counter((x or {}).get("exit_reason") for x, k in zip(g["A"], keep) if k)
                    c["tp1_rate"] = float(np.mean([(x or {}).get("tps_hit", 0) >= 1 for x, k in zip(g["A"], keep) if k]))
                cells.append(c)
        if "A_null" in g:
            ex = [(n or {}).get("excess_post") if isinstance(n, dict) else None for n in g["A_null"]]
            keep = [v is not None for v in ex]
            if sum(keep) >= 3:
                cells.append(_cell(f"{ch}|{grp}|A_excess_post", pd.Series([v for v in ex if v is not None]),
                                   g["week"][keep]))
        for hz in ("r24h", "r7d"):
            vals = [(x or {}).get(hz) for x in g["C"]]
            keep = [v is not None for v in vals]
            if sum(keep) >= 3:
                cells.append(_cell(f"{ch}|{grp}|C_{hz}", pd.Series([v for v in vals if v is not None]), g["week"][keep]))
            nk = "24h" if hz == "r24h" else "7d"
            if "C_null" in g:
                ex = [((n or {}).get(nk) or {}).get("excess_post") if isinstance(n, dict) else None for n in g["C_null"]]
                keep2 = [v is not None for v in ex]
                if sum(keep2) >= 3:
                    cells.append(_cell(f"{ch}|{grp}|C_{nk}_excess_post", pd.Series([v for v in ex if v is not None]),
                                       g["week"][keep2]))
                exp = [((n or {}).get(nk) or {}).get("excess_pre") if isinstance(n, dict) else None for n in g["C_null"]]
                keep3 = [v is not None for v in exp]
                if sum(keep3) >= 3:
                    cells.append(_cell(f"{ch}|{grp}|C_{nk}_excess_pre", pd.Series([v for v in exp if v is not None]),
                                       g["week"][keep3]))
    ps = sorted([(c["p"], i) for i, c in enumerate(cells) if c.get("p") is not None])
    m, q_prev = len(ps), 1.0
    for rank, (p, i) in reversed(list(enumerate(ps, 1))):
        q_prev = min(q_prev, p * m / rank)
        cells[i]["q"] = q_prev
    E.write_json(BT / "summary.json", {"data_end": res["data_end"], "status": status, "cells": cells})
    show = pd.DataFrame(cells)[["cell", "n", "mean", "median", "win", "p"] + (["q"] if any("q" in c for c in cells) else [])]
    for col in ("mean", "median"):
        show[col] = (show[col] * 100).round(2)
    pd.set_option("display.width", 220)
    print(show.to_string())
    print(json.dumps(status, indent=1, default=str)[:6000])


def cmd_extras(_a) -> None:
    """Stated-leverage rerun, cross-channel agreement, and the author's own claims against the simulation."""
    data = E.read_json(BT / "trades_pre_price.json")
    res = E.read_json(BT / "results.json")
    trades = {(t["channel"], str(t["id"]), t["symbol"], t["side"]): t for t in data["trades"]}
    out = {"leverage": [], "consensus": [], "claims": []}
    # 1) stated leverage, isolated margin: does the as-written plan survive?
    for rec in res["rows"]:
        t = trades[(rec["channel"], str(rec["id"]), rec["symbol"], rec["side"])]
        lev = t["lv"].get("lev")
        a = rec.get("A") or {}
        if t["group"] != "main" or not isinstance(lev, (int, float)) or a.get("ret_on_filled") is None:
            continue
        lv = simulate(t, "A", leverage=float(lev))
        out["leverage"].append({"channel": t["channel"], "id": t["id"], "symbol": t["symbol"], "lev": lev,
                                "exit_reason": lv.get("exit_reason"), "ret_on_filled": lv.get("ret_on_filled"),
                                "ret_on_margin": max(-1.0, lv["ret_on_filled"] * lev)
                                if lv.get("ret_on_filled") is not None else None})
    # 2) the same coin and side called by two or more channels within 24 hours
    rows = [r for r in res["rows"] if r["group"] == "main" and (r.get("C") or {}).get("r24h") is not None]
    rows.sort(key=lambda r: r["activation"])
    for i, r in enumerate(rows):
        t0 = pd.Timestamp(r["activation"])
        others = {x["channel"] for x in rows[:i] if x["symbol"] == r["symbol"] and x["side"] == r["side"]
                  and x["channel"] != r["channel"] and t0 - pd.Timestamp(x["activation"]) <= pd.Timedelta(hours=24)}
        if others:
            out["consensus"].append({"channel": r["channel"], "id": r["id"], "symbol": r["symbol"], "side": r["side"],
                                     "with": sorted(others), "r24h": r["C"]["r24h"], "r7d": r["C"].get("r7d")})
    # 3) every claim/close the author attached to a call, next to what the plan actually did
    for slug in K.CHANNELS:
        claims: dict = {}
        for r in read_labels(slug):
            if r["c"] in ("claim", "update_close", "update_partial") and r.get("ref") is not None:
                for ref in (r["ref"] if isinstance(r["ref"], list) else [r["ref"]]):
                    claims.setdefault(str(ref), []).append({"id": r["id"], "c": r["c"], "n": (r.get("n") or "")[:120]})
        for rec in res["rows"]:
            if rec["channel"] != slug or str(rec["id"]) not in claims:
                continue
            a = rec.get("A") or {}
            out["claims"].append({"channel": slug, "id": rec["id"], "symbol": rec["symbol"], "group": rec["group"],
                                  "claims": claims[str(rec["id"])], "A_status": a.get("status"),
                                  "A_exit": a.get("exit_reason"), "A_ret": a.get("ret_on_filled"),
                                  "A_tps": a.get("tps_hit"), "C_r24h": (rec.get("C") or {}).get("r24h")})
    E.write_json(BT / "extras.json", out)
    lev = pd.DataFrame(out["leverage"])
    if len(lev):
        print(lev.groupby("channel").agg(n=("id", "size"), liquidated=("exit_reason", lambda x: (x == "liquidated").sum()),
                                         mean_margin=("ret_on_margin", "mean")))
    con = pd.DataFrame(out["consensus"])
    if len(con):
        print("consensus", len(con), con[["r24h", "r7d"]].mean().to_dict())
    print("claims", len(out["claims"]))


def _perm_p(a: pd.Series, b: pd.Series, reps: int = 10000, seed: int = 5) -> float | None:
    """Two-sided permutation p for mean(a) - mean(b)."""
    if len(a) < 3 or len(b) < 3:
        return None
    rng = np.random.default_rng(seed)
    pool, na, obs = np.concatenate([a.to_numpy(), b.to_numpy()]), len(a), abs(a.mean() - b.mean())
    hits = sum(abs(x[:na].mean() - x[na:].mean()) >= obs for x in (rng.permutation(pool) for _ in range(reps)))
    return (hits + 1) / (reps + 1)


def cmd_improve(_a) -> None:
    """Pre-declared variants (backtest/improve_declared.json): run-up filter, break-even after TP1, TP1 only."""
    import copy
    data = E.read_json(BT / "trades_pre_price.json")
    res = E.read_json(BT / "results.json")
    trades = {(t["channel"], str(t["id"]), t["symbol"], t["side"]): t for t in data["trades"]}
    rows = []
    for rec in res["rows"]:
        if rec["group"] != "main":
            continue
        t = trades[(rec["channel"], str(rec["id"]), rec["symbol"], rec["side"])]
        a, c = rec.get("A") or {}, rec.get("C") or {}
        native = c.get("native") or a.get("native")
        if not native or (a.get("ret_on_filled") is None and c.get("r24h") is None):
            continue
        try:
            pm = E.pre_move(native, {"published_at": t["published_at"], "side": t["side"]})
        except Exception:
            pm = None
        row = {"channel": t["channel"], "id": t["id"], "symbol": t["symbol"], "activation": t["activation"],
               "pre_move_30m": pm, "A": a.get("ret_on_filled"), "C24": c.get("r24h"), "C7": c.get("r7d")}
        if a.get("ret_on_filled") is not None:
            for name in ("be_after_tp1", "tp1_only"):
                u = copy.deepcopy(t)
                u["lv"][name] = True
                row[name] = simulate(u, "A").get("ret_on_filled")
        rows.append(row)
    df = pd.DataFrame(rows)
    df["week"] = pd.to_datetime(df["activation"]).dt.tz_convert(None).dt.to_period("W").astype(str)
    cells = []
    for ch, g in df.groupby("channel"):
        g = g.sort_values("activation")
        for name in ("be_after_tp1", "tp1_only"):
            d = g[g["A"].notna() & g[name].notna()]
            if len(d) < 5:
                continue
            diff = (d[name] - d["A"]).astype(float).reset_index(drop=True)
            half = len(diff) // 2
            cells.append({"cell": f"{ch}|{name}-A", "n": len(diff), "mean": float(diff.mean()),
                          "first_half": float(diff.iloc[:half].mean()), "second_half": float(diff.iloc[half:].mean()),
                          "variant_mean": float(d[name].mean()), "p": E.block_p(diff, d["week"].reset_index(drop=True))})
        for metric in ("A", "C24", "C7"):
            d = g[g[metric].notna()]
            skip = d["pre_move_30m"].fillna(0) >= 0.01
            kept, gone = d.loc[~skip, metric].astype(float), d.loc[skip, metric].astype(float)
            if len(gone) == 0:
                continue
            kh = kept.reset_index(drop=True)
            half = len(kh) // 2
            cells.append({"cell": f"{ch}|skip_runup|{metric}", "n": len(kept), "skipped": int(len(gone)),
                          "mean": float(kept.mean()), "all_mean": float(d[metric].mean()),
                          "skipped_mean": float(gone.mean()), "first_half": float(kh.iloc[:half].mean()),
                          "second_half": float(kh.iloc[half:].mean()), "p": _perm_p(kept, gone)})
    ps = sorted([(c["p"], i) for i, c in enumerate(cells) if c.get("p") is not None])
    m, q_prev = len(ps), 1.0
    for rank, (pv, i) in reversed(list(enumerate(ps, 1))):
        q_prev = min(q_prev, pv * m / rank)
        cells[i]["q"] = q_prev
    E.write_json(BT / "improve.json", {"declared_sha256": sha((BT / "improve_declared.json").read_bytes()),
                                       "rows": rows, "cells": cells})
    show = pd.DataFrame(cells)
    for col in ("mean", "all_mean", "skipped_mean", "variant_mean", "first_half", "second_half"):
        if col in show:
            show[col] = (show[col].astype(float) * 100).round(2)
    pd.set_option("display.width", 250)
    print(show.to_string())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "freeze", "simulate", "nulls", "report", "extras", "improve"])
    a = ap.parse_args()
    {"build": cmd_build, "freeze": cmd_freeze, "simulate": cmd_simulate, "nulls": cmd_nulls, "report": cmd_report,
     "extras": cmd_extras, "improve": cmd_improve}[a.cmd](a)


if __name__ == "__main__":
    main()
