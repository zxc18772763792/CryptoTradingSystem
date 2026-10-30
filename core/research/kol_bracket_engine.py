"""Frozen HUB signal rules and bracket execution engine (rule version hub-v1, 2026-10-30).

Copied verbatim from scripts/research_kol_execution.py at commit 399fcba (the code behind
docs/TELEGRAM_KOL_EXECUTION_BACKTEST_20261028.md) so the forward ledger hub_runup_filter_v1 cannot drift when
the research script changes. The ledger pins this file's SHA-256; any edit starts a new journal.
Only the CRYPTO SIGNALS HUB parser is kept. ``DEEP_REST`` stays False (the frozen primary rule).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re

import numpy as np
import pandas as pd

NUM = re.compile(r"\d[\d,]*(?:\.\d+)?|\.\d+")


def to_float(token: str) -> float | None:
    token = token.strip()
    if token.startswith("."):
        token = "0" + token
    if "," in token:
        if re.fullmatch(r"\d{1,3}(,\d{3})+(\.\d+)?", token):
            token = token.replace(",", "")
        elif re.fullmatch(r"\d+,\d+", token):
            token = token.replace(",", ".")
        else:
            return None
    try:
        return float(token)
    except ValueError:
        return None


def numbers(text: str) -> list[float]:
    """Prices on a line: drops leverage (10x), percentages, and 'xxx' placeholders."""
    text = re.sub(r"\d[\d,]*x{2,}", " ", text, flags=re.I)  # 65,xxx
    out = []
    for m in NUM.finditer(text):
        tail = text[m.end():m.end() + 2]
        if re.match(r"\s?[x×%]", tail, re.I):
            continue
        value = to_float(m.group())
        if value is not None and value > 0:
            if re.match(r"[kK]\b", tail):
                value *= 1000
            out.append(value)
    return out


INDEX = re.compile(r"^(tp|take\s*profit|target|targets|entry)\s*(\d)(?=\s*[-–:•)]|\s+•|\s*$)", re.I)


def clean_line(line: str) -> str:
    line = re.sub(r"[^\x00-\x7F]+", " ", line)  # emoji / bullets
    line = line.replace("*", " ").strip()
    return INDEX.sub(lambda m: m.group(1), line).strip()


def leverage_of(text: str) -> float | None:
    vals = [float(v) for v in re.findall(r"(\d+)\s*[x×]\b", text, re.I)]
    vals += [float(v) for v in re.findall(r"\blev(?:erage)?\s*:?\s*(?:cross\s*)?(\d+)", text, re.I)]
    vals = [v for v in vals if 1 <= v <= 200]
    return min(vals) if vals else None


@dataclass
class Signal:
    channel: str
    message_id: int
    published_at: str
    edited_at: str | None
    symbol: str
    side: int
    entry_type: str  # market | limit
    legs: list = field(default_factory=list)  # limit prices
    targets: list = field(default_factory=list)
    target_weights: list | None = None
    target_pct_margin: list | None = None  # "Target: 50-100%" at stated leverage
    stop: float | None = None
    stop_stated_none: bool = False
    leverage: float | None = None
    open_ended: bool = False
    be_after_tp1: bool = False
    note: str = ""
    text: str = ""


def sym_from(text: str) -> str | None:
    m = re.search(r"[#$]?\b([A-Z0-9]{2,15})\s*/?\s*USDT\b", text)
    if m:
        return m.group(1)
    m = re.search(r"[#$]([A-Za-z0-9]{2,15})\b", text)
    return m.group(1).upper() if m else None


def generic_body(lines: list[str], sig: Signal, entry_on_header: bool = False) -> None:
    section = None
    for raw in lines:
        line = clean_line(raw)
        low = line.lower()
        if not line:
            continue
        if re.match(r"^(tp|take\s*profit|targets?|sell|final target)\b", low):
            section = "tp_list"
            body = re.sub(r"^(tp|take\s*profit|targets?|sell|final target)\s*[:\-–•]*", "", line, flags=re.I)
            if re.search(r"\bopen\b|new ath", body, re.I):
                sig.open_ended = True
            if "%" in body and not numbers(body.replace("%", "")):
                continue
            sig.targets += numbers(body)
            continue
        if re.match(r"^(sl|s/l|stop[\s\-]*lo(?:ss|se)|stoploss)\b", low):
            section = "sl"
            body = re.sub(r"^(sl|s/l|stop[\s\-]*lo(?:ss|se)|stoploss)\s*(at)?\s*[:\-–•.]*", "", line, flags=re.I)
            vals = numbers(body)
            if vals:
                sig.stop = vals[-1] if re.search(r"\bno\b", body, re.I) else vals[0]
            elif re.search(r"\bno\b", body, re.I):
                sig.stop_stated_none = True
            continue
        if re.match(r"^(no\s+stop\s*loss|no\s+stoploss)", low):
            sig.stop_stated_none = True
            continue
        if re.match(r"^(entry|buy|ist entry|open market|open at market|entry at market)", low):
            section = "entry"
            if "market" in low:
                sig.entry_type = "market"
                continue
            body = re.sub(r"^(entry|buy)\s*[:\-–]*", "", line, flags=re.I)
            vals = numbers(body)
            if vals:
                sig.entry_type = "limit"
                sig.legs += vals
            continue
        vals = numbers(line)
        if vals and re.fullmatch(r"[\d.,\s$\-–|]+", line):
            if section == "tp_list":
                sig.targets += vals
            elif section == "sl" and sig.stop is None:
                sig.stop = vals[0]
            elif section == "entry":
                sig.entry_type = "limit"
                sig.legs += vals


def side_of(text: str) -> int | None:
    long_ = bool(re.search(r"\b(long|buy|bullish|longing|relong)\b", text, re.I))
    short = bool(re.search(r"\b(short|bearish)\b", text, re.I))
    if long_ == short:
        return None
    return 1 if long_ else -1


def parse_hub(p: dict) -> Signal | None:
    t = p["text"]
    if not re.search(r"\b(LONG|SHORT)\b", t) or not re.search(r"\bTP\b|target", t, re.I):
        return None
    head = t.split("\n")[:3]
    sym = sym_from(" ".join(head)) or sym_from(t)
    side = 1 if re.search(r"LONG", t) and not re.search(r"SHORT", t) else -1 if re.search(r"SHORT", t) and not re.search(r"LONG", t) else None
    if not sym or side is None:
        return None
    sig = Signal("hub", p["message_id"], p["published_at"], p["edited_at"], sym, side, "market", text=t)
    if not re.search(r"market price", t, re.I):
        sig.entry_type = "unknown"
    generic_body(t.split("\n"), sig)
    sig.leverage = leverage_of(t)
    return sig


def classify(sig: Signal) -> str:
    """Rule completeness, from the text only (no prices)."""
    if sig.entry_type == "unknown":
        return "no_entry_rule"
    if sig.entry_type == "limit" and not sig.legs:
        return "no_entry_rule"
    has_exit = bool(sig.targets or sig.target_pct_margin or "rr_targets" in sig.note or sig.stop is not None)
    if not has_exit:
        return "direction_only"
    if sig.stop is None and not sig.stop_stated_none:
        return "targets_without_stop"
    if not (sig.targets or sig.target_pct_margin or "rr_targets" in sig.note):
        return "stop_without_targets"
    if sig.stop_stated_none:
        return "targets_no_stop_by_design"
    return "full_bracket"


COST = 0.0010
MMR = 0.005
DEEP_REST = False  # sensitivity only: a bracket entirely beyond the market at activation keeps resting


@dataclass
class Plan:
    side: int
    entry_market: bool
    legs: list            # limit prices (absolute)
    targets: list         # absolute
    weights: list         # per target, sum <= 1; remainder rides to stop/expiry
    stop: float | None
    be_after_tp1: bool
    liq_leverage: float | None = None


class Engine:
    """Bar-by-bar bracket execution; finer() returns sub-bars for an ambiguous bar or None."""

    def __init__(self, plan: Plan, activation: pd.Timestamp, expiry: pd.Timestamp, entry_deadline: pd.Timestamp,
                 finer=None, funding_series: pd.Series | None = None):
        self.p = plan
        self.act, self.expiry, self.deadline = activation, expiry, entry_deadline
        self.finer = finer
        self.fund = funding_series if funding_series is not None else pd.Series(dtype=float)
        n = len(plan.legs) if not plan.entry_market else 1
        self.leg_w = [1.0 / n] * n
        self.leg_filled = [False] * n
        self.fills = []        # (time, price, weight)
        self.qty = 0.0         # open weight
        self.tp_done = [False] * len(plan.targets)
        self.stop = plan.stop
        self.pnl = 0.0         # in units of planned notional (price-relative)
        self.avg = None
        self.status = "pending"
        self.exit_reason = None
        self.exit_time = None
        self.events = []
        self.ambiguous_unresolved = 0
        self.refined = 0
        self.last_close = None
        self._armed = False  # TP1-before-fill cancel; with DEEP_REST it arms once price is on the entry side of TP1
        self.funding_paid = 0.0
        self._next_fund = 0

    # -- helpers
    def _fill(self, t, price, w):
        cost_basis = (self.avg or 0) * self.qty
        self.qty += w
        self.avg = (cost_basis + price * w) / self.qty
        self.fills.append((t, price, w))
        self.pnl -= COST * w
        if self.status == "pending":
            self.status = "open"

    def _exit(self, t, price, w, why):
        w = min(w, self.qty)
        if w <= 0:
            return
        if why == "liquidated":
            # isolated liquidation forfeits the whole margin of the remaining slice, not just the move to the liq price
            self.pnl += -w / self._lev() - COST * w
        else:
            self.pnl += self.p.side * (price / self.avg - 1) * w - COST * w
        self.qty -= w
        self.events.append((str(t), why, price, w))
        if self.qty <= 1e-12:
            self.qty = 0.0
            self.status = "closed"
            self.exit_reason, self.exit_time = why, t

    def _lev(self):
        # an unlevered short is still fully margined: it is wiped out when the price doubles
        return self.p.liq_leverage or (1.0 if self.p.side < 0 else None)

    def _liq(self):
        L = self._lev()
        if not L or self.avg is None:
            return None
        return self.avg * (1 - self.p.side * (1 / L - MMR))

    def _adverse_level(self):
        levels = [x for x in (self.stop, self._liq()) if x is not None]
        if not levels:
            return None
        return max(levels) if self.p.side > 0 else min(levels)

    def _beyond(self, price, level, favorable):
        # favorable: target direction for this side
        s = self.p.side if favorable else -self.p.side
        return (price >= level) if s > 0 else (price <= level)

    def _touch(self, bar, level, favorable):
        return self._beyond(bar.high if (self.p.side > 0) == favorable else bar.low, level, favorable)

    def _apply_funding(self, t_end):
        while self._next_fund < len(self.fund) and self.fund.index[self._next_fund] < t_end:
            ft = self.fund.index[self._next_fund]
            if self.qty > 0 and ft > self.act and self.last_close is not None:
                paid = self.p.side * float(self.fund.iloc[self._next_fund]) * self.qty * (self.last_close / self.avg)
                self.pnl -= paid
                self.funding_paid += paid
            self._next_fund += 1

    # -- per bar
    def run(self, bars: pd.DataFrame, bar_len: pd.Timedelta):
        for t, bar in zip(bars.index, bars.itertuples(index=False)):
            if self.status == "closed":
                break
            if t < self.act.ceil(bar_len):
                continue
            if t >= self.expiry:
                if self.qty > 0:
                    self._exit(t, bar.open, self.qty, "expiry")
                else:
                    self.status = "expired_unfilled"
                break
            self._apply_funding(t)
            self.bar(t, bar, bar_len)
            self.last_close = bar.close
            if self.status in ("cancelled", "stale", "closed", "expired_unfilled"):
                break

    def _first_bar(self, t, bar):
        """Activation bar: market fill / marketable limits / staleness at the open."""
        p = self.p
        o = bar.open
        tp1 = p.targets[0] if p.targets else None
        if p.entry_market:
            if tp1 is not None and self._beyond(o, tp1, True):
                self.status, self.exit_reason = "stale", "tp1_passed_at_activation"
                return False
            if p.stop is not None and self._beyond(o, p.stop, False):
                self.status, self.exit_reason = "stale", "stop_passed_at_activation"
                return False
            self.leg_filled[0] = True
            self._fill(t, o * (1 + 0), 1.0)
            return True
        for i, lp in enumerate(p.legs):
            if o == lp or not self._beyond(o, lp, True):  # long: open <= limit -> marketable
                self.leg_filled[i] = True
                self._fill(t, o, self.leg_w[i])
        return True

    def bar(self, t, bar, bar_len):
        if self.status == "pending" and not self.fills and not getattr(self, "_started", False):
            self._started = True
            if not self._first_bar(t, bar):
                return
            # the open has been consumed; continue with intrabar logic on the same bar
        self._intrabar(t, bar, bar_len)

    def _intrabar(self, t, bar, bar_len):
        p = self.p
        unfilled = [i for i, f in enumerate(self.leg_filled) if f is False] if t < self.deadline else []
        entry_hit = [i for i in unfilled if self._touch(bar, p.legs[i], False)]
        open_tps = [k for k, d in enumerate(self.tp_done) if not d]
        tp_hit = [k for k in open_tps if self._touch(bar, p.targets[k], True)]
        adverse = self._adverse_level() if self.qty > 0 else None
        adverse_hit = adverse is not None and self._touch(bar, adverse, False)
        stop_hit_pre = self.qty == 0 and p.stop is not None and self._touch(bar, p.stop, False)
        favorable = bool(tp_hit)
        unfavorable = bool(adverse_hit or entry_hit or stop_hit_pre)
        if favorable and unfavorable:
            sub = self.finer(t, bar_len) if self.finer else None
            if sub is not None and len(sub) > 1:
                self.refined += 1
                sub_len = sub.index[1] - sub.index[0]
                for st, sb in zip(sub.index, sub.itertuples(index=False)):
                    if self.status in ("cancelled", "closed"):
                        break
                    self._intrabar_conservative(st, sb, sub_len)
                return
            self.ambiguous_unresolved += 1
        self._intrabar_conservative(t, bar, bar_len)

    def _intrabar_conservative(self, t, bar, bar_len):
        """Resolve a bar assuming the adverse path first; at gaps use the open."""
        p = self.p
        o = bar.open
        # gaps at the open of this (sub)bar
        if self.qty > 0:
            adv = self._adverse_level()
            if adv is not None and self._beyond(o, adv, False):
                self._exit(t, o, self.qty, self._adv_reason(adv))
                return
        unfilled = [i for i, f in enumerate(self.leg_filled) if f is False] if t < self.deadline else []
        # pre-entry cancels (TP1 first is the conservative order for entry+TP1)
        if not self.fills:
            tp1 = p.targets[0] if p.targets else None
            if tp1 is not None and not self._armed and not self._beyond(o, tp1, True):
                self._armed = True
            if tp1 is not None and (self._armed or not DEEP_REST) and self._touch(bar, tp1, True):
                self.status, self.exit_reason = "cancelled", "tp1_before_entry"
                return
            if p.stop is not None and self._touch(bar, p.stop, False) and not any(
                    self._touch(bar, p.legs[i], False) for i in unfilled):
                self.status, self.exit_reason = "cancelled", "stop_before_entry"
                return
        # adverse path: fill resting legs (price moves against us first), then stop
        for i in sorted(unfilled, key=lambda i: -p.side * p.legs[i]):
            lp = p.legs[i]
            if self._touch(bar, lp, False):
                self.leg_filled[i] = True
                self._fill(t, o if not self._beyond(o, lp, True) else lp, self.leg_w[i])
        if self.qty > 0:
            adv = self._adverse_level()
            if adv is not None and self._touch(bar, adv, False):
                self._exit(t, adv, self.qty, self._adv_reason(adv))
                return
        # favorable path within the same bar only if no adverse touch happened
        if self.qty > 0:
            for k, d in enumerate(self.tp_done):
                if d or self.qty <= 0:
                    continue
                tp = p.targets[k]
                if self._touch(bar, tp, True):
                    price = o if self._beyond(o, tp, True) else tp
                    self.tp_done[k] = True
                    w = self.planned_weight(k)
                    last = k == len(p.targets) - 1 and sum(self.weights_full()) >= 0.999
                    self._exit(t, price, self.qty if last else w, f"tp{k + 1}")
                    if k == 0:
                        for i, f in enumerate(self.leg_filled):
                            if not f:
                                self.leg_filled[i] = None  # cancelled leg
                        if p.be_after_tp1 and self.avg:
                            self.stop = self.avg
                else:
                    break

    def _adv_reason(self, level):
        liq = self._liq()
        return "liquidated" if liq is not None and level == liq and (self.stop is None or level != self.stop) else (
            "stop_be" if self.stop is not None and self.p.stop is not None and self.stop != self.p.stop else "stop")

    def weights_full(self):
        return self.p.weights

    def planned_weight(self, k):
        filled = sum(w for (_, _, w) in self.fills)
        return self.p.weights[k] * filled

    def result(self):
        filled = sum(w for (_, _, w) in self.fills)
        return {"status": self.status, "exit_reason": self.exit_reason,
                "exit_time": str(self.exit_time) if self.exit_time is not None else None,
                "filled_fraction": filled, "avg_entry": self.avg,
                "first_fill": str(self.fills[0][0]) if self.fills else None,
                "ret_on_filled": (self.pnl / filled) if filled else None,
                "funding": self.funding_paid / filled if filled else None,
                "tps_hit": sum(bool(x) for x in self.tp_done), "events": self.events[:12],
                "refined_bars": self.refined, "ambiguous_unresolved": self.ambiguous_unresolved}


def build_plan(sig: dict, ref: float, mult: float, leverage: float | None = None) -> tuple[Plan | None, str]:
    """Absolute plan in contract price units. ref = activation open (contract units)."""
    side = sig["side"]
    scale = mult
    legs = [x * scale for x in sig["legs"]]
    targets = [x * scale for x in sig["targets"]]
    stop = sig["stop"] * scale if sig["stop"] is not None else None
    note = sig.get("note") or ""
    entry_ref = (np.mean(legs) if legs else ref)
    # $-offset conventions in hand labels (Wise BTC posts)
    if sig["channel"] == "wise" and sig["stop"] is not None and sig["stop"] < 2000 and sig["symbol"] == "BTC":
        stop = entry_ref - side * sig["stop"]
    if targets and min(sig["targets"]) < 0:
        targets = [entry_ref - side * x for x in sig["targets"]]
    m = re.search(r"rr_targets=([\d,]+)", note)
    if m and stop is not None and not targets:
        risk = abs(entry_ref - stop)
        targets = [entry_ref + side * risk * float(r) for r in m.group(1).split(",")]
    if sig.get("target_pct_margin") and not targets:
        lev = sig.get("leverage") or 10.0
        targets = [entry_ref * (1 + side * pct / 100 / lev) for pct in sig["target_pct_margin"]]
    # Unit slips such as ETH "TP 1.7" for 1,700 or a stop with one zero missing: rescale a level by a
    # power of ten only when that lands it near the activation price (what a human follower sees).
    # Passive limit legs are never rescaled: a deep resting order is a legitimate instruction.
    repaired = 0

    def fix(x, anchor, powers=(3, -3)):
        nonlocal repaired
        if 0.2 < x / anchor < 5.0:
            return x
        for k in powers:
            y = x * 10.0 ** k
            if 0.67 < y / anchor < 1.5:
                repaired += 1
                return y
        return None

    fixed_legs = []
    for x in legs:
        passive = side * (ref - x) >= 0
        if passive and 0.2 < x / ref < 5.0:
            fixed_legs.append(x)
            continue
        y = None if passive else fix(x, ref)
        if y is None or not (0.5 < y / ref < 2.0):
            return None, "price_scale_mismatch"
        fixed_legs.append(y)
    legs = fixed_legs
    entry_ref = (np.mean(legs) if legs else ref)
    targets = [fix(x, entry_ref) for x in targets]
    # thousands written as decimals (HUB "1.7" for 1,700) for every level; a dropped/extra zero only on stops,
    # since a target off by 10x can be a real instruction that just makes no sense (HUB AKE short, 07-15)
    stop = fix(stop, entry_ref, (1, -1, 3, -3)) if stop else stop
    if any(x is None for x in targets) or (sig["stop"] is not None and stop is None):
        return None, "price_scale_mismatch"
    targets = sorted(targets, key=lambda x: side * x)
    market = sig["entry_type"] == "market"
    if any(side * (tp - entry_ref) <= 0 for tp in targets):
        # for a market order the reference is the price a follower actually gets: the move already happened
        return None, "stale_target_passed" if market else "target_on_wrong_side_of_entry"
    if stop is not None and side * (entry_ref - stop) <= 0:
        return None, "stale_stop_passed" if market else "stop_on_wrong_side_of_entry"
    n = len(targets)
    weights = sig.get("target_weights") or ([1.0 / (n + 1)] * n if sig.get("open_ended") and n else [1.0 / n] * n if n else [])
    return Plan(side, market, legs, targets, weights, stop, bool(sig.get("be_after_tp1")),
                liq_leverage=leverage), "ok" if not repaired else f"ok_unit_repaired_{repaired}"
