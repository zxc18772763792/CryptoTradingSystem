"""Rules-as-written execution backtest of joined Telegram KOL signals (2026-10-28).

Earlier KOL studies scored calls as fixed-horizon direction bets (24h/72h/168h),
or as a single TP1/stop with midpoint limit fills. This study executes each post
the way it is written: market vs limit vs range entries, every listed target with
an even split (or the split the post states), the stated stop, in-post management
("move SL to BE at TP1"), and the stated leverage as a separate liquidation check.

Subcommands: label (parse + freeze before any price read), fetch, simulate, report.
Private message text stays under the restricted research directory; the repo only
gets aggregate output. Never places orders.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
HOME = Path.home() / ".crypto_trading_system/research"
OUT = HOME / "kol_execution_20261028"
LOCAL = ROOT / "data/historical/binance_futures"

SOURCES = {
    "hub": HOME / "multi_channel_backtest_180d_20261025/cryptosignalshub_history.json",
    "zetoshi": HOME / "multi_channel_backtest_180d_20261025/zetoshix_history.json",
    "next10xgem1": HOME / "telegram_new_kols_20261025/next10xgem1_history.json",
    "richtrader": HOME / "telegram_new_kols_20261025/therichtrader_reborn_history.json",
    "wise": HOME / "telegram_new_kols_20261025/wiseadvicebysumit_history.json",
}
FOLLOWUP = HOME / "telegram_kol_followup_20261027T151200Z"
FOLLOWUP_FILES = {"next10xgem1": "next10xgem1_history.json", "richtrader": "therichtrader_reborn_history.json",
                  "wise": "wiseadvicebysumit_history.json"}

# Frozen execution rules (written before any price was read for this study).
RULES = {
    "activation": "max(published_at, edited_at) + 60s; primary sample requires edited_at <= published_at + 10min",
    "market_entry": "open of the first 1m bar at/after activation",
    "limit_entry": "marketable at activation -> fill at that open; else rest at the stated price, fill on touch",
    "range_entry": "two equal legs at both ends of the range; 'Entry 1/Entry 2' blocks are separate legs",
    "entry_validity_hours": 72,
    "cancel_rules": "TP1 touched before any fill -> cancel; stop touched before any fill -> cancel; "
                    "after TP1 the unfilled legs are cancelled",
    "stale_rule": "market entry already beyond TP1 or stop at activation -> not executable (counted)",
    "targets": "even split over listed targets unless the post states a split; 'open'/'+' keeps the last slice to the stop or expiry",
    "stop": "stop-market at the stated price (gap -> bar open); no stated stop -> none",
    "management": "only in-post rules (e.g. 'at TP1 book 50% and move SL to BE')",
    "max_hold_days": 14,
    "same_bar": "1m refinement of ambiguous 5m bars; still ambiguous -> adverse first; entry+TP1 ambiguous -> TP1 first (cancel)",
    "cost_bps_per_side": 10,
    "funding": "actual Binance USD-M funding on the open quantity",
    "overlap": "a new signal for a channel+symbol with an active order/position is skipped",
    "leverage_check": "stated (minimum) leverage, isolated, liquidation at entry*(1-1/L+0.5%) for longs",
    "null": "same coin, same relative entry/target/stop geometry, 58 start times every 12h in [-30d,-1d] on local 1h bars; "
            "signal re-run on the same 1h engine; excess = signal_1h - mean(null_1h)",
}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=1, default=str), encoding="utf-8")


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- parsing

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


def parse_next10x(p: dict) -> Signal | None:
    t = p["text"]
    if not re.search(r"LONG Below|Buy\s*:", t):
        return None
    m = re.search(r"#?\b([A-Z0-9]{2,15})\s?USDT\b", t) or re.search(r"#([A-Z0-9]{2,15})\b", t)
    if not m:
        return None
    sig = Signal("next10xgem1", p["message_id"], p["published_at"], p["edited_at"], m.group(1), 1, "limit", text=t)
    lim = re.search(r"(?:LONG Below|Buy)\s*:\s*([\d.]+)", t)
    sig.legs = [float(lim.group(1))]
    tp = re.search(r"(?:TAKE PROFIT|Sell)\s*:\s*([^\n]+)", t)
    sig.targets = numbers(tp.group(1).replace("|", " ")) if tp else []
    sl = re.search(r"\bSl\b[^\n]*", t)
    vals = numbers(sl.group(0)) if sl else []
    sig.stop = vals[-1] if vals else None
    sig.leverage = leverage_of(t)
    return sig


def parse_zetoshi(p: dict) -> Signal | None:
    t = p["text"]
    m = re.search(r"\b(LONG|SHORT)\b[^\n]*?-\s*\$?([A-Za-z0-9]{2,15})\s+([\d.]+)\s*\$?", t)
    if not m or not re.search(r"Take Profit", t):
        return None
    side = 1 if m.group(1) == "LONG" else -1
    sig = Signal("zetoshi", p["message_id"], p["published_at"], p["edited_at"], m.group(2).upper(), side, "limit", text=t)
    sig.legs = [float(m.group(3))]
    sig.targets = [numbers(x)[-1] for x in re.findall(r"Take Profit\s*\d\s*[-–]\s*([^\n]+)", t) if numbers(x)]
    sl = re.search(r"Stop\s*lo(?:ss|se)\s*[:\-–]*\s*([^\n]*)", t, re.I)
    if sl:
        vals = numbers(sl.group(1).split("(")[0])
        if vals:
            sig.stop = vals[0]
        elif re.search(r"no stop", sl.group(1), re.I):
            sig.stop_stated_none = True
    sig.leverage = leverage_of(t)
    return sig


def parse_rich(p: dict) -> Signal | None:
    t = p["text"]
    first = clean_line(t.split("\n")[0])
    if not re.search(r"\b(long|short|longing|relong)\b", first, re.I):
        return None
    if re.search(r"liquidated", first, re.I):
        return None
    sym = sym_from(t.split("\n")[0])
    side = side_of(first)
    if not sym or side is None:
        return None
    sig = Signal("richtrader", p["message_id"], p["published_at"], p["edited_at"], sym, side, "market", text=t)
    lines = t.split("\n")
    # entry on the header: "at 0.099", "around : 63,900 - 64,100", "at : $1679", "scalping trade at 62,400 - 62,200"
    head = re.split(r"stop\s*loss|stoploss|target", first, flags=re.I)[0]
    hm = re.search(r"\b(?:at|around)\s*:?\s*\$?([\d.,]+)(?:\s*-\s*\$?([\d.,]+))?", head, re.I)
    if hm:
        sig.entry_type = "limit"
        sig.legs = [v for v in (to_float(hm.group(1)), to_float(hm.group(2) or "")) if v]
    body = lines[1:] if hm else lines
    # inline "Target : 50 - 100%" on the header line
    tm = re.search(r"Target\s*:\s*([\d\s\-–]+)%", t)
    if tm:
        sig.target_pct_margin = numbers(tm.group(1))
    else:
        im = re.search(r"Target\s*:\s*([\d.,\s\-–]+)", first, re.I)
        if im:
            sig.targets += numbers(im.group(1))
    sm = re.search(r"Stoploss\s*(?:at)?\s*:?\s*\$?([\d.,]+)", first, re.I)
    if sm:
        sig.stop = to_float(sm.group(1))
    if len(lines) > 1:
        generic_body(body, sig)
    if re.search(r"No STOPLOSS", t, re.I):
        sig.stop_stated_none = True
    sig.leverage = leverage_of(t)
    if re.search(r"market price", t, re.I):
        sig.entry_type = "market"
        sig.legs = []
    if p["message_id"] == 5643:  # "Entry 2" block followed by an unlabelled target list
        sig.legs, sig.targets = [0.0439, 0.045], [0.0515, 0.053, 0.055, 0.06]
    return sig


# Wise Advice is mostly free-form BTC/ETH/gold scalps; labelled by hand from the
# text only (no prices consulted). None = no usable rule.
WISE_MANUAL = {
    27887: dict(symbol="ETH", side=-1, entry_type="limit", legs=[2390, 2397], stop=2426, note="1:2-1:3 RR, no numeric TP"),
    28002: dict(symbol="XAU", side=-1, entry_type="market", targets=[4690, 4681], stop=4610, note="stop below a short entry: invalid as written"),
    28026: dict(symbol="XAU", side=-1, entry_type="limit", legs=[4702], targets=[4691, 4681], stop=4714),
    28054: dict(symbol="XAU", side=1, entry_type="limit", legs=[4698, 4702], targets=[4709], stop=4691, open_ended=True),
    28056: dict(symbol="XAU", side=1, entry_type="limit", legs=[4685], targets=[4696], stop=4678, open_ended=True),
    28078: dict(symbol="BTC", side=1, entry_type="limit", legs=[79820], targets=[80180, 80490], stop=79460, open_ended=True),
    28114: dict(symbol="XAU", side=-1, entry_type="market", targets=[4548, 4540], stop=4562, open_ended=True),
    28136: dict(symbol="XAU", side=1, entry_type="limit", legs=[4548], targets=[4557, 4564], stop=4539, open_ended=True),
    28255: dict(symbol="ETH", side=1, entry_type="limit", legs=[2078, 2081], stop=2048, note="target 'good profit'"),
    28376: dict(symbol="BTC", side=1, entry_type="limit", legs=[68050], targets=[68550], stop=67550),
    28412: dict(symbol="BTC", side=1, entry_type="limit", legs=[65200], stop=64580, note="ETH leg has no stop"),
    28441: dict(symbol="BTC", side=1, entry_type="limit", legs=[60250], stop=59450, leverage=3, note="1:3 to 1:5, no numeric TP"),
    28572: dict(symbol="BTC", side=-1, entry_type="market", targets=[-300, -600, -900], stop=300, note="targets/stop as $ offsets"),
    28606: dict(symbol="XAU", side=1, entry_type="market", stop=4309, note="short scalp, no numeric TP"),
    28919: dict(symbol="BTC", side=-1, entry_type="limit", legs=[62900], stop=63510, rr_targets=[1, 2]),
    29003: dict(symbol="BTC", side=-1, entry_type="limit", legs=[64700], stop=65400, rr_targets=[1, 2]),
    29084: dict(symbol="BTC", side=-1, entry_type="market", stop=700, rr_targets=[1, 2, 5], note="stop as $ offset"),
    29508: dict(symbol="BZ", side=1, entry_type="limit", legs=[87.32], targets=[88.35, 89.59, 110.0], stop=86.0, leverage=10),
    29560: dict(symbol="PENDLE", side=1, entry_type="limit", legs=[1.46], targets=[1.5043, 1.55], stop=1.4238, leverage=10),
    29584: dict(symbol="BTC", side=1, entry_type="limit", legs=[77155.3], targets=[77716, 78400], stop=76525.5, leverage=10),
    30381: dict(symbol="ETH", side=-1, entry_type="limit", legs=[2700], targets=[2650, 1980.40], stop=2750),
    30519: dict(symbol="SKDD", side=1, entry_type="limit", legs=[7.149], targets=[8.285, 12.009, 16.286, 20.8], stop=6.769),
    29219: dict(symbol="PENDLE", side=1, entry_type="limit", legs=[1.42], targets=[1.4378, 1.4590], stop=1.4010, leverage=5, be=True, weights=[0.5, 0.5]),
    29232: dict(symbol="BTC", side=1, entry_type="market", targets=[65012, 65815], stop=64150, leverage=5, be=True, weights=[0.5, 0.5]),
    29287: dict(symbol="ETH", side=-1, entry_type="limit", legs=[1854], targets=[1839, 1821, 1803], stop=1869, leverage=10, be=True, weights=[0.5, 0.25, 0.25]),
    29306: dict(symbol="CL", side=1, entry_type="limit", legs=[79.43], targets=[81.0, 82.5], stop=77.8, leverage=10, be=True, weights=[0.5, 0.5]),
    29319: dict(symbol="ZEC", side=1, entry_type="limit", legs=[490], targets=[497.33, 505.79], stop=480.90, leverage=10, be=True, weights=[0.5, 0.5]),
    29367: dict(symbol="ADA", side=1, entry_type="limit", legs=[0.1997], targets=[0.2037, 0.2076], stop=0.1958, leverage=10, be=True, weights=[0.5, 0.5]),
}


def wise_signal(p: dict) -> Signal | None:
    spec = WISE_MANUAL.get(p["message_id"])
    if not spec:
        return None
    s = Signal("wise", p["message_id"], p["published_at"], p["edited_at"], spec["symbol"], spec["side"], spec["entry_type"],
               legs=list(spec.get("legs", [])), targets=list(spec.get("targets", [])), stop=spec.get("stop"),
               leverage=spec.get("leverage"), open_ended=spec.get("open_ended", False), be_after_tp1=spec.get("be", False),
               note=spec.get("note", ""), text=p["text"])
    s.target_weights = spec.get("weights")
    if spec.get("rr_targets"):
        s.note = (s.note + "; rr_targets=" + ",".join(map(str, spec["rr_targets"]))).strip("; ")
    return s


PARSERS = {"hub": parse_hub, "next10xgem1": parse_next10x, "zetoshi": parse_zetoshi, "richtrader": parse_rich, "wise": wise_signal}


def load_posts(channel: str) -> list[dict]:
    data = read_json(SOURCES[channel])
    posts = {p["message_id"]: p for p in data["posts"]}
    extra = FOLLOWUP_FILES.get(channel)
    if extra and (FOLLOWUP / extra).exists():
        for p in read_json(FOLLOWUP / extra)["posts"]:
            posts.setdefault(p["message_id"], p)
    return sorted(posts.values(), key=lambda p: p["published_at"])


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


def cmd_label(_args) -> None:
    rows, counts = [], {}
    for ch, parser in PARSERS.items():
        posts = load_posts(ch)
        got = []
        for p in posts:
            if not p.get("text"):
                continue
            s = parser(p)
            if s is None:
                continue
            d = s.__dict__.copy()
            d["completeness"] = classify(s)
            pub = pd.Timestamp(s.published_at)
            ed = pd.Timestamp(s.edited_at) if s.edited_at else None
            d["edit_lag_s"] = (ed - pub).total_seconds() if ed is not None else None
            d["point_in_time"] = ed is None or (ed - pub) <= pd.Timedelta(minutes=10)
            act = max(pub, ed) if ed is not None else pub
            d["activation"] = (act + pd.Timedelta(seconds=60)).isoformat()
            got.append(d)
        counts[ch] = {"posts": len(posts), "parsed": len(got), "by_completeness": Counter(g["completeness"] for g in got),
                      "point_in_time": sum(g["point_in_time"] for g in got),
                      "window": [posts[0]["published_at"], posts[-1]["published_at"]]}
        rows += got
    OUT.mkdir(parents=True, exist_ok=True)
    write_json(OUT / "labels_before_prices.json", rows)
    write_json(OUT / "label_counts.json", counts)
    with (OUT / "labels_review.txt").open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(f"[{r['channel']} {r['message_id']} {r['completeness']} pit={r['point_in_time']}] {r['symbol']} "
                    f"{'L' if r['side'] > 0 else 'S'} {r['entry_type']} legs={r['legs']} tp={r['targets']} "
                    f"pct={r['target_pct_margin']} sl={r['stop']} none={r['stop_stated_none']} lev={r['leverage']} "
                    f"open={r['open_ended']} be={r['be_after_tp1']} {r['note']}\n    "
                    + r["text"][:300].replace("\n", " | ") + "\n")
    freeze = {"frozen_at_utc": pd.Timestamp.now(tz="UTC").isoformat(), "rules": RULES,
              "labels_sha256": sha((OUT / "labels_before_prices.json").read_bytes()),
              "script_sha256": sha(Path(__file__).read_bytes())}
    write_json(OUT / "freeze.json", freeze)
    print(json.dumps(counts, indent=1, default=str))


# ---------------------------------------------------------------- market data

FAPI = "https://fapi.binance.com"
_LIMITER = None


def _session():
    from scripts.audit_official_timezone_reference import Limiter, session
    global _LIMITER
    if _LIMITER is None:
        _LIMITER = Limiter(3.0)
    return session(), _LIMITER


def api_get(path: str, params: dict):
    s, lim = _session()
    for attempt in range(3):
        lim.wait()
        try:
            r = s.get(FAPI + path, params=params, timeout=(6, 20))
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2)
            continue
        if r.status_code in (403, 418, 429):
            raise RuntimeError(f"HTTP {r.status_code}: stop")
        return r.status_code, r.json()
    raise RuntimeError("unreachable")


EXINFO = None


def native_symbol(sym: str) -> tuple[str | None, float]:
    """Binance USD-M contract for a ticker, with its price multiplier (1000-prefixed contracts)."""
    global EXINFO
    if EXINFO is None:
        cache = OUT / "market/exchangeInfo.json"
        if cache.exists():
            EXINFO = read_json(cache)
        else:
            _, payload = api_get("/fapi/v1/exchangeInfo", {})
            EXINFO = {s["symbol"]: {"status": s["status"], "onboardDate": s.get("onboardDate"), "type": s.get("contractType")}
                      for s in payload["symbols"] if s["quoteAsset"] == "USDT"}
            write_json(cache, EXINFO)
    local = {p.name[:-5] for p in LOCAL.glob("*_USDT")}
    for name, mult in ((sym, 1.0), ("1000" + sym, 1000.0), ("1000000" + sym, 1e6)):
        if name + "USDT" in EXINFO or name in local:
            return name + "USDT", mult
    return None, 1.0


def klines(native: str, interval: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Official klines [start, end) with an on-disk cache keyed by the exact request."""
    folder = OUT / "market" / native
    folder.mkdir(parents=True, exist_ok=True)
    key = f"{interval}_{int(start.timestamp())}_{int(end.timestamp())}.json.gz"
    path = folder / key
    if path.exists():
        rows = json.loads(gzip.decompress(path.read_bytes()))
    else:
        rows, cursor, stop = [], int(start.timestamp() * 1000), int(end.timestamp() * 1000) - 1
        step = {"1m": 60_000, "5m": 300_000}[interval]
        while cursor <= stop:
            status, page = api_get("/fapi/v1/klines", {"symbol": native, "interval": interval, "startTime": cursor,
                                                        "endTime": stop, "limit": 1500})
            if status != 200:
                rows = {"error": page}
                break
            if not page:
                break
            rows += page
            cursor = int(page[-1][0]) + step
            if len(page) < 1500:
                break
        path.write_bytes(gzip.compress(json.dumps(rows).encode()))
    if isinstance(rows, dict) or not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    f = pd.DataFrame([[float(x) for x in r[1:5]] for r in rows], columns=["open", "high", "low", "close"],
                     index=pd.to_datetime([int(r[0]) for r in rows], unit="ms"))
    return f[~f.index.duplicated()].sort_index()


def local_frame(native: str, name: str) -> pd.DataFrame:
    path = LOCAL / (native[:-4] + "_USDT") / f"{name}.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


FUNDING_CACHE: dict = {}


def funding(native: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    if native not in FUNDING_CACHE:
        f = local_frame(native, "funding_rate")
        series = pd.Series(dtype=float)
        if not f.empty:
            series = f["funding_rate"].copy()
            series.index = pd.DatetimeIndex(series.index).floor("min")
        path = OUT / "market" / native / "funding_api.json.gz"
        if path.exists():
            rows = json.loads(gzip.decompress(path.read_bytes()))
        else:
            rows = []
            if native in (EXINFO or {}):
                # limit-only form: a startTime window on fundingRate is blocked by the WAF (HTTP 403)
                status, page = api_get("/fapi/v1/fundingRate", {"symbol": native, "limit": 1000})
                if status == 200 and page:
                    rows = page
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(gzip.compress(json.dumps(rows).encode()))
        if rows:
            api = pd.Series([float(r["fundingRate"]) for r in rows],
                            index=pd.to_datetime([int(r["fundingTime"]) for r in rows], unit="ms").floor("min"))
            series = pd.concat([series, api])
            series = series[~series.index.duplicated(keep="last")].sort_index()
        FUNDING_CACHE[native] = series
    s = FUNDING_CACHE[native]
    return s[(s.index >= start) & (s.index < end)] if len(s) else s


# ---------------------------------------------------------------- engine

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
        # Settle through the open of bar t_end (funding times are floored to the minute): a position
        # still open at a settlement owes it even if it closes inside that bar or expires at the
        # next open. The frozen copy in core/research/kol_bracket_engine.py keeps "<" by design.
        while self._next_fund < len(self.fund) and self.fund.index[self._next_fund] <= t_end:
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


# ---------------------------------------------------------------- simulate

HOLD = pd.Timedelta(days=14)
ENTRY_VALID = pd.Timedelta(hours=72)
# last complete hour when the run starts (override with KOL_DATA_END for an exact rerun); recorded in run_meta.json
DATA_END = pd.Timestamp(os.environ.get("KOL_DATA_END") or (pd.Timestamp.now(tz="UTC").tz_convert(None).floor("h") - pd.Timedelta(hours=1)))


def signal_bars(native: str, act: pd.Timestamp):
    """1m for the first two hours, 5m afterwards up to the hold limit; falls back to local 1h."""
    # always fetch the 14-day window (stable cache key); shorter-hold variants stop early via the engine expiry
    end = min(act + pd.Timedelta(days=14, minutes=5), DATA_END)
    a1 = act.floor("min")
    m1 = klines(native, "1m", a1, a1 + pd.Timedelta(hours=2))
    m5 = klines(native, "5m", (a1 + pd.Timedelta(hours=2)).floor("5min"), end) if end > a1 + pd.Timedelta(hours=2) else m1.iloc[:0]
    if m1.empty:
        h = local_frame(native, "1h")
        h = h[(h.index >= act.floor("h")) & (h.index < end)][["open", "high", "low", "close"]]
        return [(h, pd.Timedelta(hours=1))], "local_1h"
    return [(m1, pd.Timedelta(minutes=1)), (m5[m5.index >= a1 + pd.Timedelta(hours=2)], pd.Timedelta(minutes=5))], "api_1m_5m"


def make_finer(native):
    def finer(t, bar_len):
        if bar_len == pd.Timedelta(minutes=5):
            f = klines(native, "1m", t, t + bar_len)
            return f if len(f) > 1 else None
        return None
    return finer


def simulate_one(sig: dict, leverage_mode: bool = False):
    native, mult = native_symbol(sig["symbol"] if sig["symbol"] != "XAU" else "XAU")
    if native is None:
        return {"status": "no_contract"}
    act = pd.Timestamp(sig["activation"]).tz_convert(None)
    if act + HOLD > DATA_END:
        return {"status": "immature", "native": native}
    segments, source = signal_bars(native, act)
    first = segments[0][0]
    # same bar the engine fills a market order on (first bar opening at/after activation)
    first = first[first.index >= act.ceil(segments[0][1])]
    if first.empty:
        return {"status": "no_market_data", "native": native}
    ref = float(first.iloc[0]["open"])
    lev = sig.get("leverage") if leverage_mode else None
    if leverage_mode and not lev:
        return {"status": "no_stated_leverage"}
    plan, why = build_plan(sig, ref, mult, lev)
    if plan is None:
        return {"status": "invalid_geometry", "reason": why, "native": native, "ref": ref}
    eng = Engine(plan, act, act + HOLD, act + ENTRY_VALID, finer=make_finer(native) if source != "local_1h" else None,
                 funding_series=funding(native, act - pd.Timedelta(hours=8), act + HOLD + pd.Timedelta(hours=1)))
    for bars, blen in segments:
        eng.run(bars, blen)
        if eng.status in ("cancelled", "stale", "closed", "expired_unfilled"):
            break
    if eng.status in ("open", "pending") and eng.qty > 0:
        last = segments[-1][0]
        eng._exit(last.index[-1], float(last.iloc[-1]["close"]), eng.qty, "expiry")
    elif eng.status == "pending":
        eng.status = "expired_unfilled"
    out = eng.result()
    out.update({"native": native, "mult": mult, "ref": ref, "source": source, "plan_check": why,
                "plan": {"legs": plan.legs, "targets": plan.targets, "weights": plan.weights, "stop": plan.stop,
                         "market": plan.entry_market, "be": plan.be_after_tp1, "liq_leverage": plan.liq_leverage}})
    if leverage_mode and out.get("ret_on_filled") is not None:
        out["ret_on_margin"] = max(-1.0, out["ret_on_filled"] * lev)
    return out


def relative_plan(sig_plan: dict, ref: float) -> dict:
    rel = lambda xs: [x / ref for x in xs]
    return {"legs": rel(sig_plan["legs"]), "targets": rel(sig_plan["targets"]), "weights": sig_plan["weights"],
            "stop": sig_plan["stop"] / ref if sig_plan["stop"] else None, "market": sig_plan["market"], "be": sig_plan["be"]}


def run_1h(native, side, relp, start, h, fund):
    bars = h[(h.index >= start) & (h.index < start + HOLD + pd.Timedelta(hours=1))]
    if len(bars) < HOLD / pd.Timedelta(hours=1):
        return None
    ref = float(bars.iloc[0]["open"])
    plan = Plan(side, relp["market"], [x * ref for x in relp["legs"]], [x * ref for x in relp["targets"]], relp["weights"],
                relp["stop"] * ref if relp["stop"] else None, relp["be"])
    eng = Engine(plan, start, start + HOLD, start + ENTRY_VALID, funding_series=fund)
    eng.run(bars, pd.Timedelta(hours=1))
    if eng.status in ("open", "pending") and eng.qty > 0:
        eng._exit(bars.index[-1], float(bars.iloc[-1]["close"]), eng.qty, "expiry")
    r = eng.result()
    return r["ret_on_filled"] if r["ret_on_filled"] is not None else 0.0, r["filled_fraction"] > 0


def cmd_simulate(_args) -> None:
    labels = read_json(OUT / "labels_before_prices.json")
    freeze = read_json(OUT / "freeze.json")
    assert sha((OUT / "labels_before_prices.json").read_bytes()) == freeze["labels_sha256"], "labels changed after freeze"
    usable = {"full_bracket", "targets_no_stop_by_design", "stop_without_targets", "targets_without_stop"}
    results, active = [], {}
    for i, sig in enumerate(labels):
        rec = {k: sig[k] for k in ("channel", "message_id", "published_at", "activation", "symbol", "side", "entry_type",
                                   "completeness", "point_in_time", "leverage", "edit_lag_s")}
        if sig["completeness"] not in usable:
            rec["status"] = "not_executable_" + sig["completeness"]
            results.append(rec)
            continue
        key = (sig["channel"], sig["symbol"], sig["point_in_time"])
        act = pd.Timestamp(sig["activation"]).tz_convert(None)
        if key in active and active[key] > act:
            rec["status"] = "skipped_overlap"
            results.append(rec)
            continue
        try:
            r = simulate_one(sig)
        except Exception as exc:  # keep going; record the failure
            r = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        rec.update(r)
        if r.get("status") in ("closed", "open") or r.get("first_fill"):
            end = pd.Timestamp(r["exit_time"]) if r.get("exit_time") else act + HOLD
            active[key] = end
        elif r.get("status") in ("cancelled", "expired_unfilled"):
            active[key] = act  # order no longer live
        if r.get("filled_fraction") and sig.get("leverage"):
            try:
                lv = simulate_one(sig, leverage_mode=True)
                rec["lev"] = {k: lv.get(k) for k in ("exit_reason", "ret_on_filled", "ret_on_margin", "status")}
            except Exception as exc:
                rec["lev"] = {"error": str(exc)}
        # same-coin null on local 1h bars
        if r.get("plan"):
            rec["null"] = null_for(sig, r)
        results.append(rec)
        if i % 20 == 0:
            print(i, len(labels), sig["channel"], sig["symbol"], rec.get("status"), flush=True)
            write_json(OUT / "trades_partial.json", results)
    write_json(OUT / "trades.json", results)
    print("done", Counter(r.get("status") for r in results))


def cmd_sensitivity(_args) -> None:
    """Limit-entry variants added after the primary run (documented deviations, not the frozen rules)."""
    global DEEP_REST, ENTRY_VALID
    labels = {(s["channel"], s["message_id"]): s for s in read_json(OUT / "labels_before_prices.json")}
    rows = read_json(OUT / "trades.json")
    out = {}
    for name, deep, valid in (("deep_rest_72h", True, pd.Timedelta(hours=72)), ("deep_rest_14d", True, pd.Timedelta(days=14))):
        DEEP_REST, ENTRY_VALID = deep, valid
        res = []
        for rec in rows:
            if rec["entry_type"] != "limit" or not rec.get("native") or rec.get("status") in ("immature", "skipped_overlap", "no_contract"):
                continue
            if rec.get("status") == "invalid_geometry":
                continue
            sig = labels[(rec["channel"], rec["message_id"])]
            r = simulate_one(sig)
            item = {k: rec[k] for k in ("channel", "message_id", "symbol", "point_in_time", "activation")}
            item.update({k: r.get(k) for k in ("status", "exit_reason", "ret_on_filled", "filled_fraction", "tps_hit", "first_fill")})
            if r.get("plan"):
                item["null"] = null_for(sig, r)
            res.append(item)
        out[name] = res
        print(name, Counter((x["channel"], x["status"]) for x in res), flush=True)
    DEEP_REST, ENTRY_VALID = False, pd.Timedelta(hours=72)
    write_json(OUT / "sensitivity_limits.json", out)


# Improvement variants, declared before any variant result was computed (2026-10-30). Each one is
# applied identically to the signal and to both same-coin baselines.
VARIANTS = {
    "as_written": {},
    "be_after_tp1": {"be": True},
    "tp1_only": {"tp1_only": True},
    "hold_72h": {"hold": pd.Timedelta(hours=72)},
    "hold_24h": {"hold": pd.Timedelta(hours=24)},
}
FILTERS = {
    "skip_runup_1pct": "30-minute move before publication in the signal direction >= 1% -> skip",
    "skip_tp1_under_0.3pct": "distance from the fill reference to TP1 < 0.3% -> skip (round-trip cost is 0.2%)",
}


def variant_sig(sig: dict, spec: dict) -> dict:
    s = dict(sig)
    if spec.get("be"):
        s["be_after_tp1"] = True
    if spec.get("tp1_only") and s["targets"]:
        s["targets"] = [sorted(s["targets"], key=lambda x: s["side"] * x)[0]]
        s["target_weights"], s["open_ended"] = [1.0], False
    return s


def pre_move(native: str, sig: dict) -> float | None:
    """Signal-direction return over the 30 minutes before publication (observable at entry)."""
    pub = pd.Timestamp(sig["published_at"]).tz_convert(None).floor("min")
    f = klines(native, "1m", pub - pd.Timedelta(minutes=30), pub + pd.Timedelta(minutes=1))
    if len(f) < 20:
        return None
    return sig["side"] * (float(f.iloc[-1]["open"]) / float(f.iloc[0]["open"]) - 1)


def cmd_improve(_args) -> None:
    global HOLD
    labels = {(s["channel"], s["message_id"]): s for s in read_json(OUT / "labels_before_prices.json")}
    base = [r for r in read_json(OUT / "trades.json") if r.get("plan")]
    out = {"declared": {"variants": {k: {kk: str(vv) for kk, vv in v.items()} for k, v in VARIANTS.items()},
                        "filters": FILTERS, "risk_sizing": "notional = 1% equity / stop distance at fill; account return = 1% x R"},
           "rows": []}
    feats = {}
    for r in base:
        sig = labels[(r["channel"], r["message_id"])]
        t1 = r["plan"]["targets"][0] if r["plan"]["targets"] else None
        feats[(r["channel"], r["message_id"])] = {
            "pre_move_30m": pre_move(r["native"], sig),
            "tp1_dist": abs(t1 / r["ref"] - 1) if t1 else None,
            "stop_dist": abs(r["plan"]["stop"] / (r.get("avg_entry") or r["ref"]) - 1) if r["plan"]["stop"] else None}
    for name, spec in VARIANTS.items():
        HOLD = spec.get("hold", pd.Timedelta(days=14))
        for r in base:
            key = (r["channel"], r["message_id"])
            sig = variant_sig(labels[key], spec)
            v = simulate_one(sig) if name != "as_written" else r
            row = {"variant": name, "channel": r["channel"], "message_id": r["message_id"], "symbol": r["symbol"],
                   "activation": r["activation"], "point_in_time": r["point_in_time"], **feats[key],
                   **{k: v.get(k) for k in ("status", "exit_reason", "ret_on_filled", "filled_fraction", "tps_hit")}}
            n = null_for(sig, v) if v.get("plan") else None
            row.update({k: (n or {}).get(k) for k in ("signal_1h", "null_mean", "excess", "null_post_mean", "excess_post")})
            out["rows"].append(row)
        print(name, "done", flush=True)
        write_json(OUT / "improve.json", out)
    HOLD = pd.Timedelta(days=14)


def block_p(values: pd.Series, weeks: pd.Series, reps: int = 4000, seed: int = 11) -> float | None:
    """Two-sided week-block bootstrap p-value for mean == 0 (recentred)."""
    rng = np.random.default_rng(seed)
    groups = [g.to_numpy() - values.mean() for _, g in values.groupby(weeks.values)]
    if len(groups) < 3:
        return None
    obs = abs(values.mean())
    hits = 0
    for _ in range(reps):
        pick = rng.integers(0, len(groups), len(groups))
        hits += abs(np.concatenate([groups[j] for j in pick]).mean()) >= obs
    return (hits + 1) / (reps + 1)


def cmd_improve_report(_args) -> None:
    data = read_json(OUT / "improve.json")
    df = pd.DataFrame(data["rows"])
    df = df[df["point_in_time"]]
    df["week"] = pd.to_datetime(df["activation"]).dt.tz_convert(None).dt.to_period("W").astype(str)
    masks = {"all": lambda d: d.index == d.index,
             "skip_runup_1pct": lambda d: ~(d["pre_move_30m"].fillna(0) >= 0.01),
             "skip_tp1_under_0.3pct": lambda d: ~(d["tp1_dist"].fillna(1) < 0.003)}
    cells = []
    for ch, g in df.groupby("channel"):
        for var, gv in g.groupby("variant"):
            for fname, m in masks.items():
                d = gv[m(gv)].sort_values("activation")
                traded = d[d["ret_on_filled"].notna()]
                ex = d[d["excess_post"].notna()]
                if len(traded) < 5:
                    continue
                r = traded["ret_on_filled"].astype(float)
                half = len(r) // 2
                cell = {"channel": ch, "variant": var, "filter": fname, "trades": len(traded), "mean": r.mean(),
                        "median": r.median(), "first_half": r.iloc[:half].mean(), "second_half": r.iloc[half:].mean(),
                        "excess_pre": d["excess"].dropna().mean(), "excess_post": ex["excess_post"].mean() if len(ex) else None,
                        "excess_post_ci": block_ci(ex["excess_post"].astype(float).reset_index(drop=True),
                                                   ex["week"].reset_index(drop=True)) if len(ex) >= 5 else None,
                        "p_post": block_p(ex["excess_post"].astype(float).reset_index(drop=True),
                                          ex["week"].reset_index(drop=True)) if len(ex) >= 5 else None}
                cells.append(cell)
    # Benjamini-Hochberg over every cell that has a p-value
    ps = sorted([(c["p_post"], i) for i, c in enumerate(cells) if c["p_post"] is not None])
    m = len(ps)
    q_prev = 1.0
    for rank, (p, i) in reversed(list(enumerate(ps, 1))):
        q_prev = min(q_prev, p * m / rank)
        cells[i]["q_post"] = q_prev
    # risk-based sizing on the as-written trades: 1% of equity at risk per trade
    base = df[(df["variant"] == "as_written") & df["ret_on_filled"].notna() & df["stop_dist"].notna()]
    sizing = {}
    for ch, g in base.groupby("channel"):
        R = g["ret_on_filled"].astype(float) / g["stop_dist"].astype(float)
        sizing[ch] = {"trades": len(g), "mean_R": R.mean(), "median_R": R.median(),
                      "account_ret_per_trade_at_1pct_risk": 0.01 * R.mean(), "worst_R": R.min(),
                      "median_notional_x_equity": float((0.01 / g["stop_dist"].astype(float)).median())}
    write_json(OUT / "improve_summary.json", {"cells": cells, "risk_sizing": sizing, "declared": data["declared"]})
    show = pd.DataFrame(cells)
    pd.set_option("display.width", 250)
    for col in ("mean", "median", "first_half", "second_half", "excess_pre", "excess_post"):
        show[col] = (show[col].astype(float) * 100).round(2)
    print(show.drop(columns=["excess_post_ci"]).to_string())
    print(json.dumps(sizing, indent=1, default=str))


def cmd_retry(_args) -> None:
    """Re-run records that failed on a transient fetch error, in place."""
    labels = {(s["channel"], s["message_id"]): s for s in read_json(OUT / "labels_before_prices.json")}
    rows = read_json(OUT / "trades.json")
    for rec in rows:
        if rec.get("status") != "error" and rec.get("reason") != "price_scale_mismatch":
            continue
        rec.pop("reason", None)
        sig = labels[(rec["channel"], rec["message_id"])]
        r = simulate_one(sig)
        rec.pop("error", None)
        rec.update(r)
        if r.get("filled_fraction") and sig.get("leverage"):
            lv = simulate_one(sig, leverage_mode=True)
            rec["lev"] = {k: lv.get(k) for k in ("exit_reason", "ret_on_filled", "ret_on_margin", "status")}
        if r.get("plan"):
            rec["null"] = null_for(sig, r)
        print(rec["channel"], rec["symbol"], rec["status"], flush=True)
    write_json(OUT / "trades.json", rows)


def null_for(sig: dict, r: dict) -> dict | None:
    native = r["native"]
    h = local_frame(native, "1h")
    if h.empty:
        return None
    h = h[["open", "high", "low", "close"]]
    act = pd.Timestamp(sig["activation"]).tz_convert(None)
    relp = relative_plan(r["plan"], r["ref"])
    fund = funding(native, act - pd.Timedelta(days=31), act + HOLD + pd.Timedelta(days=1))
    sig_start = act.ceil("h")
    s = run_1h(native, sig["side"], relp, sig_start, h, fund)
    vals, fills = [], []
    for k in range(58):
        start = (act - pd.Timedelta(days=30) + pd.Timedelta(hours=12 * k)).ceil("h")
        out = run_1h(native, sig["side"], relp, start, h, fund)
        if out is not None:
            vals.append(out[0])
            fills.append(out[1])
    # Second baseline after the signal: a pre-signal window contains the pump a late caller reacts to.
    post = []
    fund_post = funding(native, act, act + pd.Timedelta(days=45))
    for k in range(58):
        start = (act + pd.Timedelta(days=1) + pd.Timedelta(hours=12 * k)).ceil("h")
        out = run_1h(native, sig["side"], relp, start, h, fund_post)
        if out is not None:
            post.append(out[0])
    rec = {"n": len(vals), "signal_1h": s[0] if s else None, "n_post": len(post)}
    if s is None:
        return rec
    rec["signal_1h_filled"] = s[1]
    if len(vals) >= 20:
        rec.update({"null_mean": float(np.mean(vals)), "null_fill_rate": float(np.mean(fills)),
                    "excess": s[0] - float(np.mean(vals))})
    if len(post) >= 20:
        rec.update({"null_post_mean": float(np.mean(post)), "excess_post": s[0] - float(np.mean(post))})
    return rec


def cmd_nulls(_args) -> None:
    """Recompute both same-coin baselines for every planned record (local 1h data only)."""
    labels = {(s["channel"], s["message_id"]): s for s in read_json(OUT / "labels_before_prices.json")}
    rows = read_json(OUT / "trades.json")
    for rec in rows:
        if rec.get("plan"):
            rec["null"] = null_for(labels[(rec["channel"], rec["message_id"])], rec)
    write_json(OUT / "trades.json", rows)
    sens = read_json(OUT / "sensitivity_limits.json")
    print("trades done", flush=True)
    global DEEP_REST, ENTRY_VALID
    for name, res in sens.items():
        DEEP_REST, ENTRY_VALID = True, pd.Timedelta(hours=72) if name.endswith("72h") else pd.Timedelta(days=14)
        for item in res:
            if item.get("null") is not None:
                sig = labels[(item["channel"], item["message_id"])]
                r = simulate_one(sig)
                item["null"] = null_for(sig, r) if r.get("plan") else None
    DEEP_REST, ENTRY_VALID = False, pd.Timedelta(hours=72)
    write_json(OUT / "sensitivity_limits.json", sens)


# ---------------------------------------------------------------- report

def block_ci(values: pd.Series, weeks: pd.Series, reps: int = 4000, seed: int = 7):
    rng = np.random.default_rng(seed)
    groups = [g.to_numpy() for _, g in values.groupby(weeks)]
    if len(groups) < 3:
        return (None, None)
    means = []
    for _ in range(reps):
        pick = rng.integers(0, len(groups), len(groups))
        allv = np.concatenate([groups[j] for j in pick])
        means.append(allv.mean())
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def claimed_hits(channel: str) -> dict:
    """Replies such as 'All Target done' / '2 TP done' / 'Tp 2 Done' keyed by the signal they answer."""
    out: dict = {}
    for p in load_posts(channel):
        ref, t = p.get("reply_to_id"), p.get("text") or ""
        if not ref:
            continue
        if re.search(r"\ball\s*(tp|target)s?\b.*\b(done|hit|reach)|\b(full|final) target", t, re.I):
            n = 99
        else:
            m = re.search(r"(\d)\s*(?:st|nd|rd|th)?\s*(?:tp|target)s?\b.*?(?:done|hit|reach)|(?:tp|target)\s*(\d)\s*(?:done|hit|reach)", t, re.I) \
                or re.search(r"(\d)\s*/\s*\d\s*targets", t, re.I)
            if not m:
                continue
            n = int(next(g for g in m.groups() if g))
        out[ref] = max(out.get(ref, 0), n)
    return out


def cmd_report(_args) -> None:
    rows = read_json(OUT / "trades.json")
    df = pd.DataFrame(rows)
    claims = {ch: claimed_hits(ch) for ch in SOURCES}
    df["claimed_tps"] = [claims[c].get(m) for c, m in zip(df["channel"], df["message_id"])]
    summary = {}
    for (ch, pit), g in df.groupby(["channel", "point_in_time"]):
        traded = g[g["ret_on_filled"].notna()] if "ret_on_filled" in g else g.iloc[:0]
        rec = {"signals": int(len(g)), "status": g["status"].value_counts().to_dict()}
        if len(traded):
            r = traded["ret_on_filled"].astype(float)
            wk = pd.to_datetime(traded["activation"]).dt.tz_convert(None).dt.to_period("W").astype(str)
            rec.update({"trades": int(len(traded)), "mean": r.mean(), "median": r.median(), "win_rate": (r > 0).mean(),
                        "ci95_week_block": block_ci(r, wk), "exit_reasons": traded["exit_reason"].value_counts().to_dict(),
                        "tp1_hit_rate": (traded["tps_hit"] >= 1).mean(),
                        "mean_drop_best_5pct": r[r < r.quantile(0.95)].mean() if len(r) >= 20 else None,
                        "first_half_mean": r.iloc[: len(r) // 2].mean(), "second_half_mean": r.iloc[len(r) // 2:].mean(),
                        "funding_mean": traded["funding"].astype(float).mean(),
                        "cost_2x_mean": (r - 0.002).mean()})
            rec["ambiguous_bars_unresolved"] = int(traded["ambiguous_unresolved"].fillna(0).sum())
            rec["trades_with_unresolved_bar"] = int((traded["ambiguous_unresolved"].fillna(0) > 0).sum())
            nulls = list(traded["null"]) if "null" in traded else [None] * len(traded)
            for key, base, tag in (("excess", "null_mean", ""), ("excess_post", "null_post_mean", "_post")):
                keep = [i for i, n in enumerate(nulls) if isinstance(n, dict) and n.get(key) is not None]
                if not keep:
                    continue
                ex = pd.Series([nulls[i][key] for i in keep], dtype=float)
                wk2 = pd.Series([wk.iloc[i] for i in keep])
                rec.update({f"null{tag}_n": len(keep), f"excess{tag}_mean": ex.mean(), f"excess{tag}_median": ex.median(),
                            f"excess{tag}_ci95_week_block": block_ci(ex, wk2),
                            f"signal_1h{tag}_mean": float(np.mean([nulls[i]["signal_1h"] for i in keep])),
                            f"null{tag}_mean": float(np.mean([nulls[i][base] for i in keep]))})
            claimed = traded[traded["claimed_tps"].notna()]
            if len(claimed):
                rec.update({"claimed_n": int(len(claimed)),
                            "claimed_but_sim_no_tp": int((claimed["tps_hit"] == 0).sum()),
                            "claimed_but_sim_stopped": int(claimed["exit_reason"].isin(["stop", "liquidated"]).sum()),
                            "claimed_mean_sim_ret": float(claimed["ret_on_filled"].astype(float).mean()),
                            "unclaimed_mean_sim_ret": float(traded[traded["claimed_tps"].isna()]["ret_on_filled"].astype(float).mean())})
            if "lev" in traded:
                lv = traded["lev"].dropna()
                lv = [x for x in lv if isinstance(x, dict) and x.get("ret_on_margin") is not None]
                if lv:
                    rec.update({"lev_n": len(lv), "lev_liquidated": sum(x.get("exit_reason") == "liquidated" for x in lv),
                                "lev_mean_ret_on_margin": float(np.mean([x["ret_on_margin"] for x in lv]))})
        summary[f"{ch}|{'pit' if pit else 'late_edit'}"] = rec
    write_json(OUT / "summary.json", summary)
    print(json.dumps(summary, indent=1, default=str))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["label", "simulate", "retry", "sensitivity", "nulls", "improve", "improve_report", "report"])
    args = ap.parse_args()
    {"label": cmd_label, "simulate": cmd_simulate, "retry": cmd_retry, "sensitivity": cmd_sensitivity, "nulls": cmd_nulls, "improve": cmd_improve, "improve_report": cmd_improve_report, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
