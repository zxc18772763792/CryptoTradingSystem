"""Four Telegram KOL channels the user joined on 2026-10-25: history snapshot, price-blind
call parsing, then an offline event study on local Binance perpetual 1h bars.

Subcommands:
  collect   read-only Telethon snapshot of the selected channels (existing DPAPI session,
            one reader.lock hold per channel, no joins/sends/media downloads)
  label     deterministic call parser -> events_before_prices.json (sealed with SHA-256
            before any price is read)
  evaluate  same-coin random-time null, 20bps round trip, shorts (entry-exit)/entry,
            week-clustered intervals; writes results.json / trades.csv

Raw messages stay in the restricted NTFS study directory outside the repo (F: is exFAT
and does not enforce ACLs). Nothing here registers a tracker or touches the news flow.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STUDY = Path.home() / ".crypto_trading_system/research/telegram_new_kols_20261025"
START = datetime(2026, 4, 28, 5, tzinfo=timezone.utc)   # 180 days, same left edge as the 10/25 three-source study
CAP = 8000


def sha(data: bytes | str) -> str:
    return hashlib.sha256(data.encode("utf-8") if isinstance(data, str) else data).hexdigest()


def write_new(path: Path, value) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)


# --------------------------------------------------------------------------- collect

async def collect_channel(selection: dict, cutoff: datetime, out: Path) -> dict:
    import scripts.telegram_desktop_bridge as bridge
    from telethon import functions
    from telethon.sessions import StringSession
    payload = json.loads(bridge.dpapi(bridge.store_path().read_bytes(), decrypt=True))
    client = bridge.make_client(StringSession(payload["session"]), payload["api_id"], payload["api_hash"])
    try:
        await bridge.connect(client)
        me = await client.get_me()
        if not me or me.id != payload["account_id"]:
            raise RuntimeError("Independent account identity mismatch")
        entity = None
        async for dialog in client.iter_dialogs(limit=500):
            if dialog.entity.id == selection["channel_id"]:
                entity = dialog.entity
                break
        if entity is None:
            raise RuntimeError("Selected channel is not among joined dialogs")
        full = await client(functions.channels.GetFullChannelRequest(entity))
        posts, boundary = [], None
        began = datetime.now(timezone.utc).isoformat()
        async for m in client.iter_messages(entity, offset_date=cutoff, limit=CAP + 1, wait_time=1.2):
            if m.date >= cutoff:
                continue
            if m.date < START:
                boundary = m.date.isoformat()
                break
            if len(posts) >= CAP:
                break
            text = m.message or ""
            fwd = m.fwd_from
            posts.append({
                "message_id": m.id, "published_at": m.date.astimezone(timezone.utc).isoformat(),
                "edited_at": m.edit_date.astimezone(timezone.utc).isoformat() if m.edit_date else None,
                "text": text, "text_sha256": sha(text), "has_media": bool(m.media),
                "media_type": type(m.media).__name__ if m.media else None,
                "reply_to_id": m.reply_to_msg_id, "forwarded": bool(fwd),
                "forward_origin_date": fwd.date.astimezone(timezone.utc).isoformat() if fwd and fwd.date else None,
                "views": m.views, "post_author": m.post_author,
            })
        coverage = {
            "slug": selection["slug"], "title": selection["title"], "username": selection["username"],
            "joined_at": selection["joined_at"], "participants": full.full_chat.participants_count,
            "about": full.full_chat.about, "start": START.isoformat(), "cutoff": cutoff.isoformat(),
            "rows": len(posts), "cap": CAP, "cap_reached": len(posts) >= CAP,
            "oldest": min((p["published_at"] for p in posts), default=None),
            "newest": max((p["published_at"] for p in posts), default=None),
            "older_boundary_observed": boundary,
            "full_window_surviving_history": boundary is not None or len(posts) < CAP,
            "collected_from": began, "collected_until": datetime.now(timezone.utc).isoformat(),
            "collection_mode": "retrospective_snapshot_not_forward", "media_downloaded": False,
        }
        write_new(out / f"{selection['slug']}_history.json", {"coverage": coverage, "posts": posts})
        return coverage
    finally:
        await client.disconnect()


def collect(study: Path) -> None:
    import scripts.telegram_desktop_bridge as bridge
    logging.disable(logging.CRITICAL)
    bridge.protect_directory(study)
    selections = json.loads((study / "selection.json").read_text(encoding="utf-8"))
    cutoff = datetime.fromisoformat(selections["cutoff"])
    summary = []
    for selection in selections["channels"]:
        path = study / f"{selection['slug']}_history.json"
        if path.exists():
            summary.append(json.loads(path.read_text(encoding="utf-8"))["coverage"])
            continue
        try:
            # One lock hold per channel so the 5-minute forward runner can interleave.
            with bridge.session_lock():
                coverage = asyncio.run(asyncio.wait_for(collect_channel(selection, cutoff, study), timeout=600))
        except Exception as exc:
            error = {"slug": selection["slug"], "status": "failed", "error_type": type(exc).__name__}
            if hasattr(exc, "seconds"):
                error["cooldown_seconds"] = exc.seconds
            print(json.dumps(error), flush=True)
            raise SystemExit(1)
        summary.append(coverage)
        print(json.dumps({k: coverage[k] for k in ("slug", "rows", "oldest", "newest", "cap_reached",
                                                   "full_window_surviving_history")}), flush=True)
    (study / "history_collection.json").write_text(json.dumps(
        {"identity_verified": True, "sources": summary,
         "selection_sha256": sha((study / "selection.json").read_bytes())}, ensure_ascii=False, indent=2),
        encoding="utf-8")


# --------------------------------------------------------------------------- label (price-blind)

SLUGS = ("market_dealer", "next10xgem1", "wiseadvicebysumit", "therichtrader_reborn")
LATE_EDIT = pd.Timedelta(minutes=10)
NAME_TO_SYMBOL = {"BITCOIN": "BTC", "ETHEREUM": "ETH", "ETHER": "ETH", "SOLANA": "SOL"}
PLAIN_MAJORS = {"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE", "BITCOIN", "ETHEREUM", "ETHER", "SOLANA"}
NOT_TICKERS = {
    "NFA", "DYOR", "BREAKING", "CRYPTO", "ALTSEASON", "ALTCOINS", "ALTCOIN", "ALTS", "NEWS", "UPDATE",
    "USDT", "USDC", "USD", "FED", "CPI", "FOMC", "ETF", "ETFS", "AI", "VIP", "TP", "SL", "ATH",
    "GOLD", "SILVER", "XAU", "XAG", "OIL", "SPX", "NASDAQ", "DXY", "NVDA", "TSLA", "MSTR", "AAPL", "APPL",
    "TOTAL", "TOTAL2", "TOTAL3", "OTHERS", "BTCD", "USDTD",
}
# Phrases where long/short/buy describe market positioning, time or news, never a call.
NEUTRALISE = re.compile(
    r"as long as|long[- ]term|long time|long way|for long\b|how long|no longer|longer|long[- ]awaited|"
    r"long positions?|short positions?|\blongs\b|\bshorts\b|short[- ]term|in short\b|short squeez\w*|"
    r"short volume|long volume|liquidat\w*\s+(?:long|short)s?\b|liquidat\w*|be careful with (?:long|short)\w*|"
    r"(?:long|short)\w*\s+(?:\S+\s+){0,3}(?:is|are)\s+(?:tantamount to suicide|suicide|dangerous|risky|a mistake)|"
    r"\b(?:whale|trader|someone|insider|address)s?\b[^\n]*|"
    r"buying (?:activity|pressure|zone)|sell(?:ing)? pressure|buy orders?|sell orders?|"
    r"(?:is|are|was|were) buying\b|hints? at buying|buying more bitcoin|buying bonds|"
    r"buy crypto|buys? \d[\d,.]*|bought \d[\d,.]*",
    re.I)
RECAP = re.compile(
    r"profit\s*[:\-]?\s*\+?\d+\s*%|\+\d+(?:\.\d+)?\s*%|\d+\s*%\s*profit|\$\d[\d,.]*k?\s*profit|in (?:good |decent |huge )?profit|"
    r"tp\s*\d?\s*(?:hit|done|reached|achieved)|\b(?:is|are|was|were)\s+(?:also\s+)?(?:achieved|hit|done)\b|"
    r"targets?\s*(?:\d\s*)?(?:is\s+)?(?:hit|done|reached|achieved)|all\s+targets?|(?:first|1st|final|\d+/\d+)\s+targets?|"
    r"reached\s+(?:the\s+)?(?:first|final|\d)|🏁|printed|sl\s+hit|stop\s*loss\s+hit|booked|congrats|"
    r"\bx\d{1,3}\b\s*🚀|closed\b|\bout of (?:the\s+)?[#$]?\w+\s+(?:long|short)|took (?:an? )?l\b|took profits?|"
    r"taking (?:some |partial )?profits?(?!\s*[:\-–]?\s*\$?\d)|take (?:some )?profits?(?!\s*[:\-–]?\s*\$?\d)|"
    r"went amazing|paid off|made (?:some )?(?:amazing |great )?profit|\bmade\b[^.\n]{0,25}\$\d",
    re.I)
# Channel boilerplate stripped before classification (news footers, social links).
FOOTER = re.compile(r"^.*(?:join (?:our |free )?vip|follow us|✉️\s*telegram|🐣\s*twitter|⚡️\s*boost).*$", re.I | re.M)
NEWS = re.compile(r"^\s*(?:📢|🚨|💰|🔽|🔼|▶️|🗓|🇺🇸|🇮🇳|🇨🇳|🇮🇷|🇯🇵|🇧🇹|🇮🇱|JUST IN|BREAKING|MARKETS:)", re.I)
PROMO = re.compile(
    r"ref=|invite_code|discount|\bjoin\b|vote|giveaway|deposit bonus|copy[- ]?trad|propfunded|"
    r"premium channel|subscribe|youtube\.com|forms\.gle|\bwin\s+\$?\d|keep this channel",
    re.I)
CONDITIONAL = re.compile(r"\bif\b|in case|\bonce\b|wait(?:ing)? for|unless|as soon as|confirmation|\blimit\b|too early",
                         re.I)
FOLLOWUP = re.compile(
    r"\bstill\b[^.\n]{0,40}\b(?:long|short|in long|in short|bullish|bearish|holding)\b|"
    r"\bholding\s+(?:my\s+|our\s+|the\s+)?(?:\w+\s+)?(?:long|short|bag)\b|hold(?:ing)? my bag|not selling|remain loyal",
    re.I)
SYM = r"(?:[#$][A-Za-z][A-Za-z0-9]{1,14}|\b[A-Z0-9]{2,15}\s?/?USDT\b|\b(?:BTC|ETH|SOL|XRP|BNB|DOGE|HYPE|Bitcoin|Ethereum|Ether|Solana)\b)"
LONG_CALL = [
    rf"\b(?:open\s+)?long\s+{SYM}", rf"{SYM}\s*[-–—|:]?\s*\(?\s*long\b", r"\blong\s+signal\b", r"\blong\s+below\b",
    rf"\bbuying\s+(?:some|a\s+bag\s+of|more)?\s*{SYM}", rf"\bbuy\s+(?:the\s+dips?|at\s+the\s+bottom)", r"\bbuy\s+(?:it|now)\b",
    r"\bbuy\s+and\s+hold\b", r"\bbuy\s*:\s*\d", rf"\bbuy\s+{SYM}", rf"\bkeep\s+buying\b",
    r"\b(?:will|gonna|going\s+to)\s+(?:take|go|open)\s+(?:a\s+)?(?:\w+\s+){0,2}long\b", r"\btake\s+(?:a\s+)?(?:\w+\s+)?long\b",
    r"\bgo\s+long\b", r"\bnow\s+in\s+long\b", r"\bentered\s+(?:a\s+)?long\b", r"\bopening\s+(?:a\s+)?long\b",
    r"\blong\s+now\b", r"\bi\s+buy\b", r"\blong\s+here\b",
]
SHORT_CALL = [
    rf"\b(?:open\s+)?short\s+{SYM}", rf"{SYM}\s*[-–—|:]?\s*\(?\s*short\b", r"\bshort\s+signal\b", r"\bshort\s+above\b",
    r"\b(?:will|gonna|going\s+to)\s+(?:take\s+(?:a\s+)?(?:\w+\s+){0,2})?short\b", r"\btake\s+(?:a\s+)?(?:\w+\s+)?short\b",
    r"\bgo\s+short\b", r"\bnow\s+in\s+short\b", r"\bentered?\s+(?:a\s+|another\s+)?short\b", r"\bopening\s+(?:a\s+)?short\b",
    r"\bshort\s+now\b", r"\bshort\s+here\b", r"\bshorted\b",
]
BULL = re.compile(r"\bbullish\b|\bmoon\b|ready to (?:boom|take off|fly|pump|explode|comeback|run)|\bprogrammed\b|"
                  r"big move soon|going (?:up|higher)\b|upward|\bundervalued\b", re.I)
BEAR = re.compile(r"\bbearish\b|going (?:down|lower)\b|downward|\boverval", re.I)
NUMBERS = re.compile(r"(?:entry|buy|long below|sl|stop[- ]?loss|stoploss|target|take profit|tp)\s*\w*\s*[:\-–]?\s*\$?\d", re.I)
LONG_RE = [re.compile(p, re.I) for p in LONG_CALL]
SHORT_RE = [re.compile(p, re.I) for p in SHORT_CALL]


def tradable_universe(root: Path = ROOT) -> tuple[set[str], set[str]]:
    def names(folder: Path) -> set[str]:
        return {p.name[:-5] for p in folder.glob("*_USDT") if (p / "1h.parquet").exists()} if folder.exists() else set()
    return names(root / "data/historical/binance_futures"), names(root / "data/historical/binance")


def resolve_symbol(raw: str, futures: set[str], spot: set[str]) -> tuple[str | None, str]:
    """Raw ticker -> (local base, venue). 1000x-denominated perps keep the same return series."""
    base = NAME_TO_SYMBOL.get(raw.upper(), raw.upper())
    if base in futures:
        return base, "futures"
    for prefix in ("1000", "1000000", "1M"):
        if prefix + base in futures:
            return prefix + base, "futures"
    if base in spot:
        return base, "spot"
    return None, "not_in_local_universe"


def extract_symbols(text: str) -> tuple[list[tuple[str, int]], bool]:
    """(raw ticker, character offset) in order, and whether the ticker was inferred.

    Order of evidence: cashtag/hashtag/XXXUSDT; the "XXX buy and hold / big move soon" template's
    leading word; exactly one plain major (BTC, Bitcoin, ...); finally, a call quoting a 5-digit
    dollar level with no ticker at all is BTC (author's own numbers only, no market data).
    """
    found: list[tuple[str, int]] = []
    for m in re.finditer(r"[#$]([A-Za-z][A-Za-z0-9]{1,14})\b|\b([A-Z0-9]{1,15})\s?/?USDT\b", text):
        raw = (m.group(1) or m.group(2)).upper()
        if raw.endswith("USDT") and len(raw) > 4:
            raw = raw[:-4]
        if raw not in NOT_TICKERS and not raw.isdigit():
            found.append((raw, m.start()))
    if found:
        return found, False
    lead = re.match(r"\s*([A-Za-z0-9]{2,15})(?:\s?usdt)?\s+(?:buy and hold|big (?:news )?(?:big )?move)", text, re.I)
    if lead and lead.group(1).upper() not in NOT_TICKERS and not lead.group(1).isdigit():
        return [(lead.group(1).upper(), lead.start(1))], False
    majors = [(m.group(1).upper(), m.start()) for m in
              re.finditer(r"\b(BTC|ETH|SOL|XRP|BNB|DOGE|HYPE|Bitcoin|Ethereum|Ether|Solana)\b", text, re.I)]
    if majors:
        return majors, False
    level = re.search(r"\$\s?([1-9]\d{1,2}),?\d{3}\b", text)
    if level and int(level.group(1)) >= 20:
        return [("BTC", level.start())], True
    return [], False


def direction_hits(text: str) -> tuple[list[int], list[int]]:
    """Character offsets of long-call and short-call grammar after neutralising positioning/news phrases."""
    clean = NEUTRALISE.sub(lambda m: " " * len(m.group(0)), text)
    longs = sorted({m.start() for r in LONG_RE for m in r.finditer(clean)})
    shorts = sorted({m.start() for r in SHORT_RE for m in r.finditer(clean)})
    return longs, shorts


def classify_post(post: dict, futures: set[str], spot: set[str]) -> dict:
    """Deterministic, price-blind label for one post. Returns label + per-post reason + signals."""
    raw_text = post.get("text") or ""
    out = {"message_id": post["message_id"], "label": None, "reason": None, "signals": [], "conditional": False,
           "inferred_symbol": False}
    if not raw_text.strip():
        return {**out, "label": "no_text", "reason": "media_or_empty"}
    news_footer = bool(re.search(r"follow us", raw_text, re.I))
    text = FOOTER.sub("", raw_text).strip()
    if not text:
        return {**out, "label": "recap_or_promo", "reason": "promotion"}
    if re.search(r"\(stock\)|\bstocks?\b", text, re.I) and not re.search(r"USDT", text):
        return {**out, "label": "other", "reason": "non_crypto_asset"}
    if PROMO.search(text):
        return {**out, "label": "recap_or_promo", "reason": "promotion"}
    if RECAP.search(text):
        return {**out, "label": "recap_or_promo", "reason": "result_or_exit"}
    if news_footer or NEWS.match(text):
        return {**out, "label": "other", "reason": "news"}
    if re.search(r"[#$](?:TOTAL\d?|OTHERS|BTC\.?D|USDT\.?D)\b", text, re.I):
        return {**out, "label": "other", "reason": "index_commentary"}
    symbols, inferred = extract_symbols(text)
    longs, shorts = direction_hits(text)
    if longs and shorts:
        return {**out, "label": "ambiguous", "reason": "both_directions"}
    if longs or shorts:
        side, hits = (1, longs) if longs else (-1, shorts)
        followup = bool(FOLLOWUP.search(text))
        structured = bool(NUMBERS.search(text))
        label = "followup_view" if followup else ("trade_plan" if structured else "direct_call")
    else:
        bull, bear = bool(BULL.search(text)), bool(BEAR.search(text))
        if bull == bear:
            return {**out, "label": "other", "reason": "no_direction" if not bull else "mixed_bias"}
        side, hits, label = (1 if bull else -1), [], "bias_view"
    if not symbols:
        return {**out, "label": "direction_without_symbol", "reason": label}
    if inferred and label == "bias_view":
        return {**out, "label": "direction_without_symbol", "reason": label}
    distinct = list(dict.fromkeys(NAME_TO_SYMBOL.get(raw, raw) for raw, _ in symbols))
    if len(distinct) > 3:
        return {**out, "label": "ambiguous", "reason": "more_than_3_symbols"}
    chosen = distinct
    line_of = lambda pos: text.count("\n", 0, pos)
    lines = {line_of(h) for h in hits}
    call_lines = [text.split("\n")[i] for i in sorted(lines)]
    if distinct != ["BTC"] and any(re.search(r"\$\s?(?:[2-9]\d|1\d\d),?\d{3}\b", line) for line in call_lines):
        # "Will long here ... $65690" next to a stray "#SOL": the quoted level belongs to BTC, not the ticker.
        return {**out, "label": "ambiguous", "reason": "price_level_conflicts_with_ticker"}
    if len(distinct) > 1 and hits:
        # Keep symbols on the same line as a direction phrase; ambiguous if that still leaves several.
        chosen = list(dict.fromkeys(NAME_TO_SYMBOL.get(raw, raw) for raw, pos in symbols if line_of(pos) in lines)) or distinct
        if len(chosen) > 1:
            return {**out, "label": "ambiguous", "reason": "several_symbols_for_one_call"}
    elif len(distinct) > 1:
        return {**out, "label": "ambiguous", "reason": "several_symbols_for_one_view"}
    signals = []
    for raw in chosen:
        base, venue = resolve_symbol(raw, futures, spot)
        signals.append({"raw_symbol": raw, "symbol": base, "venue": venue, "side": side})
    return {**out, "label": label, "reason": "parsed", "signals": signals,
            "conditional": bool(CONDITIONAL.search(text)), "inferred_symbol": inferred}


def availability(post: dict) -> tuple[pd.Timestamp, bool]:
    """Earliest time the surviving (final) text is known to be visible, and whether it is a late edit."""
    published = pd.Timestamp(post["published_at"]).tz_convert("UTC")
    if not post.get("edited_at"):
        return published, False
    edited = pd.Timestamp(post["edited_at"]).tz_convert("UTC")
    return max(published, edited), edited - published > LATE_EDIT


def label_study(study: Path, root: Path = ROOT) -> dict:
    futures, spot = tradable_universe(root)
    sealed: dict = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "rule_version": "kol4-v1",
                    "script_sha256": sha(Path(__file__).read_bytes()), "channels": {}}
    for slug in SLUGS:
        history = json.loads((study / f"{slug}_history.json").read_text(encoding="utf-8"))
        rows, counts = [], Counter()
        for post in sorted(history["posts"], key=lambda p: (p["published_at"], p["message_id"])):
            label = classify_post(post, futures, spot)
            at, late = availability(post)
            counts[label["label"]] += 1
            rows.append({**label, "published_at": post["published_at"], "available_at": str(at),
                         "late_edit": late, "forwarded": post["forwarded"], "reply_to_id": post["reply_to_id"],
                         "text_sha256": post["text_sha256"]})
        sealed["channels"][slug] = {"history_sha256": sha((study / f"{slug}_history.json").read_bytes()),
                                    "label_counts": dict(counts), "posts": rows}
    return sealed


# --------------------------------------------------------------------------- evaluate (offline prices)

HORIZONS = (1, 4, 24, 72, 168)
PRIMARY_HOURS = 24
CALL_GROUPS = {"fresh_call": ("trade_plan", "direct_call"), "conditional_call": ("trade_plan", "direct_call"),
               "bias_view": ("bias_view",), "followup_view": ("followup_view",)}


def call_group(post: dict) -> str | None:
    """Unconditional explicit calls are the primary sample; conditional ones are an immediate-entry proxy only."""
    if post["label"] in CALL_GROUPS["fresh_call"]:
        return "conditional_call" if post["conditional"] else "fresh_call"
    return next((g for g in ("bias_view", "followup_view") if post["label"] in CALL_GROUPS[g]), None)


def load_bars(symbol: str, venue: str, root: Path = ROOT) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    folder = root / ("data/historical/binance_futures" if venue == "futures" else "data/historical/binance") / f"{symbol}_USDT"
    bars = pd.read_parquet(folder / "1h.parquet")
    bars.index = pd.to_datetime(bars.index, utc=True)
    bars = bars[~bars.index.duplicated(keep="last")].sort_index()
    funding = None
    if venue == "futures" and (folder / "funding_rate.parquet").exists():
        funding = pd.read_parquet(folder / "funding_rate.parquet")
        funding.index = pd.to_datetime(funding.index, utc=True)
    return bars, funding


def signal_rows(sealed: dict) -> list[dict]:
    rows = []
    for slug, channel in sealed["channels"].items():
        for post in channel["posts"]:
            group = call_group(post)
            if group is None:
                continue
            for i, s in enumerate(post["signals"]):
                rows.append({"signal_id": f"{slug}:{post['message_id']}:{i}", "channel": slug, "group": group,
                             "kind": post["label"], "symbol": s["symbol"], "raw_symbol": s["raw_symbol"],
                             "venue": s["venue"], "side": int(s["side"]), "published_at": post["published_at"],
                             "available_at": post["available_at"], "late_edit": post["late_edit"],
                             "conditional": post["conditional"], "inferred_symbol": post.get("inferred_symbol", False),
                             "forwarded": post["forwarded"], "message_id": post["message_id"]})
    return rows


def run_up(panel, pos: int, side: int, hours: int = 24) -> float | None:
    """Side-signed move over the `hours` before entry: positive means the call chased an existing move."""
    if pos is None or pos < hours or not (np.isfinite(panel.open[pos]) and np.isfinite(panel.open[pos - hours])):
        return None
    return float(side * (panel.open[pos] / panel.open[pos - hours] - 1.0))


def evaluate_rows(signals: list[dict], panels: dict, horizons=HORIZONS, window: int | str = 30 * 24) -> tuple[list[dict], Counter]:
    """Per signal and horizon: net after 20bps + funding and its same-coin/same-side random-start null.

    Entry is the next full hourly open >= available_at + 60s (available_at already covers edits).
    window=int uses +/-window hours excluding overlap; window="pre" uses only the 30 days before entry.
    """
    from scripts.review_telegram_signals import BASE_COST, SMALLCAP_COST, entry_hour, null_candidates
    cache: dict[tuple, tuple] = {}
    out, status = [], Counter()
    for hours in horizons:
        for s in signals:
            if s["symbol"] is None:
                status[(s["channel"], hours, "symbol_not_in_local_universe")] += 1
                continue
            if s["venue"] == "spot" and s["side"] < 0:
                status[(s["channel"], hours, "short_without_perp")] += 1
                continue
            panel = panels[(s["symbol"], s["venue"])]
            key = (s["symbol"], s["venue"], hours, s["side"])
            if key not in cache:
                cache[key] = panel.outcomes(hours, s["side"])
            gross, funding = cache[key]
            at = entry_hour(s["available_at"])
            pos = panel.position(at)
            if pos is None or not np.isfinite(gross[pos]):
                status[(s["channel"], hours, "no_complete_price_path")] += 1
                continue
            valid = np.isfinite(gross)
            if s["venue"] == "futures":
                if not np.isfinite(funding[pos]):
                    status[(s["channel"], hours, "funding_unavailable")] += 1
                    continue
                valid &= np.isfinite(funding)
            if window == "pre":
                idx = np.arange(max(0, pos - 30 * 24), max(0, pos - hours))
                candidates = idx[valid[idx]]
            else:
                candidates = null_candidates(pos, hours, valid, window)
            if len(candidates) < 50:
                status[(s["channel"], hours, "null_sample_below_50")] += 1
                continue
            fund = float(funding[pos]) if s["venue"] == "futures" else 0.0
            null_funding = funding[candidates] if s["venue"] == "futures" else 0.0
            null_values = np.maximum(-1.0, gross[candidates] - BASE_COST + null_funding)
            out.append({**s, "hours": hours, "entry_at": at, "gross": float(gross[pos]), "funding": fund,
                        "funding_missing": s["venue"] == "futures" and not np.isfinite(funding[pos]),
                        "liquidated": bool(gross[pos] <= -1.0),
                        "net": max(-1.0, float(gross[pos]) - BASE_COST + fund),
                        "net_smallcap": max(-1.0, float(gross[pos]) - SMALLCAP_COST + fund),
                        "null_values": null_values, "null_mean": float(null_values.mean()),
                        "null_hit": float((null_values > 0).mean()), "null_n": int(len(candidates)),
                        "run_up_24h": run_up(panel, pos, s["side"])})
            status[(s["channel"], hours, "ok")] += 1
    return out, status


def verdict(cell: dict) -> str:
    """Pre-registered screen (written before the first evaluate run)."""
    if cell.get("n_independent", 0) < 30:
        return "insufficient_sample"
    ci = cell.get("excess_week_bootstrap_95")
    if (cell.get("bh_q", 1) < .10 and cell["p_greater_than_null"] < .05 and ci and ci[0] > 0
            and (cell.get("mean_without_best") or -1) > 0 and cell["mean_net_80bps"] > 0):
        return "passes_screen"
    return "no_convincing_edge"


def claim_audit(sealed: dict, histories: dict, panels: dict) -> list[dict]:
    """Self-reported "profit N%" replies vs what the unlevered call could have shown (max high in 7 days)."""
    from scripts.review_telegram_signals import entry_hour
    rows = []
    for slug, channel in sealed["channels"].items():
        calls = {p["message_id"]: p for p in channel["posts"] if p["label"] in CALL_GROUPS["fresh_call"] and p["signals"]}
        for post in histories[slug]:
            m = re.search(r"profit\s*[:\-]?\s*\+?(\d+(?:\.\d+)?)\s*%|(\d+(?:\.\d+)?)\s*%\s*profit|\+(\d+(?:\.\d+)?)\s*%",
                          post.get("text") or "", re.I)
            parent = calls.get(post.get("reply_to_id"))
            if not m or parent is None or len(parent["signals"]) != 1:
                continue
            s = parent["signals"][0]
            if s["symbol"] is None or s["side"] < 0:
                continue
            panel = panels.get((s["symbol"], s["venue"]))
            if panel is None:
                continue
            pos = panel.position(entry_hour(parent["available_at"]))
            claim_at = pd.Timestamp(post["published_at"]).tz_convert("UTC").floor("h")
            end = panel.position(min(claim_at, entry_hour(parent["available_at"]) + pd.Timedelta(hours=168)))
            if pos is None or end is None or end <= pos or not np.isfinite(panel.open[pos]):
                continue
            highs = panel.high[pos:end + 1]
            if not np.isfinite(highs).any():
                continue
            mfe = float(np.nanmax(highs) / panel.open[pos] - 1.0)
            claimed = float(next(g for g in m.groups() if g)) / 100
            rows.append({"channel": slug, "call_id": parent["message_id"], "claim_id": post["message_id"],
                         "symbol": s["symbol"], "claimed": claimed, "mfe_to_claim": mfe,
                         "implied_leverage": claimed / mfe if mfe > 0 else None,
                         "hours_to_claim": float((claim_at - entry_hour(parent["available_at"])) / pd.Timedelta(hours=1))})
    return rows


PLAN_NUMBER = r"[<~$\s.]*(\d[\d,]*(?:\.\d+)?)"


def _number(raw: str) -> float:
    return float(re.sub(r",(?=\d{3}\b)", "", raw).replace(",", "."))


def parse_plan(text: str) -> dict:
    """First target and stop of a structured plan; percent/R-multiple targets and missing levels stay None."""
    def first(pattern):
        m = re.search(pattern + r"(?:\s*(?:at\b|[:\-–•🎯*]))*\s*" + PLAN_NUMBER, text, re.I)
        if m is None:
            return None
        rest = text[m.end():].split("\n", 1)[0]
        if rest.lstrip().startswith("/") or "%" in rest:
            return None
        return _number(m.group(1))
    return {"tp1": first(r"(?:take profit|targets?|\btp\s*1?\b|\bsell\b)"),
            "sl": first(r"(?:\bsl\b|stop[- ]?loss|stoploss|s/l)")}


def simulate_plan(panel, at: pd.Timestamp, side: int, tp1: float, sl: float, max_hours: int = 168,
                  cost: float = .002, *, funding_required: bool = True) -> dict:
    """Market entry at the hourly open `at`; exit at the first of TP1/stop on hourly high/low (stop first when
    both touch in one bar, gaps through the stop fill at the open), otherwise at the open after max_hours."""
    pos = panel.position(at)
    if pos is None or not np.isfinite(panel.open[pos]):
        return {"status": "no_complete_price_path"}
    entry = float(panel.open[pos])
    if not ((side > 0 and sl < entry < tp1) or (side < 0 and tp1 < entry < sl)):
        return {"status": "invalid_geometry_at_entry"}
    if max(abs(tp1 / entry - 1), abs(sl / entry - 1)) > .5:
        return {"status": "implausible_levels"}
    for k in range(max_hours):
        if pos + k >= len(panel.open):
            return {"status": "no_complete_price_path"}
        o, hi, lo = panel.open[pos + k], panel.high[pos + k], panel.low[pos + k]
        if not (np.isfinite(o) and np.isfinite(hi) and np.isfinite(lo)):
            return {"status": "no_complete_price_path"}
        stop_hit = lo <= sl if side > 0 else hi >= sl
        if stop_hit:
            fill = min(o, sl) if side > 0 else max(o, sl)
            exit_, reason, held = fill, "stop", k + 1
            break
        if (hi >= tp1) if side > 0 else (lo <= tp1):
            exit_, reason, held = tp1, "tp1", k + 1
            break
    else:
        if pos + max_hours >= len(panel.open) or not np.isfinite(panel.open[pos + max_hours]):
            return {"status": "no_complete_price_path"}
        exit_, reason, held = float(panel.open[pos + max_hours]), "timeout", max_hours
    charged = slice(pos + 1, pos + held)
    if funding_required and (not np.all(panel.funding_known[charged]) or not np.all(np.isfinite(panel.fund[charged]))):
        return {"status": "funding_unavailable"}
    funding = -side * float(panel.fund[charged].sum()) if funding_required else 0.0
    gross = max(-1.0, side * (exit_ / entry - 1.0))
    return {"status": "ok", "exit_reason": reason, "held_hours": held, "gross": gross,
            "net": max(-1.0, gross - cost + funding)}


def plan_simulation(sealed: dict, histories: dict, panels: dict) -> list[dict]:
    from scripts.review_telegram_signals import entry_hour
    texts = {(slug, p["message_id"]): p.get("text") or "" for slug, posts in histories.items() for p in posts}
    rows = []
    for slug, channel in sealed["channels"].items():
        open_until: dict[str, pd.Timestamp] = {}
        for post in channel["posts"]:
            if post["label"] != "trade_plan" or post["late_edit"] or len(post["signals"]) != 1:
                continue
            s = post["signals"][0]
            plan = parse_plan(texts[(slug, post["message_id"])])
            row = {"channel": slug, "message_id": post["message_id"], "symbol": s["symbol"], "side": s["side"], **plan}
            if s["symbol"] is None or plan["tp1"] is None or plan["sl"] is None:
                rows.append({**row, "status": "plan_or_symbol_missing"})
                continue
            at = entry_hour(post["available_at"])
            if s["symbol"] in open_until and at < open_until[s["symbol"]]:
                rows.append({**row, "status": "overlaps_open_position"})
                continue
            result = simulate_plan(panels[(s["symbol"], s["venue"])], at, s["side"], plan["tp1"], plan["sl"],
                                   funding_required=s["venue"] == "futures")
            if result["status"] == "ok":
                open_until[s["symbol"]] = at + pd.Timedelta(hours=result["held_hours"])
            rows.append({**row, "entry_at": str(at), **result})
    return rows


def evaluate_study(study: Path, out: Path, root: Path = ROOT) -> dict:
    from scripts.review_telegram_signals import SymbolPanel, bh_fdr, dedup_independent, summarise
    sealed_path = study / "events_before_prices.json"
    sealed = json.loads(sealed_path.read_text(encoding="utf-8"))
    signals = signal_rows(sealed)
    panels = {}
    for key in {(s["symbol"], s["venue"]) for s in signals if s["symbol"]}:
        bars, funding = load_bars(*key, root=root)
        panels[key] = SymbolPanel(bars, funding)
    primary_pool = [s for s in signals if not s["late_edit"]]
    evaluated, status = evaluate_rows(primary_pool, panels)
    late, _ = evaluate_rows([s for s in signals if s["late_edit"] and s["group"] == "fresh_call"], panels)
    pre, _ = evaluate_rows([s for s in primary_pool if s["group"] == "fresh_call"], panels, window="pre")
    cells = []
    # resolve_symbol picks one venue per ticker, so the symbol alone identifies the panel.
    by_symbol = {symbol: panel for (symbol, _), panel in panels.items()}

    def cell_for(rows, base, **meta):
        rows = dedup_independent(rows)
        cell = {**meta, "n_events": len(base), "n_independent": len(rows)}
        if len(rows) >= 2:
            cell.update(summarise(rows, by_symbol))
            cell["mean_run_up_24h"] = float(np.nanmean([r["run_up_24h"] if r["run_up_24h"] is not None else np.nan
                                                        for r in rows]))
        return cell

    for slug in SLUGS:
        for group in CALL_GROUPS:
            for side in ("all", "long", "short"):
                for hours in HORIZONS:
                    base = [r for r in evaluated if r["channel"] == slug and r["group"] == group and r["hours"] == hours
                            and (side == "all" or r["side"] == (1 if side == "long" else -1))]
                    cells.append(cell_for(base, base, channel=slug, group=group, side=side, hours=hours,
                                          sample="primary"))
        for kind in CALL_GROUPS["fresh_call"]:  # post-hoc format split, outside the FDR family
            for hours in HORIZONS:
                base = [r for r in evaluated if r["channel"] == slug and r["group"] == "fresh_call"
                        and r["kind"] == kind and r["hours"] == hours]
                cells.append(cell_for(base, base, channel=slug, group=kind, side="all", hours=hours, sample="by_kind"))
        for name, pool in (("late_edit", late), ("pre_only_null", pre)):
            for hours in HORIZONS:
                base = [r for r in pool if r["channel"] == slug and r["hours"] == hours]
                cells.append(cell_for(base, base, channel=slug, group="fresh_call", side="all", hours=hours, sample=name))
    family = [c for c in cells if c["sample"] == "primary" and c["side"] == "all" and c.get("n_independent", 0) >= 10
              and "p_greater_than_null" in c]
    for c, q in zip(family, bh_fdr([c["p_greater_than_null"] for c in family])):
        c["bh_q"] = q
    for c in cells:
        if c["sample"] == "primary" and c["side"] == "all" and "p_greater_than_null" in c:
            c["verdict"] = verdict(c)
    histories = {slug: json.loads((study / f"{slug}_history.json").read_text(encoding="utf-8"))["posts"] for slug in SLUGS}
    claims = claim_audit(sealed, histories, panels)
    plans = plan_simulation(sealed, histories, panels)
    from scripts.review_telegram_signals import week_bootstrap
    plan_summary = {}
    for slug in SLUGS:
        done = [r for r in plans if r["channel"] == slug and r["status"] == "ok"]
        net = np.array([r["net"] for r in done])
        plan_summary[slug] = {
            "status": dict(Counter(r["status"] for r in plans if r["channel"] == slug)),
            "n": len(done), "exit_reasons": dict(Counter(r["exit_reason"] for r in done)),
            "mean_net": float(net.mean()) if len(net) else None, "median_net": float(np.median(net)) if len(net) else None,
            "net_positive_share": float((net > 0).mean()) if len(net) else None,
            "mean_without_best": float(np.delete(net, net.argmax()).mean()) if len(net) > 1 else None,
            "week_bootstrap_95": week_bootstrap(net, [pd.Timestamp(r["entry_at"]) for r in done]) if len(net) else None,
        }
    gaps = {}
    for slug, posts in histories.items():
        ids = sorted(p["message_id"] for p in posts)
        gaps[slug] = {"first_id": ids[0], "last_id": ids[-1], "present": len(ids),
                      "missing_ids": ids[-1] - ids[0] + 1 - len(ids)}
    out.mkdir(parents=True, exist_ok=True)
    trades = pd.DataFrame([{k: v for k, v in r.items() if k != "null_values"} for r in evaluated + late])
    trades.to_csv(out / "trades.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(cells).to_csv(out / "cells.csv", index=False, encoding="utf-8-sig")
    result = {
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "accounting_revision": "2026-10-25-audit: funded futures and null coverage required; equal collateral floor; no post-exit price requirement",
        "events_sha256": sha(sealed_path.read_bytes()), "script_sha256": sha(Path(__file__).read_bytes()),
        "local_data_last_bar": {f"{k[0]}:{k[1]}": str(p.index[-1]) for k, p in panels.items() if len(p.index)},
        "signals": {slug: dict(Counter(f"{s['group']}|{s['venue']}" for s in signals if s["channel"] == slug))
                    for slug in SLUGS},
        "status": {"|".join(map(str, k)): v for k, v in status.items()},
        "fdr_family_size": len(family), "cells": cells, "claims": claims, "message_id_gaps": gaps,
        "plan_simulation": {"summary": plan_summary, "rows": plans,
                            "rule": "exploratory: market entry, first of TP1/stop on hourly bars, stop first, 168h cap"},
        "rules": {"entry": "next full 1h open >= available_at + 60s; available_at = edit time when edited",
                  "late_edit": f"edit more than {LATE_EDIT} after publication -> separate sample",
                  "cost": "20bps round trip + actual funding (futures); 80bps sensitivity",
                  "short": "(entry-exit)/entry, 1x collateral floor",
                  "null": "same symbol/venue/side/horizon, random hourly starts within +/-30d excluding overlap",
                  "dedup": "one open position per channel/symbol/horizon", "primary_horizon_hours": PRIMARY_HOURS},
    }
    (out / "results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("collect", "label", "evaluate"))
    parser.add_argument("--study", type=Path, default=STUDY)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if args.command == "collect":
        collect(args.study)
    elif args.command == "label":
        sealed = label_study(args.study)
        path = args.study / "events_before_prices.json"
        write_new(path, sealed)
        print(json.dumps({slug: c["label_counts"] for slug, c in sealed["channels"].items()}, indent=1))
        print("sealed", sha(path.read_bytes()))
    elif args.command == "evaluate":
        result = evaluate_study(args.study, args.study / "results")
        print(json.dumps({k: result[k] for k in ("events_sha256", "signals", "fdr_family_size")}, indent=1, default=str))


if __name__ == "__main__":
    main()
