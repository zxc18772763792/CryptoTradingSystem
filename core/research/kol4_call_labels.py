"""Frozen copy of the kol4-v1 Telegram call labels (price-blind, deterministic).

Copied verbatim from scripts/research_telegram_new_kols.py at commit 1e9f9c1, the code that sealed
the 2026-10-25 labels (events_before_prices.json, SHA f1d969de...). The next10xgem1 fade tracker pins
this file's SHA-256 in every journal record, so the forward test keeps exactly the rule the study
used even if the research script changes later. Do not edit: a change here can only start a new
tracker version. tests/test_kol4_call_labels.py checks the copy against the study script.
"""
from __future__ import annotations

import re

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
