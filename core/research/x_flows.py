"""Parse on-chain flow alerts posted by tracked X accounts into sized, directed events.

"WHALE DEPOSITS $4.4M HYPE TO COINBASE" is a different kind of statement from an opinion: it says which
token moved, in which direction relative to an exchange, and how much. That makes it testable with a
mechanism (coins moving onto an exchange can be sold there; coins withdrawn are less likely to be) and
with a size that can be set against how much the coin trades.

Pure text parsing; no prices and no network. The rules here are part of the flow study protocol
(docs/X_FLOW_STUDY_PROTOCOL.md) and are frozen before any return is computed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

_EXCHANGES = (
    r"binance|coinbase(?: prime)?|okx|okex|bybit|bitget|kraken|gate(?:\.io)?|gemini|bitfinex|htx|huobi|kucoin|"
    r"upbit|bithumb|mexc|crypto\.com|robinhood|bitstamp|deribit"
)
# Market makers and OTC desks: coins sent to them are usually about to be sold or lent out.
_DESKS = r"falconx|wintermute|cumberland|galaxy digital|b2c2|flowdesk|gsr|amber group|abcde|jump trading"
_VENUE = rf"(?:{_EXCHANGES}|{_DESKS})"

_DEPOSIT = re.compile(
    rf"\b(?:deposit(?:ed|s|ing)?|sen[dt]s?|transfer(?:r?ed|s)?|moved?|moves)\b[^.\n]{{0,160}}?\b(?:to|into)\b[^.\n]{{0,40}}?\b{_VENUE}\b",
    re.IGNORECASE)
_WITHDRAW = re.compile(
    rf"\b(?:withdr(?:ew|awn|aws|aw|awing)|pull(?:ed|s)?|receiv(?:ed|es))\b[^.\n]{{0,160}}?\bfrom\b[^.\n]{{0,40}}?\b{_VENUE}\b",
    re.IGNORECASE)
_BUY = re.compile(r"\b(?:bought|buys?|buying|accumulat(?:ed|es|ing|ion)|longs?|longed)\b", re.IGNORECASE)
_SELL = re.compile(r"\b(?:sold|sells?|selling|dump(?:ed|s|ing)?|offload(?:ed|s)?)\b", re.IGNORECASE)
_AMOUNT = re.compile(r"\$\s?([0-9][0-9,]*(?:\.[0-9]+)?)\s?([KMBkmb])?\b")
_UNITS = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9}


@dataclass(frozen=True)
class Flow:
    kind: str                  # cex_deposit | cex_withdraw | buy | sell
    sign: int                  # +1 bullish, -1 bearish
    usd: Optional[float]       # first dollar amount in the post, None when absent
    venue: Optional[str]


def usd_amounts(text: str) -> List[float]:
    values = []
    for number, unit in _AMOUNT.findall(text):
        try:
            values.append(float(number.replace(",", "")) * _UNITS[unit.upper()])
        except ValueError:
            continue
    return values


def parse_flow(text: str) -> Optional[Flow]:
    """Classify one post. Exchange movements outrank plain buy/sell words; no match -> None."""
    body = re.sub(r"https?://\S+", " ", text or "")
    amounts = usd_amounts(body)
    usd = amounts[0] if amounts else None
    deposit, withdraw = _DEPOSIT.search(body), _WITHDRAW.search(body)
    if deposit and not withdraw:
        venue = re.search(_VENUE, deposit.group(0)[::-1][::-1], re.IGNORECASE)
        return Flow("cex_deposit", -1, usd, venue.group(0).lower() if venue else None)
    if withdraw and not deposit:
        venue = re.search(_VENUE, withdraw.group(0), re.IGNORECASE)
        return Flow("cex_withdraw", 1, usd, venue.group(0).lower() if venue else None)
    if deposit and withdraw:
        return None  # a post describing both directions is ambiguous: leave it out rather than guess
    buy, sell = _BUY.search(body), _SELL.search(body)
    if buy and not sell:
        return Flow("buy", 1, usd, None)
    if sell and not buy:
        return Flow("sell", -1, usd, None)
    return None
