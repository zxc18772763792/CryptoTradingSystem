"""Point-in-time evidence recorded while a paper trade happens.

Historical event studies here cannot prove what was visible or tradable at the
time: announcement titles are fetched afterwards, the contract universe is
today's exchangeInfo, entry liquidity and the intraday path to a stop are
unknown (docs/STRATEGY_EXPLORATION_2026-09-29.md). Paper trackers therefore
record, as it happens: the contract's status, the order book around the
signal and the entry close, the 5-minute path over the hold, and an
equal-weight basket of liquid perps chosen at the signal (a contemporaneous
market control, diagnostic only). Every helper is best-effort: a failure is
recorded or retried next pass, never allowed to change a trade.
"""
from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Any, Dict, Iterable, List, Mapping, Optional

FAPI = "https://fapi.binance.com/fapi/v1"
EVIDENCE_VERSION = 1
BASKET_SIZE = 30
DEPTH_LEVELS = 100
PATH_INTERVAL, PATH_BAR_MS = "5m", 300_000
DAY_MS = 86_400_000


def contract_record(contracts: Mapping[str, Mapping[str, Any]], candidates: Iterable[str], now: datetime) -> Dict[str, Any]:
    """Status of each candidate perp in exchangeInfo right now ("absent" when not listed at all)."""
    out: Dict[str, Any] = {"captured_at": now.isoformat()}
    for symbol in candidates:
        row = contracts.get(symbol)
        out[symbol] = "absent" if row is None else {
            "status": row.get("status"), "onboard_date": int(row.get("onboardDate") or 0),
        }
    return out


def book_metrics(depth: Mapping[str, Any], now: datetime) -> Dict[str, Any]:
    """Spread and USDT depth within 1% / 2% of mid from a /depth payload."""
    bids = [(float(p), float(q)) for p, q in depth.get("bids") or []]
    asks = [(float(p), float(q)) for p, q in depth.get("asks") or []]
    if not bids or not asks:
        return {"captured_at": now.isoformat(), "error": "empty_book"}
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    out: Dict[str, Any] = {
        "captured_at": now.isoformat(), "exchange_time_ms": depth.get("T") or depth.get("E"),
        "mid": mid, "spread_bps": round((best_ask - best_bid) / mid * 1e4, 3),
    }
    for pct in (1, 2):
        out[f"bid_usdt_{pct}pct"] = round(sum(p * q for p, q in bids if p >= mid * (1 - pct / 100)), 2)
        out[f"ask_usdt_{pct}pct"] = round(sum(p * q for p, q in asks if p <= mid * (1 + pct / 100)), 2)
    # 100 levels may end inside the band on a deep book: the sums are then lower bounds.
    out["band_2pct_complete"] = bids[-1][0] < mid * 0.98 and asks[-1][0] > mid * 1.02
    return out


async def book_snapshot(client, symbol: str, now: datetime) -> Dict[str, Any]:
    try:
        resp = await client.get(f"{FAPI}/depth", params={"symbol": symbol, "limit": DEPTH_LEVELS})
        if resp.status_code != 200:
            return {"captured_at": now.isoformat(), "error": f"http_{resp.status_code}"}
        return book_metrics(resp.json(), now)
    except Exception as exc:  # noqa: BLE001 - evidence is best-effort
        return {"captured_at": now.isoformat(), "error": type(exc).__name__}


async def market_basket(client, tradable: Iterable[str], exclude: str, now: datetime) -> Dict[str, Any]:
    """The BASKET_SIZE most-traded live USDT perps at the signal (chosen before the outcome is known)."""
    live = set(tradable) - {exclude}
    resp = await client.get(f"{FAPI}/ticker/24hr")
    if resp.status_code != 200:
        return {"captured_at": now.isoformat(), "error": f"http_{resp.status_code}"}
    rows = [r for r in resp.json() if r.get("symbol") in live]
    rows.sort(key=lambda r: float(r.get("quoteVolume") or 0.0), reverse=True)
    return {"captured_at": now.isoformat(), "symbols": [r["symbol"] for r in rows[:BASKET_SIZE]]}


async def fetch_path(client, symbol: str, start_ms: int, end_ms: int) -> List[List[float]]:
    """5-minute bars [open_ms, open, high, low, close] in [start_ms, end_ms); raises on a failed page."""
    bars: List[List[float]] = []
    cursor = start_ms
    while cursor < end_ms:
        resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": PATH_INTERVAL,
                                                          "startTime": cursor, "endTime": end_ms - 1, "limit": 1500})
        if resp.status_code != 200:
            raise RuntimeError(f"path http_{resp.status_code}")
        page = [[int(b[0]), float(b[1]), float(b[2]), float(b[3]), float(b[4])] for b in resp.json()]
        if not page:
            break
        bars += page
        cursor = page[-1][0] + PATH_BAR_MS
    return [b for b in bars if start_ms <= b[0] < end_ms]


def path_metrics(bars: List[List[float]], entry: float, start_ms: int, end_ms: int, stop_pct: float) -> Dict[str, Any]:
    """Adverse/favourable excursion and when (and how) the stop level was first reached."""
    expected = max(1, (end_ms - start_ms) // PATH_BAR_MS)
    out: Dict[str, Any] = {"interval": PATH_INTERVAL, "bars": len(bars), "coverage": round(len(bars) / expected, 3)}
    if not bars:
        return out
    stop_level = entry * (1 + stop_pct)
    out["mae_pct"] = round((max(b[2] for b in bars) / entry - 1) * 100, 3)   # against the short
    out["mfe_pct"] = round((1 - min(b[3] for b in bars) / entry) * 100, 3)   # in favour of the short
    hit = next((b for b in bars if b[2] >= stop_level), None)
    out["stop_hit_at"] = hit[0] if hit else None
    # A bar that OPENS beyond the stop gapped through it: the daily model's
    # "stop +2%" fill is then optimistic by this much.
    out["stop_gap_pct"] = round(max(0.0, hit[1] / stop_level - 1) * 100, 3) if hit else None
    return out


async def basket_return(client, symbols: List[str], entry_ms: int, exit_ms: int) -> Optional[Dict[str, Any]]:
    """Equal-weight close-to-close move of the basket over [entry_ms, exit_ms]; None when too few legs resolve."""
    days = (exit_ms - entry_ms) // DAY_MS
    moves = []
    for symbol in symbols:
        resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1d",
                                                          "startTime": entry_ms - DAY_MS, "limit": days + 1})
        if resp.status_code != 200:
            continue
        bars = resp.json()
        if len(bars) != days + 1 or int(bars[0][0]) != entry_ms - DAY_MS or int(bars[-1][0]) != exit_ms - DAY_MS:
            continue
        moves.append(float(bars[-1][4]) / float(bars[0][4]) - 1.0)
    if not moves or len(moves) * 2 < len(symbols):  # at least half the basket must resolve
        return None
    # mean = what an equal-weight long earns; median = the typical coin (one +100% leg moves the mean a lot)
    return {"return_pct": round(sum(moves) / len(moves) * 100, 3), "median_return_pct": round(median(moves) * 100, 3),
            "legs": len(moves), "basket": len(symbols)}
