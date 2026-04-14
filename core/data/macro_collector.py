"""Macro indicator collector with US + China coverage.

Cross-market / US market pricing (zero config, `pip install yfinance`):
    vix          - CBOE VIX         (^VIX)
    dxy          - USD Index        (DX-Y.NYB)
    tnx_10y      - 10Y Treasury     (^TNX)

US macro (free FRED API key, auto-loaded from config/fred_api_key.txt
or FRED_API_KEY env var):
    fed_rate     - Federal Funds Effective Rate
    cpi_yoy      - CPI YoY %
    ppi_yoy      - PPI YoY %
    m1_yoy       - M1 YoY %
    m2_yoy       - M2 YoY %
    ppi_cpi_gap  - PPI YoY minus CPI YoY ("scissors spread")
    m1_m2_gap    - M1 YoY minus M2 YoY ("liquidity scissors spread")

China macro (official public releases, no key required):
    cn_cpi_yoy       - NBS CPI YoY %
    cn_ppi_yoy       - NBS PPI YoY %
    cn_m1_yoy        - PBOC M1 YoY %
    cn_m2_yoy        - PBOC M2 YoY %
    cn_ppi_cpi_gap   - PPI YoY minus CPI YoY ("scissors spread")
    cn_m1_m2_gap     - M1 YoY minus M2 YoY ("liquidity scissors spread")

Cache: data/macro/<name>.parquet, refreshed daily via background worker.

Usage:
    await update_macro_cache()      # fetch + persist (background task)
    snap = load_macro_snapshot()    # {name: float|None} from local cache
    grouped = group_macro_snapshot()  # {"market": {...}, "us": {...}, "china": {...}}
"""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import urljoin

from loguru import logger

_CACHE_DIR = Path("data/macro")
_KEY_FILE = Path(__file__).parent.parent.parent / "config" / "fred_api_key.txt"
_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_YAHOO_CHART_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_NBS_RELEASE_INDEX_URL = "https://www.stats.gov.cn/english/PressRelease/"
_PBOC_REPORT_INDEX_URL = "https://www.pbc.gov.cn/en/3688247/3688978/3709137/index.html"
_TIMEOUT_SEC = 30
_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# US FRED-backed series. Raw monthly observations are fetched from FRED and
# transformed locally where needed.
_SERIES_FRED: Dict[str, str] = {
    "fed_rate": "FEDFUNDS",
    "cpi_yoy": "CPIAUCSL",
    "ppi_yoy": "PPIACO",
    "m1_yoy": "M1SL",
    "m2_yoy": "M2SL",
}

# yfinance tickers -> local name
_SERIES_YF: Dict[str, str] = {
    "^VIX": "vix",
    "DX-Y.NYB": "dxy",
    "^TNX": "tnx_10y",
}

_CHINA_OFFICIAL_SERIES = {
    "cn_cpi_yoy",
    "cn_ppi_yoy",
    "cn_m1_yoy",
    "cn_m2_yoy",
}

_SCISSORS_SPREAD_NAME = "ppi_cpi_gap"
_LIQUIDITY_SCISSORS_SPREAD_NAME = "m1_m2_gap"
_CN_SCISSORS_SPREAD_NAME = "cn_ppi_cpi_gap"
_CN_LIQUIDITY_SCISSORS_SPREAD_NAME = "cn_m1_m2_gap"

_YOY_SERIES = {
    "cpi_yoy",
    "ppi_yoy",
    "m1_yoy",
    "m2_yoy",
    "cn_cpi_yoy",
    "cn_ppi_yoy",
    "cn_m1_yoy",
    "cn_m2_yoy",
}

_MARKET_KEYS = ("vix", "dxy", "tnx_10y")
_US_MACRO_KEYS = ("fed_rate", "cpi_yoy", "ppi_yoy", _SCISSORS_SPREAD_NAME, "m1_yoy", "m2_yoy", _LIQUIDITY_SCISSORS_SPREAD_NAME)
_CHINA_MACRO_KEYS = (
    "cn_cpi_yoy",
    "cn_ppi_yoy",
    _CN_SCISSORS_SPREAD_NAME,
    "cn_m1_yoy",
    "cn_m2_yoy",
    _CN_LIQUIDITY_SCISSORS_SPREAD_NAME,
)

_ALL_MACRO_NAMES = list(_MARKET_KEYS) + list(_SERIES_FRED.keys()) + [
    _SCISSORS_SPREAD_NAME,
    _LIQUIDITY_SCISSORS_SPREAD_NAME,
] + list(_CHINA_OFFICIAL_SERIES) + [
    _CN_SCISSORS_SPREAD_NAME,
    _CN_LIQUIDITY_SCISSORS_SPREAD_NAME,
]

_NBS_CPI_TITLE_RE = re.compile(r"^Consumer Price Index (?:in|for) .+\d{4}$", re.IGNORECASE)
_NBS_PPI_TITLE_RE = re.compile(r"^Industrial Producer Price Indexes (?:in|for) .+\d{4}$", re.IGNORECASE)
_PBOC_MONTHLY_REPORT_RE = re.compile(
    r"^Financial Statistics Report \((January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\)$",
    re.IGNORECASE,
)

_MONTH_RANK: Dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _title_period_key(title: str) -> tuple:
    """Extract (year, month_number) from a release title for recency ranking."""
    year_m = re.search(r"\b(20\d{2})\b", title)
    year = int(year_m.group(1)) if year_m else 0
    month = 0
    lower = title.lower()
    for name, num in _MONTH_RANK.items():
        if name in lower:
            month = num
            break
    return (year, month)


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []
        self._current: Optional[dict[str, object]] = None

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() != "a":
            return
        attr_map = {str(key or "").lower(): str(value or "") for key, value in attrs}
        self._current = {
            "href": attr_map.get("href", ""),
            "title": attr_map.get("title", ""),
            "parts": [],
        }

    def handle_data(self, data: str) -> None:
        if self._current is None:
            return
        parts = self._current.get("parts")
        if isinstance(parts, list):
            parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current is None:
            return
        parts = self._current.get("parts")
        text = "".join(parts) if isinstance(parts, list) else ""
        title = str(self._current.get("title") or "").strip() or text.strip()
        href = str(self._current.get("href") or "").strip()
        if href and title:
            self.anchors.append(
                {
                    "href": href,
                    "title": _normalize_space(title),
                    "text": _normalize_space(text),
                }
            )
        self._current = None


def _normalize_space(value: str) -> str:
    normalized = unescape(str(value or "")).replace("\xa0", " ").replace("Â", " ")
    return " ".join(normalized.split())


def _api_key() -> str:
    """Read FRED key from env var first, then key file."""
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        return key
    try:
        key = _KEY_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return key


def _compute_yoy(series) -> Optional[float]:
    """Compute the most recent YoY % change for a monthly series."""
    try:
        import pandas as pd  # noqa: PLC0415

        idx = pd.to_datetime(series.index)
        values = series.copy()
        values.index = idx
        values = values.sort_index().dropna()
        if len(values) < 13:
            return None
        latest = float(values.iloc[-1])
        year_ago = float(values.iloc[-13])
        if year_ago == 0:
            return None
        return round((latest / year_ago - 1) * 100, 4)
    except Exception:
        return None


def _series_latest_timestamp(series) -> Optional[datetime]:
    """Return the latest valid timestamp from a pandas-like series index."""
    try:
        import pandas as pd  # noqa: PLC0415

        idx = pd.to_datetime(series.index, utc=True, errors="coerce").dropna()
        if len(idx) <= 0:
            return None
        return idx[-1].to_pydatetime().astimezone(timezone.utc)
    except Exception:
        return None


def _write_snapshot_value(name: str, value: float, *, timestamp: Optional[datetime] = None) -> None:
    """Persist a single latest value for a macro series."""
    import pandas as pd  # noqa: PLC0415

    ts = timestamp or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_label = ts.astimezone(timezone.utc).strftime("%Y-%m-%d")
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.Series(
        {ts_label: float(value)},
        name=name,
        dtype=float,
    ).to_frame().to_parquet(_CACHE_DIR / f"{name}.parquet")


def _read_cached_series_value(name: str) -> Optional[float]:
    """Read a cached macro value, supporting legacy raw-series cache files."""
    path = _CACHE_DIR / f"{name}.parquet"
    if not path.exists():
        return None

    try:
        import pandas as pd  # noqa: PLC0415

        df = pd.read_parquet(path)
        if df.empty:
            return None
        series = df.iloc[:, 0]
        if name in _YOY_SERIES and len(series) >= 13:
            # Older cache files stored the raw monthly index series under the
            # transformed filenames. Recompute the YoY value when that layout is
            # detected.
            yoy = _compute_yoy(series)
            if yoy is not None:
                return yoy
        return round(float(series.iloc[-1]), 4)
    except Exception:
        return None


def _clean_html_text(html: str) -> str:
    stripped = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", str(html or ""), flags=re.IGNORECASE | re.DOTALL)
    stripped = re.sub(r"<[^>]+>", " ", stripped)
    return _normalize_space(stripped)


def _request_text(url: str) -> Optional[str]:
    import requests  # noqa: PLC0415

    try:
        resp = requests.get(url, headers=_DEFAULT_HEADERS, timeout=max(5, _TIMEOUT_SEC))
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("macro_collector: request failed for {}: {}", url, exc)
        return None

    try:
        encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        return resp.content.decode(encoding, errors="ignore")
    except Exception:
        return resp.text


def _extract_publish_timestamp(html: str) -> Optional[datetime]:
    patterns = [
        r'<meta[^>]+name=["\']PubDate["\'][^>]+content=["\'](?P<value>[^"\']+)["\']',
        r'<meta[^>]+content=["\'](?P<value>[^"\']+)["\'][^>]+name=["\']PubDate["\']',
        r'<meta[^>]+name=["\']createDate["\'][^>]+content=["\'](?P<value>[^"\']+)["\']',
        r'<meta[^>]+content=["\'](?P<value>[^"\']+)["\'][^>]+name=["\']createDate["\']',
    ]
    raw_value = None
    for pattern in patterns:
        match = re.search(pattern, html or "", flags=re.IGNORECASE)
        if match:
            raw_value = _normalize_space(match.group("value"))
            break
    if not raw_value:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw_value, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _extract_latest_anchor(html: str, title_pattern: re.Pattern[str], *, base_url: str) -> Optional[dict[str, str]]:
    parser = _AnchorParser()
    parser.feed(html or "")

    seen: set[str] = set()
    for anchor in parser.anchors:
        href = str(anchor.get("href") or "").strip()
        title = str(anchor.get("title") or "").strip()
        if not href or not title or not title_pattern.search(title):
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        return {"title": title, "url": absolute}
    return None


def _extract_triplet_row_value(text: str, row_label: str, value_index: int) -> Optional[float]:
    match = re.search(
        rf"{re.escape(row_label)}\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return round(float(match.group(value_index)), 4)
    except Exception:
        return None


def _extract_signed_yoy_from_text(text: str, label_pattern: str) -> Optional[float]:
    normalized = str(text or "")
    positive_match = re.search(
        rf"{label_pattern}.*?(?:rose by|rising by|grew by|increased by|up)\s+(\d+(?:\.\d+)?)\s*(?:percent|%)\s+year(?:\s*-\s*|\s+)on(?:\s*-\s*|\s+)year",
        normalized,
        flags=re.IGNORECASE,
    )
    if positive_match:
        return round(float(positive_match.group(1)), 4)

    negative_match = re.search(
        rf"{label_pattern}.*?(?:declined by|decreased by|fell by|down)\s+(\d+(?:\.\d+)?)\s*(?:percent|%)\s+year(?:\s*-\s*|\s+)on(?:\s*-\s*|\s+)year",
        normalized,
        flags=re.IGNORECASE,
    )
    if negative_match:
        return round(-float(negative_match.group(1)), 4)
    return None


def _extract_latest_percent_from_table(html: str, balance_label: str) -> Optional[float]:
    table_match = re.search(
        rf"{re.escape(balance_label)}.*?</table>",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        return None

    section = _clean_html_text(table_match.group(0))
    if "YOY Growth Rates" not in section:
        return None
    tail = section.split("YOY Growth Rates", 1)[1]
    values = re.findall(r"(-?\d+(?:\.\d+)?)\s*%", tail)
    if not values:
        return None
    try:
        return round(float(values[-1]), 4)
    except Exception:
        return None


def _persist_gap(
    result: Dict[str, Optional[float]],
    *,
    output_name: str,
    left_name: str,
    right_name: str,
    timestamps: Dict[str, Optional[datetime]],
) -> None:
    left_value = result.get(left_name)
    right_value = result.get(right_name)
    if left_value is None or right_value is None:
        result[output_name] = None
        return

    gap = round(float(left_value) - float(right_value), 4)
    gap_ts = max(
        [timestamps.get(left_name), timestamps.get(right_name), datetime.now(timezone.utc)],
        key=lambda item: item or datetime.now(timezone.utc),
    )
    result[output_name] = gap
    _write_snapshot_value(output_name, gap, timestamp=gap_ts)
    logger.debug(
        "macro_collector: cached {}={}, {}={}, {}={}",
        output_name,
        gap,
        left_name,
        left_value,
        right_name,
        right_value,
    )


async def _fetch_yfinance_macro() -> Dict[str, Optional[float]]:
    """Fetch VIX, DXY, 10Y yield via yfinance (runs in a thread pool)."""

    def _sync() -> Dict[str, Optional[float]]:
        try:
            import yfinance as yf  # noqa: PLC0415
        except ImportError:
            logger.debug("macro_collector: yfinance not installed; run: pip install yfinance")
            return {}

        result: Dict[str, Optional[float]] = {}
        tickers = list(_SERIES_YF.keys())
        try:
            raw = yf.download(tickers, period="5d", progress=False, auto_adjust=True)
            close = raw["Close"] if "Close" in raw.columns else raw
            for ticker, name in _SERIES_YF.items():
                try:
                    column = close[ticker] if ticker in close.columns else close.get(ticker)
                    result[name] = float(column.dropna().iloc[-1]) if column is not None else None
                except Exception:
                    result[name] = None
        except Exception as exc:
            logger.debug("macro_collector: yfinance batch failed: {}", exc)
            for ticker, name in _SERIES_YF.items():
                try:
                    history = yf.Ticker(ticker).history(period="5d")
                    result[name] = float(history["Close"].dropna().iloc[-1]) if not history.empty else None
                except Exception:
                    result[name] = None
        return result

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)


async def _fetch_yahoo_chart_macro(names: Optional[Dict[str, str]] = None) -> Dict[str, Optional[float]]:
    """Fetch VIX/DXY/TNX from Yahoo chart endpoints one by one as a rate-limit fallback."""

    def _sync() -> Dict[str, Optional[float]]:
        import requests  # noqa: PLC0415

        targets = dict(names or _SERIES_YF)
        results: Dict[str, Optional[float]] = {local_name: None for local_name in targets.values()}
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }
        params = {
            "range": "5d",
            "interval": "1d",
            "includePrePost": "false",
            "events": "div,splits",
        }
        for symbol, local_name in targets.items():
            url = _YAHOO_CHART_BASE.format(symbol=symbol)
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=max(5, _TIMEOUT_SEC))
                if resp.status_code != 200:
                    logger.debug("macro_collector: yahoo chart {} HTTP {}", symbol, resp.status_code)
                    continue
                payload = resp.json()
            except Exception as exc:
                logger.debug("macro_collector: yahoo chart {} failed: {}", symbol, exc)
                continue

            try:
                chart = (((payload or {}).get("chart") or {}).get("result") or [])[0] or {}
                quote = (((chart.get("indicators") or {}).get("quote") or [])[0] or {})
                closes = list(quote.get("close") or [])
                valid = [float(item) for item in closes if item is not None]
                if valid:
                    results[local_name] = valid[-1]
            except Exception:
                continue
        return results

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)


async def _fetch_fred_series(series_id: str, api_key: str, days: int = 730):
    """Fetch one FRED series. Returns pd.Series or None."""
    import aiohttp  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "sort_order": "asc",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SEC)) as session:
            async with session.get(_FRED_BASE, params=params) as resp:
                if resp.status != 200:
                    logger.debug("FRED {}: HTTP {}", series_id, resp.status)
                    return None
                data = await resp.json()
                records: Dict[str, float] = {}
                for observation in data.get("observations") or []:
                    try:
                        records[observation["date"]] = float(observation["value"])
                    except (KeyError, ValueError, TypeError):
                        continue
                if not records:
                    return None
                return pd.Series(records, name=series_id, dtype=float)
    except Exception as exc:
        logger.debug("FRED fetch {}: {}", series_id, exc)
        return None


async def _fetch_fred_macro(api_key: str) -> Dict[str, Optional[float]]:
    """Fetch US FRED-backed macro series and persist transformed snapshot values."""
    result: Dict[str, Optional[float]] = {}
    timestamps: Dict[str, Optional[datetime]] = {}

    tasks = {
        name: asyncio.create_task(_fetch_fred_series(series_id, api_key))
        for name, series_id in _SERIES_FRED.items()
    }
    fetched = {
        name: await task
        for name, task in tasks.items()
    }

    for name, series in fetched.items():
        if series is None or getattr(series, "empty", True):
            result[name] = None
            timestamps[name] = None
            continue

        latest_ts = _series_latest_timestamp(series)
        timestamps[name] = latest_ts
        if name == "fed_rate":
            value = round(float(series.iloc[-1]), 4)
        else:
            value = _compute_yoy(series)
        result[name] = value
        if value is not None:
            _write_snapshot_value(name, value, timestamp=latest_ts)
        logger.debug(
            "macro_collector: FRED cached {} ({}), latest={}",
            name,
            _SERIES_FRED[name],
            value,
        )

    _persist_gap(
        result,
        output_name=_SCISSORS_SPREAD_NAME,
        left_name="ppi_yoy",
        right_name="cpi_yoy",
        timestamps=timestamps,
    )
    _persist_gap(
        result,
        output_name=_LIQUIDITY_SCISSORS_SPREAD_NAME,
        left_name="m1_yoy",
        right_name="m2_yoy",
        timestamps=timestamps,
    )

    return result


async def _fetch_china_macro() -> Dict[str, Optional[float]]:
    """Fetch China macro series from official public NBS and PBOC releases."""

    def _sync() -> Dict[str, Optional[float]]:
        result: Dict[str, Optional[float]] = {name: None for name in _CHINA_OFFICIAL_SERIES}
        timestamps: Dict[str, Optional[datetime]] = {name: None for name in _CHINA_OFFICIAL_SERIES}

        nbs_index_html = _request_text(_NBS_RELEASE_INDEX_URL)
        if nbs_index_html:
            cpi_release = _extract_latest_anchor(nbs_index_html, _NBS_CPI_TITLE_RE, base_url=_NBS_RELEASE_INDEX_URL)
            if cpi_release:
                cpi_html = _request_text(cpi_release["url"])
                if cpi_html:
                    cpi_text = _clean_html_text(cpi_html)
                    cpi_value = _extract_triplet_row_value(cpi_text, "Consumer Price Index", 2)
                    if cpi_value is None:
                        match = re.search(
                            r"Consumer Price Index \(CPI\)\s+(increased|decreased)\s+by\s+(\d+(?:\.\d+)?)\s*(?:%|percent)\s+year(?:\s*-\s*|\s+)on(?:\s*-\s*|\s+)year",
                            cpi_text,
                            flags=re.IGNORECASE,
                        )
                        if match:
                            sign = -1.0 if match.group(1).lower() == "decreased" else 1.0
                            cpi_value = round(sign * float(match.group(2)), 4)
                    result["cn_cpi_yoy"] = cpi_value
                    timestamps["cn_cpi_yoy"] = _extract_publish_timestamp(cpi_html)
                    if cpi_value is not None:
                        _write_snapshot_value("cn_cpi_yoy", cpi_value, timestamp=timestamps["cn_cpi_yoy"])
                        logger.debug("macro_collector: NBS cached cn_cpi_yoy={} from {}", cpi_value, cpi_release["url"])

            ppi_release = _extract_latest_anchor(nbs_index_html, _NBS_PPI_TITLE_RE, base_url=_NBS_RELEASE_INDEX_URL)
            if ppi_release:
                ppi_html = _request_text(ppi_release["url"])
                if ppi_html:
                    ppi_text = _clean_html_text(ppi_html)
                    ppi_value = _extract_triplet_row_value(ppi_text, "I. Producer Price Indexes for Industrial Products", 2)
                    if ppi_value is None:
                        match = re.search(
                            r"producer price index for industrial products \(PPI\).*?to a\s+(\d+(?:\.\d+)?)%\s+(increase|decline)",
                            ppi_text,
                            flags=re.IGNORECASE,
                        )
                        if match:
                            sign = -1.0 if match.group(2).lower() == "decline" else 1.0
                            ppi_value = round(sign * float(match.group(1)), 4)
                    result["cn_ppi_yoy"] = ppi_value
                    timestamps["cn_ppi_yoy"] = _extract_publish_timestamp(ppi_html)
                    if ppi_value is not None:
                        _write_snapshot_value("cn_ppi_yoy", ppi_value, timestamp=timestamps["cn_ppi_yoy"])
                        logger.debug("macro_collector: NBS cached cn_ppi_yoy={} from {}", ppi_value, ppi_release["url"])

        pboc_index_html = _request_text(_PBOC_REPORT_INDEX_URL)
        if pboc_index_html:
            report_entry = _extract_latest_anchor(pboc_index_html, _PBOC_MONTHLY_REPORT_RE, base_url=_PBOC_REPORT_INDEX_URL)
            if report_entry:
                report_html = _request_text(report_entry["url"])
                if report_html:
                    report_text = _clean_html_text(report_html)
                    cn_m2 = _extract_signed_yoy_from_text(report_text, r"broad money supply \(M2\)")
                    cn_m1 = _extract_signed_yoy_from_text(report_text, r"Narrow money supply \(M1\)")
                    if cn_m2 is None:
                        cn_m2 = _extract_latest_percent_from_table(report_html, "M2 Balances")
                    if cn_m1 is None:
                        cn_m1 = _extract_latest_percent_from_table(report_html, "M1 Balances")
                    report_ts = _extract_publish_timestamp(report_html)
                    result["cn_m2_yoy"] = cn_m2
                    result["cn_m1_yoy"] = cn_m1
                    timestamps["cn_m2_yoy"] = report_ts
                    timestamps["cn_m1_yoy"] = report_ts
                    if cn_m2 is not None:
                        _write_snapshot_value("cn_m2_yoy", cn_m2, timestamp=report_ts)
                    if cn_m1 is not None:
                        _write_snapshot_value("cn_m1_yoy", cn_m1, timestamp=report_ts)
                    logger.debug(
                        "macro_collector: PBOC cached cn_m1_yoy={}, cn_m2_yoy={} from {}",
                        cn_m1,
                        cn_m2,
                        report_entry["url"],
                    )

        _persist_gap(
            result,
            output_name=_CN_SCISSORS_SPREAD_NAME,
            left_name="cn_ppi_yoy",
            right_name="cn_cpi_yoy",
            timestamps=timestamps,
        )
        _persist_gap(
            result,
            output_name=_CN_LIQUIDITY_SCISSORS_SPREAD_NAME,
            left_name="cn_m1_yoy",
            right_name="cn_m2_yoy",
            timestamps=timestamps,
        )
        return result

    return await asyncio.to_thread(_sync)


async def update_macro_cache() -> Dict[str, int]:
    """Fetch macro data and write the latest values to the local cache."""
    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}

    yf_data = await _fetch_yfinance_macro()
    missing_yf = {symbol: name for symbol, name in _SERIES_YF.items() if yf_data.get(name) is None}
    if missing_yf:
        yahoo_chart_data = await _fetch_yahoo_chart_macro(missing_yf)
        for name, value in yahoo_chart_data.items():
            if yf_data.get(name) is None and value is not None:
                yf_data[name] = value
                logger.debug("macro_collector: yahoo chart fallback filled {}={:.4f}", name, value)
    for name, value in yf_data.items():
        if value is None:
            continue
        path = _CACHE_DIR / f"{name}.parquet"
        pd.Series(
            {datetime.now(timezone.utc).strftime("%Y-%m-%d"): float(value)},
            name=name,
            dtype=float,
        ).to_frame().to_parquet(path)
        updated[name] = 1
        logger.debug("macro_collector: yfinance cached {}={:.4f}", name, value)

    china_task = asyncio.create_task(_fetch_china_macro())

    key = _api_key()
    fred_task = asyncio.create_task(_fetch_fred_macro(key)) if key else None
    if fred_task is not None:
        fred_data = await fred_task
        for name, value in fred_data.items():
            if value is not None:
                updated[name] = 1
    else:
        logger.debug("macro_collector: no FRED key; skipping fed_rate/cpi_yoy/ppi_yoy/m1_yoy/m2_yoy/ppi_cpi_gap/m1_m2_gap")

    china_data = await china_task
    for name, value in china_data.items():
        if value is not None:
            updated[name] = 1

    return updated


def load_macro_snapshot() -> Dict[str, Optional[float]]:
    """Read the latest cached macro values. Returns None for missing/corrupt series."""
    snapshot: Dict[str, Optional[float]] = {name: None for name in _ALL_MACRO_NAMES}

    for name in _ALL_MACRO_NAMES:
        snapshot[name] = _read_cached_series_value(name)

    if snapshot.get(_SCISSORS_SPREAD_NAME) is None:
        ppi_yoy = snapshot.get("ppi_yoy")
        cpi_yoy = snapshot.get("cpi_yoy")
        if ppi_yoy is not None and cpi_yoy is not None:
            snapshot[_SCISSORS_SPREAD_NAME] = round(float(ppi_yoy) - float(cpi_yoy), 4)

    if snapshot.get(_LIQUIDITY_SCISSORS_SPREAD_NAME) is None:
        m1_yoy = snapshot.get("m1_yoy")
        m2_yoy = snapshot.get("m2_yoy")
        if m1_yoy is not None and m2_yoy is not None:
            snapshot[_LIQUIDITY_SCISSORS_SPREAD_NAME] = round(float(m1_yoy) - float(m2_yoy), 4)

    if snapshot.get(_CN_SCISSORS_SPREAD_NAME) is None:
        cn_ppi_yoy = snapshot.get("cn_ppi_yoy")
        cn_cpi_yoy = snapshot.get("cn_cpi_yoy")
        if cn_ppi_yoy is not None and cn_cpi_yoy is not None:
            snapshot[_CN_SCISSORS_SPREAD_NAME] = round(float(cn_ppi_yoy) - float(cn_cpi_yoy), 4)

    if snapshot.get(_CN_LIQUIDITY_SCISSORS_SPREAD_NAME) is None:
        cn_m1_yoy = snapshot.get("cn_m1_yoy")
        cn_m2_yoy = snapshot.get("cn_m2_yoy")
        if cn_m1_yoy is not None and cn_m2_yoy is not None:
            snapshot[_CN_LIQUIDITY_SCISSORS_SPREAD_NAME] = round(float(cn_m1_yoy) - float(cn_m2_yoy), 4)

    return snapshot


def _group_values(snapshot: Dict[str, Optional[float]], keys: Iterable[str]) -> Dict[str, Optional[float]]:
    return {key: snapshot.get(key) for key in keys}


def group_macro_snapshot(snapshot: Optional[Dict[str, Optional[float]]] = None) -> Dict[str, Dict[str, Optional[float]]]:
    """Split the flat macro snapshot into cross-market, US, and China views."""
    snap = dict(snapshot or load_macro_snapshot() or {})
    return {
        "market": _group_values(snap, _MARKET_KEYS),
        "us": _group_values(snap, _US_MACRO_KEYS),
        "china": _group_values(snap, _CHINA_MACRO_KEYS),
    }
