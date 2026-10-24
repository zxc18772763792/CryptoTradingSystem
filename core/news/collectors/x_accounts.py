"""Read allowlisted X (Twitter) accounts through one dedicated logged-in session.

X has no public read path, so this collector uses the ``auth_token`` cookie of a
dedicated throwaway account (``X_AUTH_TOKEN`` in ``.env``, never in the repository)
through the Scweet library, which calls X's undocumented web GraphQL endpoints.
That is against X's terms of service and the endpoints can change without notice,
so the source is off unless ``NEWS_ENABLE_X_ACCOUNTS=true`` and it fails closed: a
rejected token or a rate limit pauses every poll for a while instead of retrying.

Only the handles in ``x_accounts_handles`` are read, one timeline request per
handle per poll. Retweets and replies to other accounts are skipped because they
are not the account's own words; a quote keeps the account's text, with the quoted
text in the payload. Posts are third-party commentary. Pictures without text never
enter the news store. Like the Telegram collector, one handle's failure does not
stop the others and only an all-handle failure raises.
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import re
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dt_parser

from core.news.collectors.common import BaseNewsCollector, clamp_int


_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}\Z")
_CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9]{1,14})(?![\w$])")
_PAIR_RE = re.compile(r"(?<!\w)([A-Z][A-Z0-9]{1,14})/?USDT(?!\w)")
_NON_COINS = {"USD", "USDT", "USDC", "EUR", "GBP", "JPY", "CNY"}
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
STATE_DB = ROOT / "data" / "x_accounts" / "scweet_state.db"  # holds the session cookies; data/ is gitignored

_AUTH_PAUSE_SEC = 6 * 3600  # a rejected or locked token needs the user, so do not keep asking
_RATE_PAUSE_SEC = 30 * 60

_client_lock = threading.Lock()
_fetch_lock = threading.Lock()  # one X request sequence at a time, across collector instances
_clients: Dict[Tuple[str, str], Any] = {}
_pauses: Dict[str, Tuple[float, str]] = {}


def setting(name: str) -> str:
    """Process environment first, then .env / .env.local through settings.

    settings reads those files without exporting them to os.environ, so a bare
    os.getenv would never see a token kept in .env.local.
    """
    value = str(os.getenv(name) or "").strip()
    if value:
        return value
    from config.settings import settings

    return str(getattr(settings, name, "") or "").strip()


def x_accounts_enabled() -> bool:
    return setting("NEWS_ENABLE_X_ACCOUNTS").lower() in {"1", "true", "yes", "on", "y"}


def x_auth_token() -> str:
    return setting("X_AUTH_TOKEN")


def _x_handles(value: Any) -> List[str]:
    """Accept explicit handles only, so config cannot become an arbitrary URL."""
    values = value.split(",") if isinstance(value, str) else value
    if not isinstance(values, (list, tuple)) or not values or len(values) > 20:
        raise ValueError("x_accounts_handles must contain 1 to 20 X handles")
    handles: List[str] = []
    for value in values:
        handle = str(value).strip().removeprefix("@").lower()
        if not _HANDLE_RE.fullmatch(handle):
            raise ValueError("X sources must be handles, not URLs or profile links")
        if handle not in handles:
            handles.append(handle)
    return handles


def _publication_time(value: Any) -> Optional[datetime]:
    """Never invent a date for a missing, malformed, or timezone-less date."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        timestamp = dt_parser.parse(text)
    except (ValueError, OverflowError):
        return None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return None
    return timestamp.astimezone(timezone.utc)


def _symbols(text: str) -> List[str]:
    """Cashtags and ``XXXUSDT`` pairs only; plain hashtags are too ambiguous to be a coin."""
    coins: List[str] = []
    for match in _CASHTAG_RE.finditer(text):
        coin = match.group(1).upper()
        if coin in _NON_COINS:
            continue
        pair = coin if coin.endswith("USDT") else f"{coin}USDT"
        if pair not in coins:
            coins.append(pair)
    for match in _PAIR_RE.finditer(text):
        pair = f"{match.group(1)}USDT"
        if pair not in coins:
            coins.append(pair)
    return coins[:30]


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]


def _failure_kind(exc: BaseException) -> str:
    names = {cls.__name__ for cls in type(exc).__mro__}
    if names & {"AuthError", "AccountSessionAuthError"}:
        return "auth"
    if names & {"RateLimitError", "AccountPoolExhausted"}:
        return "rate_limit"
    return "other"


def _run_blocking(func, *args, **kwargs):
    """Scweet's sync API calls asyncio.run, which refuses to run on a thread with a live loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return func(*args, **kwargs)
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(func, *args, **kwargs).result()


class XAccountsCollector(BaseNewsCollector):
    """Collect the own posts of allowlisted X accounts."""

    provider_name = "x_accounts"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        defaults = (cfg or {}).get("defaults") or {}
        self.handles = _x_handles(defaults.get("x_accounts_handles"))
        super().__init__(cfg)
        self._token = x_auth_token()
        if not self._token:
            raise ValueError("X_AUTH_TOKEN is not set")
        self._proxy = setting("X_PROXY") or setting("HTTPS_PROXY") or setting("HTTP_PROXY") or None
        self.tweets_per_account = clamp_int(defaults.get("x_accounts_tweets_per_account"), 20, 1, 100)
        self.max_text_chars = clamp_int(defaults.get("x_accounts_max_text_chars"), 8000, 64, 20000)
        self.daily_requests_limit = clamp_int(defaults.get("x_accounts_daily_requests_limit"), 300, 10, 1000)
        if self.min_interval_sec <= 0:
            self.min_interval_sec = 6.0
        if self.jitter_sec <= 0:
            self.jitter_sec = 3.0
        self.last_account_errors: Dict[str, str] = {}

    def _scrub(self, value: Any) -> str:
        return str(value).replace(self._token, "***")

    def _pause(self, kind: str, seconds: float, reason: str) -> None:
        _pauses[_fingerprint(self._token)] = (time.monotonic() + seconds, f"{kind}: {reason}")

    def _raise_if_paused(self) -> None:
        until, reason = _pauses.get(_fingerprint(self._token), (0.0, ""))
        remaining = until - time.monotonic()
        if remaining > 0:
            raise RuntimeError(f"x_accounts paused for another {int(remaining // 60) + 1} min after {reason}")

    def _client(self) -> Any:
        key = (_fingerprint(self._token), self._proxy or "")
        with _client_lock:
            client = _clients.get(key)
            if client is not None:
                return client
            try:
                from Scweet import Scweet, ScweetConfig
            except ImportError as exc:
                raise RuntimeError("Scweet is not installed (pip install 'Scweet>=5.8.2,<6')") from exc
            STATE_DB.parent.mkdir(parents=True, exist_ok=True)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                client = Scweet(
                    auth_token=self._token,
                    db_path=str(STATE_DB),
                    proxy=self._proxy,
                    # X rotates its GraphQL query ids; read fresh ones once per process.
                    manifest_scrape_on_init=True,
                    config=ScweetConfig(daily_requests_limit=self.daily_requests_limit),
                )
            problems = [self._scrub(w.message) for w in caught if issubclass(w.category, RuntimeWarning)]
            if problems:
                # Scweet only warns when X rejects the token; do not cache a client that cannot read.
                self._pause("auth", _AUTH_PAUSE_SEC, problems[0])
                raise RuntimeError(f"X login failed: {problems[0]}")
            _clients[key] = client
            return client

    def _from_tweets(self, handle: str, tweets: List[Any]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for tweet in tweets or []:
            if not isinstance(tweet, dict):
                continue
            tweet_id = str(tweet.get("tweet_id") or "").strip()
            if not tweet_id.isdigit() or tweet_id in seen:
                continue
            user = tweet.get("user") if isinstance(tweet.get("user"), dict) else {}
            # Promoted or foreign entries can sit in a timeline; keep the handle's own posts only.
            if str(user.get("screen_name") or "").lower() != handle:
                continue
            if tweet.get("is_retweet"):
                continue
            reply_to = str(tweet.get("in_reply_to_user") or "").lower()
            if reply_to and reply_to != handle:
                continue
            published = _publication_time(tweet.get("timestamp"))
            if published is None:
                continue
            full_text = re.sub(r"\n{3,}", "\n\n", html.unescape(str(tweet.get("text") or "")).replace("\r\n", "\n")).strip()
            media = tweet.get("media") if isinstance(tweet.get("media"), dict) else {}
            images = media.get("image_links") or []
            videos = media.get("video_links") or []
            if not full_text:
                continue
            seen.add(tweet_id)
            content = full_text[:self.max_text_chars]
            title = re.sub(r"\s+", " ", content).strip()[:600]
            url = f"https://x.com/{handle}/status/{tweet_id}"
            source_id = f"x:{handle}:{tweet_id}"
            symbols = _symbols(content)
            quoted = html.unescape(str(tweet.get("embedded_text") or "")).strip() if tweet.get("is_quote") else ""
            items.append(
                {
                    "source": self.provider_name,
                    "source_id": source_id,
                    "title": title,
                    "url": url,
                    "content": content,
                    "published_at": published.isoformat(),
                    "lang": str(tweet.get("lang") or "en")[:8],
                    "symbols": symbols,
                    "payload": {
                        "provider": self.provider_name,
                        "source_id": source_id,
                        "handle": handle,
                        "display_name": str(user.get("name") or "")[:100],
                        "tweet_id": tweet_id,
                        "public_url": url,
                        "provenance": "x_logged_in_session",
                        "source_kind": "third_party_commentary",
                        "is_third_party_commentary": True,
                        "is_quote": bool(tweet.get("is_quote")),
                        "quoted_text": quoted[:2000],
                        "in_reply_to_tweet_id": tweet.get("in_reply_to_tweet_id") or None,
                        "likes": _int(tweet.get("likes")),
                        "retweets": _int(tweet.get("retweets")),
                        "replies": _int(tweet.get("comments")),
                        "quotes": _int(tweet.get("quotes")),
                        "views": _int(tweet.get("views")),
                        "hashtags": [str(h) for h in (tweet.get("hashtags") or [])][:20],
                        "mentions": [str(m) for m in (tweet.get("mentions") or [])][:20],
                        "urls": [str(u) for u in (tweet.get("urls") or [])][:10],
                        "image_count": len(images),
                        "has_video": bool(videos),
                        "original_text": content,
                        "text_truncated": len(full_text) > len(content),
                        "symbols": symbols,
                    },
                }
            )
        return items

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = clamp_int(max_records, self.max_records, 1, 500)
        since = datetime.now(timezone.utc) - timedelta(minutes=clamp_int(since_minutes, 240, 1, 525600))
        self._raise_if_paused()
        items: List[Dict[str, Any]] = []
        self.last_account_errors = {}
        succeeded = 0
        with _fetch_lock:
            client = self._client()
            for handle in self.handles:
                try:
                    self._respect_rate_limit()
                    tweets = _run_blocking(client.get_profile_tweets, [handle], limit=self.tweets_per_account)
                except Exception as exc:
                    # One handle's failure must not drop the others, unless X is refusing the
                    # session itself: every further request would get the same answer.
                    reason = f"{type(exc).__name__}: {self._scrub(exc)}"
                    self.last_account_errors[handle] = reason
                    logger.warning("X account %s failed: %s", handle, reason)
                    kind = _failure_kind(exc)
                    if kind == "other":
                        continue
                    self._pause(kind, _AUTH_PAUSE_SEC if kind == "auth" else _RATE_PAUSE_SEC, reason)
                    break
                succeeded += 1
                for item in self._from_tweets(handle, tweets):
                    if datetime.fromisoformat(item["published_at"]) >= since:
                        items.append(item)
        if not succeeded:
            raise RuntimeError("All X accounts failed: " + "; ".join(
                f"{handle}: {error}" for handle, error in self.last_account_errors.items()))
        items.sort(key=lambda item: (item["published_at"], item["payload"]["tweet_id"]), reverse=True)
        return items[:limit]
