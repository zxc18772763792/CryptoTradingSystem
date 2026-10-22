"""Read allowlisted public Telegram previews without a user login or API key.

This collector reads one preview page per channel, not private chats or complete
history. Public posts are third-party commentary; edits are annotated when
visible, but the existing news store does not synchronize already stored edits.
Picture-only posts (no caption) never enter the news store, but they do reach
the first-receipt research archive, which marks them as media-only. One
channel's failure does not stop the others; only an all-channel failure raises.
"""
from __future__ import annotations

import copy
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from core.news.collectors.common import BaseNewsCollector, BeautifulSoup, clamp_int


_CHANNEL_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{3,31}\Z")
_POST_RE = re.compile(r"([A-Za-z][A-Za-z0-9_]{3,31})/([1-9][0-9]{0,14})\Z")
_TAG_RE = re.compile(r"(?<![\w/])[$#]([A-Za-z][A-Za-z0-9]{0,14})(?!\w)")
_PAIR_RE = re.compile(r"(?<!\w)([A-Z][A-Z0-9]{0,14})/?USDT(?!\w)")
_NON_COINS = {"USD", "USDT", "USDC", "UPDATE", "CRYPTO", "TRADING", "NEWS", "BITCOIN"}
_RECAP_RE = re.compile(r"top catches|detected early by|after the alert", re.IGNORECASE)
logger = logging.getLogger(__name__)


def _unseen_ids_before(widgets: List[Any], channel: str) -> Dict[int, List[int]]:
    """Message IDs absent between consecutive visible posts of one preview page.

    An album occupies one ID per media item but renders as one widget keyed by
    its first ID, so those IDs are not counted as missing.
    """
    spans: Dict[int, int] = {}
    for widget in widgets:
        post = _POST_RE.fullmatch(str(widget.get("data-post") or ""))
        if not post or post.group(1).lower() != channel:
            continue
        group = widget.select_one(".tgme_widget_message_grouped_wrap")
        media = len(group.select(".tgme_widget_message_photo_wrap, .tgme_widget_message_video_player")) if group else 0
        spans[int(post.group(2))] = max(1, media)
    gaps: Dict[int, List[int]] = {}
    ordered = sorted(spans)
    for previous, current in zip(ordered, ordered[1:]):
        gaps[current] = list(range(previous + spans[previous], current))[:100]
    return gaps


def _public_channels(value: Any) -> List[str]:
    """Accept explicit usernames only, so config cannot become an arbitrary URL."""
    values = value.split(",") if isinstance(value, str) else value
    if not isinstance(values, (list, tuple)) or not values or len(values) > 10:
        raise ValueError("telegram_alastar_channels must contain 1 to 10 public usernames")
    channels: List[str] = []
    for value in values:
        channel = str(value).strip().removeprefix("@").lower()
        if not _CHANNEL_RE.fullmatch(channel):
            raise ValueError("Telegram sources must be public channel usernames, not URLs or invites")
        if channel not in channels:
            channels.append(channel)
    return channels


def _publication_time(value: Any) -> Optional[datetime]:
    """Never invent a date for a missing, malformed, or timezone-less date."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (ValueError, OverflowError):
        return None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return None
    return timestamp.astimezone(timezone.utc)


def _symbols(text: str) -> List[str]:
    coins: List[str] = []
    for match in _TAG_RE.finditer(text):
        coin = match.group(1).upper()
        if coin == "BITCOIN":
            coin = "BTC"
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


class TelegramPublicChannelCollector(BaseNewsCollector):
    """Collect text and photo captions from allowlisted ``t.me/s`` pages."""

    provider_name = "telegram_alastar"
    MAX_HTML_BYTES = 2 * 1024 * 1024

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        defaults = (cfg or {}).get("defaults") or {}
        configured = defaults.get("telegram_alastar_channels", ["alastartrades"])
        self.channels = _public_channels(configured)
        super().__init__(cfg)
        self.max_text_chars = clamp_int(defaults.get("telegram_alastar_max_text_chars"), 16000, 64, 20000)
        self.observation_archive_enabled = defaults.get("telegram_alastar_observation_archive") is True
        archived = defaults.get("telegram_observation_archive_channels", ["alastartrades"])
        self.archive_channels = [c for c in _public_channels(archived) if c in self.channels]
        if self.min_interval_sec <= 0:
            self.min_interval_sec = 15.0

    def _from_html(self, html: str, channel: Optional[str] = None, *, preserve_full_text: bool = False,
                   include_media_only: bool = False) -> List[Dict[str, Any]]:
        if len(html) > self.MAX_HTML_BYTES or len(html.encode("utf-8")) > self.MAX_HTML_BYTES:
            raise RuntimeError("Telegram public preview exceeds the HTML size limit")
        if BeautifulSoup is None:
            raise RuntimeError("bs4 is required for Telegram public preview parsing")
        if channel is None:
            if len(self.channels) != 1:
                raise ValueError("channel must be supplied for a multi-channel collector")
            channel = self.channels[0]
        channel = str(channel).lower()
        if channel not in self.channels:
            raise ValueError("Telegram channel is outside the configured allowlist")

        soup = BeautifulSoup(html, "html.parser")
        widgets = soup.select(".tgme_widget_message[data-post]")
        if not widgets:
            raise RuntimeError("Telegram public preview is unavailable or its message markup changed")
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        matching_widgets = 0
        unseen_before = _unseen_ids_before(widgets[:500], channel)
        for widget in widgets[:500]:
            post = _POST_RE.fullmatch(str(widget.get("data-post") or ""))
            if not post or post.group(1).lower() != channel:
                continue
            matching_widgets += 1
            message_id = int(post.group(2))
            source_id = f"telegram:{channel}:{message_id}"
            if source_id in seen:
                continue
            seen.add(source_id)

            own_message = copy.deepcopy(widget)
            # Reply previews also use tgme_widget_message_text. Keep only this
            # post's text/caption, never a quoted previous post or site chrome.
            for extra in own_message.select(".tgme_widget_message_reply, .js-message_reply_text, script, style"):
                extra.decompose()
            timestamp_node = own_message.select_one(".tgme_widget_message_date time[datetime]")
            published = _publication_time(timestamp_node.get("datetime") if timestamp_node else None)
            if published is None:
                continue
            text_nodes = own_message.select(".js-message_text") or own_message.select(".tgme_widget_message_text")
            parts: List[str] = []
            for text_node in text_nodes:
                for linebreak in text_node.find_all("br"):
                    linebreak.replace_with("\n")
                text = text_node.get_text("", strip=False).replace("\xa0", " ")
                text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
                if text:
                    parts.append(text)
            full_text = re.sub(r"\n{3,}", "\n\n", "\n\n".join(parts)).strip()
            photos = own_message.select(".tgme_widget_message_photo_wrap")
            media_only = not full_text
            if media_only and not (include_media_only and photos):
                continue
            content = full_text if preserve_full_text else full_text[:self.max_text_chars]
            # Include the opening body, rather than just '#Q update', because
            # storage also de-duplicates titles inside a publication time bucket.
            title = re.sub(r"\s+", " ", content).strip()[:600]
            meta = own_message.select_one(".tgme_widget_message_meta")
            edited = bool(own_message.select_one('[class*="edited"]')) or bool(
                meta and re.search(r"\bedited\b", meta.get_text(" ", strip=True), re.IGNORECASE)
            )
            url = f"https://t.me/{channel}/{message_id}"
            symbols = _symbols(content)
            items.append(
                {
                    "source": self.provider_name,
                    "source_id": source_id,
                    "title": title,
                    "url": url,
                    "content": content,
                    "published_at": published.isoformat(),
                    "lang": "en",
                    "symbols": symbols,
                    "payload": {
                        "provider": self.provider_name,
                        "source_id": source_id,
                        "channel": channel,
                        "message_id": message_id,
                        "public_url": url,
                        "preview_url": f"https://t.me/s/{channel}",
                        "provenance": "telegram_public_preview",
                        "source_kind": "third_party_commentary",
                        "is_third_party_commentary": True,
                        "telegram_edited": edited,
                        "has_photo": bool(photos),
                        "photo_count": len(photos),
                        "media_only": media_only,
                        "original_text": content,
                        "text_truncated": len(full_text) > len(content),
                        "symbols": symbols,
                        # Retrospective "top catches" lists tag many coins but are
                        # self-reported track records, not new signals.
                        "is_promotional_recap": bool(_RECAP_RE.search(content)),
                        # IDs missing between this and the previous visible post on
                        # the same page (deleted/hidden posts); None = page start.
                        "unseen_message_ids_before": unseen_before.get(message_id),
                    },
                }
            )
        if not matching_widgets:
            raise RuntimeError("Telegram public preview does not match the configured channel")
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
        items: List[Dict[str, Any]] = []
        self.last_channel_errors: Dict[str, str] = {}
        for channel in self.channels:
            try:
                page_items = self._pull_channel(channel)
            except Exception as exc:
                # One channel's outage or markup change must not drop the others.
                self.last_channel_errors[channel] = f"{type(exc).__name__}: {exc}"
                logger.warning("Telegram channel %s failed: %s", channel, exc)
                continue
            for item in page_items:
                if datetime.fromisoformat(item["published_at"]) >= since:
                    items.append(item)
        if self.last_channel_errors and len(self.last_channel_errors) == len(self.channels):
            raise RuntimeError("All Telegram channels failed: " + "; ".join(
                f"{channel}: {error}" for channel, error in self.last_channel_errors.items()))
        items.sort(key=lambda item: (item["published_at"], item["payload"]["message_id"]), reverse=True)
        return items[:limit]

    def _pull_channel(self, channel: str) -> List[Dict[str, Any]]:
        endpoint = f"https://t.me/s/{channel}"
        response = self._request(endpoint)
        final_url = urlsplit(str(getattr(response, "url", endpoint) or endpoint))
        if final_url.scheme != "https" or final_url.netloc.lower() != "t.me" or final_url.path.rstrip("/").lower() != f"/s/{channel}":
            raise RuntimeError("Telegram public preview redirected away from its public channel")
        body = response.text or ""
        received_at = datetime.now(timezone.utc)
        page_items = self._from_html(body, channel=channel)
        if self.observation_archive_enabled and channel in self.archive_channels:
            # Preserve first receipt and observed revisions before news age /
            # URL de-duplication can discard them. This only queues research
            # review; it never creates an executable order or calls an AI API.
            try:
                from core.news.alastar_observation import archive_dir, observe_records
                raw = getattr(response, "content", None)
                observe_records(
                    self._from_html(body, channel=channel, preserve_full_text=True, include_media_only=True),
                    output=archive_dir(channel), mode="live_public_preview", observed_at=received_at,
                    evidence={"url": endpoint, "collector": self.provider_name},
                    raw_preview=raw if isinstance(raw, bytes) else body.encode("utf-8"),
                    channel=channel,
                )
            except Exception:
                # Research archive failure must be visible without making
                # normal news ingestion depend on disk/research availability.
                logger.exception("Telegram research observation archive failed for %s", channel)
                for item in page_items:
                    item["payload"]["observation_archive_status"] = "failed"
        return page_items
