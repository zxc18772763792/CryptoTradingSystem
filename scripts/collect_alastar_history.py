"""Archive public Alastar preview history for offline research, without live DB writes.

Only follow pagination exposed by https://t.me/s/alastartrades. Archived wording
is the version visible at collection time, not a point-in-time trading dataset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qs, urljoin, urlsplit

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.news.collectors.common import BeautifulSoup
from core.news.collectors.telegram import TelegramPublicChannelCollector

CHANNEL = "alastartrades"
ENDPOINT = f"https://t.me/s/{CHANNEL}"
DEFAULT_OUTPUT = ROOT / "data" / "research" / "alastar_20261020"
_POST = re.compile(r"alastartrades/([1-9][0-9]{0,14})\Z", re.IGNORECASE)


class HistoryCollectionError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_preview_url(url: str, channel: str = CHANNEL) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc.lower() != "t.me" or parsed.path.rstrip("/").lower() != f"/s/{channel}" or parsed.fragment:
        raise HistoryCollectionError("Pagination left the allowlisted public channel")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if not query:
        return f"https://t.me/s/{channel}"
    if set(query) != {"before"} or len(query["before"]) != 1 or not re.fullmatch(r"[1-9][0-9]{0,14}", query["before"][0]):
        raise HistoryCollectionError("Pagination contains an unsupported public preview cursor")
    return f"https://t.me/s/{channel}?before={int(query['before'][0])}"


def _previous_url(soup: Any, current_url: str, channel: str = CHANNEL) -> Optional[str]:
    previous = soup.select_one('[rel~="prev"][href]') or soup.select_one("a.tme_messages_more[href]")
    if previous is None:
        return None
    raw_target = urljoin(current_url, str(previous.get("href") or ""))
    # At the oldest page Telegram leaves only an "after" (newer posts) link.
    # It is not an older-page cursor and must not be followed as history.
    parts = urlsplit(raw_target)
    if (parts.scheme == "https" and parts.netloc.lower() == "t.me"
        and parts.path.lower() == f"/s/{channel}" and set(parse_qs(parts.query)) == {"after"}
        and soup.select_one('[rel~="prev"][href]') is None):
        return None
    target = _safe_preview_url(raw_target, channel)
    before = parse_qs(urlsplit(target).query).get("before", [])
    if not before:
        raise HistoryCollectionError("Previous-page link has no before cursor")
    current = parse_qs(urlsplit(current_url).query).get("before", [])
    if current and int(before[0]) >= int(current[0]):
        raise HistoryCollectionError("Previous-page cursor did not move backwards")
    return target


def _retry_delay(value: str, attempt: int) -> float:
    if value:
        try:
            seconds = float(value)
        except ValueError:
            try:
                due = parsedate_to_datetime(value)
                if due.tzinfo is None:
                    due = due.replace(tzinfo=timezone.utc)
                seconds = (due - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                seconds = 5.0 * 2 ** attempt
        return max(1.5, seconds)
    return 5.0 * 2 ** attempt


def _fetch_page(session: Any, url: str, sleep_fn: Callable[[float], None]) -> bytes:
    """Bound downloads and retries; stop rather than ignore a long provider cooldown."""
    for attempt in range(3):
        response = None
        try:
            response = session.get(url, timeout=(10, 25), stream=True, allow_redirects=False)
            status = int(response.status_code)
            if status == 429 or 500 <= status <= 599:
                if attempt >= 2:
                    raise HistoryCollectionError(f"Public preview HTTP {status}; retry limit reached")
                delay = _retry_delay(str(response.headers.get("Retry-After") or ""), attempt)
                if delay > 60:
                    raise HistoryCollectionError(f"Public preview HTTP {status}; Retry-After exceeds 60 seconds, stopped for provider cooldown")
                response.close()
                response = None
                sleep_fn(delay)
                continue
            if status != 200:
                raise HistoryCollectionError(f"Public preview HTTP {status}; collection stopped")
            declared = str(response.headers.get("Content-Length") or "")
            if declared.isdigit() and int(declared) > TelegramPublicChannelCollector.MAX_HTML_BYTES:
                raise HistoryCollectionError("Public preview exceeds the HTML size limit")
            chunks = []
            size = 0
            for chunk in response.iter_content(chunk_size=16384):
                size += len(chunk)
                if size > TelegramPublicChannelCollector.MAX_HTML_BYTES:
                    raise HistoryCollectionError("Public preview exceeds the HTML size limit")
                chunks.append(chunk)
            return b"".join(chunks)
        except requests.RequestException as exc:
            if attempt >= 2:
                # Avoid copying proxy addresses or credentials from transport errors.
                raise HistoryCollectionError(f"Public preview transport failure ({type(exc).__name__}); retry limit reached") from None
            sleep_fn(5.0 * 2 ** attempt)
        finally:
            if response is not None:
                response.close()
    raise HistoryCollectionError("Public preview retry limit reached")


def _inventory(soup: Any, channel: str = CHANNEL) -> Dict[int, Dict[str, Any]]:
    observed = {}
    for widget in soup.select(".tgme_widget_message[data-post]"):
        match = re.fullmatch(re.escape(channel) + r"/([1-9][0-9]{0,14})", str(widget.get("data-post") or ""), re.IGNORECASE)
        if not match:
            continue
        message_id = int(match.group(1))
        date_node = widget.select_one(".tgme_widget_message_date time[datetime]")
        text_node = widget.select_one(".js-message_text")
        has_photo = bool(widget.select_one(".tgme_widget_message_photo_wrap"))
        has_video = bool(widget.select_one("video, .tgme_widget_message_video_player"))
        observed[message_id] = {
            "source_id": f"telegram:{channel}:{message_id}",
            "message_id": message_id,
            "published_at_raw": str(date_node.get("datetime") or "") if date_node else None,
            "has_text_or_caption": bool(text_node and text_node.get_text(" ", strip=True)),
            "has_photo": has_photo,
            "has_video": has_video,
        }
    return observed


def _id_gaps(ids: list[int]) -> list[dict[str, int]]:
    ordered = sorted(set(ids))
    return [
        {"start": previous + 1, "end": current - 1, "count": current - previous - 1}
        for previous, current in zip(ordered, ordered[1:]) if current > previous + 1
    ]


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def collect_history(
    output_dir: Path | str = DEFAULT_OUTPUT,
    max_pages: int = 40,
    delay_seconds: float = 1.8,
    *,
    channel: str = CHANNEL,
    session: Any = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    if channel not in {"alastartrades", "cryptoquant_alert", "cryptoquant_official"}:
        raise ValueError("Unknown research channel")
    if not 1 <= int(max_pages) <= 200:
        raise ValueError("max_pages must be between 1 and 200")
    if not 1.5 <= float(delay_seconds) <= 60:
        raise ValueError("delay_seconds must be between 1.5 and 60")
    if BeautifulSoup is None:
        raise RuntimeError("bs4 is required for public preview pagination")
    output = Path(output_dir).resolve()
    if (output / "messages.jsonl").exists() or (output / "collection_manifest.json").exists():
        raise FileExistsError("Research archive already exists; choose a fresh output directory")
    pages_dir = output / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    own_session = session is None
    session = session or requests.Session()
    if own_session:
        session.headers.update({"User-Agent": "crypto-trading-system-public-research/1.0"})
    collector = TelegramPublicChannelCollector({"defaults": {"telegram_alastar_channels": [channel]}})
    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "channel": channel,
        "public_channel_url": f"https://t.me/{channel}",
        "started_at": _now(),
        "status": "collecting",
        "max_pages": int(max_pages),
        "delay_seconds": float(delay_seconds),
        "messages_file": "messages.jsonl",
        "pages": [],
        "complete_channel_history": False,
        "complete_public_preview_pagination": False,
        "limitations": [
            "Only pages linked by the public Telegram preview are collected; private or paid chats and bots are outside this archive.",
            "Message ID gaps may represent deletion, service messages, albums, or content unavailable in public previews; their cause is unknown.",
            "Image/video-only posts without a text caption are inventoried but not OCR/transcribed or included as parsed messages.",
            "Published dates are original public post dates. Visible wording and edit markers reflect collection time, not wording known at publication.",
            "Telegram edit timestamps, prior revisions, deleted posts, and publication-time availability cannot be reconstructed from these previews.",
            "This research archive writes no live news database, triggers no AI tasks, and executes no trades.",
        ],
    }
    records: Dict[str, Dict[str, Any]] = {}
    observed: Dict[int, Dict[str, Any]] = {}
    seen_urls: set[str] = set()
    next_url: Optional[str] = f"https://t.me/s/{channel}"
    stop_reason = "max_pages"
    error = None
    try:
        for page_index in range(1, int(max_pages) + 1):
            if next_url in seen_urls:
                raise HistoryCollectionError("Repeated pagination URL; collection stopped")
            if page_index > 1:
                sleep_fn(float(delay_seconds))
            current_url = _safe_preview_url(str(next_url), channel)
            seen_urls.add(current_url)
            raw = _fetch_page(session, current_url, sleep_fn)
            collected_at = _now()
            cursor = parse_qs(urlsplit(current_url).query).get("before", ["latest"])[0]
            page_file = f"pages/{page_index:04d}_{cursor}.html"
            (output / page_file).write_bytes(raw)
            digest = hashlib.sha256(raw).hexdigest()
            html = raw.decode("utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            page_inventory = _inventory(soup, channel)
            page_info: Dict[str, Any] = {
                "page_number": page_index, "url": current_url, "file": page_file,
                "sha256": digest, "bytes": len(raw), "collected_at": collected_at,
                "observed_count": len(page_inventory), "observed_message_ids": sorted(page_inventory),
            }
            manifest["pages"].append(page_info)
            if not page_inventory:
                raise HistoryCollectionError("Public preview contains no channel messages; boundary is unverified")
            parsed = collector._from_html(html, channel=channel)
            parsed_ids = {item["source_id"] for item in parsed}
            duplicates = 0
            for message_id, details in page_inventory.items():
                details["first_collected_at"] = collected_at
                details["first_page_file"] = page_file
                details["parsed"] = details["source_id"] in parsed_ids
                if not details["parsed"]:
                    details["reason"] = (
                        "media_without_text_or_caption" if not details["has_text_or_caption"] and (details["has_photo"] or details["has_video"])
                        else "no_text_or_caption" if not details["has_text_or_caption"]
                        else "missing_or_invalid_publication_time"
                    )
                observed.setdefault(message_id, details)
            for item in parsed:
                source_id = item["source_id"]
                if source_id in records:
                    duplicates += 1
                    continue
                item["collected_at"] = collected_at
                if channel != CHANNEL:
                    item["source"] = f"telegram_{channel}"
                    item["payload"]["provider"] = item["source"]
                item["collection"] = {
                    "page_url": current_url, "page_file": page_file, "page_sha256": digest,
                    "original_published_at": item["published_at"],
                    "visible_edit_marker": item["payload"]["telegram_edited"],
                    "purpose": "historical_research",
                }
                records[source_id] = item
            page_info.update({"parsed_count": len(parsed), "duplicate_message_count": duplicates})
            next_url = _previous_url(soup, current_url, channel)
            page_info["previous_page_url"] = next_url
            if progress:
                progress(json.dumps({"page": page_index, "observed": len(observed), "parsed": len(records), "next": next_url}))
            if next_url is None:
                stop_reason = "public_history_boundary"
                manifest["complete_public_preview_pagination"] = True
                break
    except (HistoryCollectionError, RuntimeError, requests.RequestException) as exc:
        stop_reason = "collection_error"
        error = str(exc)
    finally:
        collector.close()
        if own_session:
            session.close()

    ordered = sorted(records.values(), key=lambda item: (item["published_at"], item["payload"]["message_id"]))
    messages_path = output / "messages.jsonl"
    with messages_path.open("w", encoding="utf-8", newline="\n") as stream:
        for item in ordered:
            stream.write(json.dumps(item, ensure_ascii=False) + "\n")
    ids = sorted(observed)
    manifest.update({
        "finished_at": _now(), "status": "complete" if manifest["complete_public_preview_pagination"] else "incomplete",
        "stop_reason": stop_reason, "error": error, "next_page_url": next_url,
        "page_count": len(manifest["pages"]), "parsed_message_count": len(ordered), "observed_message_count": len(ids),
        "earliest_published_at": ordered[0]["published_at"] if ordered else None,
        "latest_published_at": ordered[-1]["published_at"] if ordered else None,
        "oldest_observed_message_id": min(ids) if ids else None,
        "latest_observed_message_id": max(ids) if ids else None,
        "not_visible_id_ranges": _id_gaps(ids),
        "not_visible_id_count_within_observed_range": sum(gap["count"] for gap in _id_gaps(ids)),
        "unparsed_messages": [observed[message_id] for message_id in ids if not observed[message_id]["parsed"]],
        "messages_sha256": hashlib.sha256(messages_path.read_bytes()).hexdigest(),
        "output_dir": str(output),
    })
    _write_json(output / "collection_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--channel", choices=[CHANNEL, "cryptoquant_alert", "cryptoquant_official"], default=CHANNEL)
    parser.add_argument("--max-pages", type=int, default=40)
    parser.add_argument("--delay-seconds", type=float, default=1.8)
    args = parser.parse_args()
    manifest = collect_history(args.output_dir, args.max_pages, args.delay_seconds, channel=args.channel, progress=print)
    print(json.dumps({key: manifest[key] for key in ["status", "stop_reason", "page_count", "parsed_message_count", "observed_message_count", "earliest_published_at", "latest_published_at", "output_dir", "error"]}, ensure_ascii=True, indent=2))
    return 0 if manifest["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
