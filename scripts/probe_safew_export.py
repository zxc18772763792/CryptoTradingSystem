"""Bounded SafeW system-notification import probe; no login, network, model or trading calls.

Reads one desktop JSON chat export. The only allowed peer is the default SafeW
service account (777000). Message bodies may contain login codes, so only
metadata and content fingerprints are persisted. This is historical import,
not proof of a continuously connected message collector.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.utils.file_lock import advisory_file_lock

PEER_ID = 777000
MAX_BYTES = 8 * 1024 * 1024
MAX_MESSAGES = 1000


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Observation time must have a timezone")
    return value.astimezone(timezone.utc).isoformat()


def _integer(value, *, minimum=1) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Invalid integer field")
    if isinstance(value, str) and (not value.isascii() or not value.isdecimal()):
        raise ValueError("Invalid integer field")
    number = int(value)
    if number < minimum:
        raise ValueError("Integer field out of range")
    return number


def _text(value) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        raise ValueError("Invalid message text")
    parts = []
    for part in value:
        if isinstance(part, str):
            parts.append(part)
        elif isinstance(part, dict) and isinstance(part.get("text"), str):
            parts.append(part["text"])
        else:
            raise ValueError("Invalid text segment")
    return "".join(parts)


def parse_export(raw: bytes, observed_at: datetime) -> list[dict]:
    """Validate the whole export before returning body-free message envelopes."""
    observed = _utc(observed_at)
    if len(raw) > MAX_BYTES:
        raise ValueError("Export exceeds probe size limit")
    document = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(document, dict) or _integer(document.get("id")) != PEER_ID:
        raise ValueError("Only the default SafeW service peer is allowed")
    if document.get("type") != "personal_chat" or document.get("name") != "SafeW":
        raise ValueError("Unexpected default service export identity")
    messages = document.get("messages")
    if not isinstance(messages, list) or not 1 <= len(messages) <= MAX_MESSAGES:
        raise ValueError("Export must contain a bounded nonempty message list")
    envelopes = []
    seen = set()
    for message in messages:
        if not isinstance(message, dict) or message.get("type") != "message":
            raise ValueError("Unsupported message record")
        message_id = _integer(message.get("id"))
        if message_id in seen or message.get("from_id") != "user777000":
            raise ValueError("Unexpected message identity")
        seen.add(message_id)
        # The human-readable export date has no timezone. Use the actual epoch.
        published_at = datetime.fromtimestamp(_integer(message.get("date_unixtime")), timezone.utc)
        if published_at > observed_at:
            raise ValueError("Message publication is after observation")
        edited_at = None
        if message.get("edited_unixtime") is not None:
            edited = datetime.fromtimestamp(_integer(message["edited_unixtime"]), timezone.utc)
            if not published_at <= edited <= observed_at:
                raise ValueError("Invalid edit chronology")
            edited_at = _utc(edited)
        content = _text(message.get("text"))
        # Do not persist the body, rich-text URLs, sender details or media paths.
        fingerprint = hashlib.sha256(json.dumps({
            "text": content, "published_at_utc": _utc(published_at),
            "edited_at_utc": edited_at, "has_media": any(
                key in message for key in ("photo", "file", "media_type")),
        }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        envelopes.append({
            "source_id": f"safew:{PEER_ID}:{message_id}",
            "message_id": message_id,
            "published_at_utc": _utc(published_at),
            "edited_at_utc": edited_at,
            "first_observed_at_utc": observed,
            "content_sha256": fingerprint,
            "has_text": bool(content),
            "has_media": any(key in message for key in ("photo", "file", "media_type")),
            "provenance": "historical_desktop_export",
            "source_kind": "platform_service_notification",
            "body_retained": False,
            "strategy_eligible": False,
            "exclusion_reason": "system_notification_probe",
        })
    return envelopes


def _atomic_json(path: Path, value: dict):
    descriptor, temporary = tempfile.mkstemp(prefix=".safew-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def import_export(export_path: Path, output_dir: Path, *, observed_at=None) -> dict:
    observed_at = observed_at or datetime.now(timezone.utc)
    export_path, output_dir = Path(export_path), Path(output_dir)
    with export_path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    envelopes = parse_export(raw, observed_at)
    state_path = output_dir / "status.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with advisory_file_lock(state_path):
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {
            "schema_version": 1, "provider": "safew", "peer_id": PEER_ID,
            "source_kind": "platform_service_notification", "versions": {},
        }
        if state.get("schema_version") != 1 or state.get("provider") != "safew" or state.get("peer_id") != PEER_ID:
            raise ValueError("Unexpected probe state identity")
        versions = state["versions"]
        new = 0
        for envelope in envelopes:
            key = envelope["source_id"] + ":" + envelope["content_sha256"]
            if key not in versions:
                versions[key] = envelope
                new += 1
        summary = {
            "provider": "safew", "peer_id": PEER_ID,
            "status": "historical_export_imported",
            "checked_at_utc": _utc(observed_at),
            "export_sha256": hashlib.sha256(raw).hexdigest(),
            "messages_in_export": len(envelopes),
            "new_versions": new, "duplicate_versions": len(envelopes) - new,
            "total_unique_messages": len({value["message_id"] for value in versions.values()}),
            "total_versions": len(versions),
            "text_messages_in_export": sum(item["has_text"] for item in envelopes),
            "media_messages_in_export": sum(item["has_media"] for item in envelopes),
            "published_from_utc": min(item["published_at_utc"] for item in envelopes),
            "published_to_utc": max(item["published_at_utc"] for item in envelopes),
            "body_retained": False, "strategy_eligible_messages": 0,
            "live_connection_verified": False,
            "continuous_coverage_verified": False,
        }
        state["last_import"] = summary
        _atomic_json(state_path, state)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path, help="SafeW desktop single-chat JSON export")
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/safew_probe")
    args = parser.parse_args()
    try:
        summary = import_export(args.export, args.output)
    except Exception as exc:
        # Exception messages from parsers can contain source text or local paths.
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
