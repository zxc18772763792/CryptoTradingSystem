"""Read-only first-receipt archive for public Telegram channels (Alastar first).

Immutable run records are authoritative; state and the human review queue are
replaceable projections. This is neither a scheduler nor an execution signal.
One archive directory holds exactly one channel; ``archive_dir`` maps a channel
to its directory and keeps Alastar's original location.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterator
import uuid

ROOT = Path(__file__).resolve().parents[2]

CHANNEL = "alastartrades"
ENDPOINT = "https://t.me/s/alastartrades"
DEFAULT_OUTPUT = ROOT / "data/research/alastar_observation"
OTHER_CHANNELS_ROOT = ROOT / "data/research/telegram_observation"
_CHANNEL_NAME = re.compile(r"[a-z][a-z0-9_]{3,31}\Z")
MAX_DELAY_SECONDS = 600
MAX_RECORDS = 5000
MAX_INPUT_BYTES = 20 * 1024 * 1024
OUTAGE_GAP_SECONDS = 600     # a live gap longer than the eligibility cutoff is an outage, not polling
LATENCY_MARKS = (60, 180, 300, 600)
# F: is exFAT with 1 MiB clusters: one ~5 KB run file per poll costs ~0.8 GB a day per channel. Old
# duplicate-only runs are packed into gzip bundles. Ledgers read run files directly (health: the
# newest 12; first-receipt lookups: the newest 400, or any run carrying new_revisions), so the newest
# runs, recent runs and every run with a new revision always stay single files.
RUN_BUNDLES = "run_bundles"
COMPACT_KEEP_RECENT_RUNS = 2000
COMPACT_MIN_AGE = timedelta(hours=48)
COMPACT_MIN_BATCH = 500
COMPACT_MAX_BATCH = 1000  # bounds how long one pass holds the archive lock
_RUN_FILE = re.compile(r"\d{12}\.json\Z")
_BUNDLE_FILE = re.compile(r"(\d{12})-(\d{12})\.jsonl\.gz\Z")


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp must include an explicit timezone")
    return parsed.astimezone(timezone.utc)


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _atomic_bytes(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pending-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


@contextmanager
def _archive_lock(output: Path) -> Iterator[None]:
    """Nonblocking OS lock; a killed process releases it without stale-lock deletion."""
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".archive.lock").open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("Another observation owns the archive lock; retry after it finishes") from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _channel(channel: str) -> str:
    channel = str(channel or "").strip().removeprefix("@").lower()
    if not _CHANNEL_NAME.fullmatch(channel):
        raise ValueError("Archive channel must be a public Telegram username")
    return channel


def archive_dir(channel: str = CHANNEL) -> Path:
    """Alastar keeps its original directory; every other channel gets its own."""
    channel = _channel(channel)
    return DEFAULT_OUTPUT if channel == CHANNEL else OTHER_CHANNELS_ROOT / channel


def _normalise(record: dict[str, Any], channel: str = CHANNEL) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("Each observation record must be an object")
    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        raise ValueError("Message payload must be an object")
    message_id = payload.get("message_id")
    if not str(message_id or "").isdigit() or not 1 <= int(message_id) <= 10**15 - 1:
        raise ValueError("A positive Telegram message_id is required")
    message_id = int(message_id)
    channel = _channel(channel)
    key = f"telegram:{channel}:{message_id}"
    if str(payload.get("channel", channel)).lower() != channel or record.get("source_id", key) != key:
        raise ValueError("Message is outside this archive's channel")
    canonical_url = f"https://t.me/{channel}/{message_id}"
    if record.get("url", canonical_url) != canonical_url:
        raise ValueError("Message URL does not match its channel identity")
    # A picture-only post has no caption; keep its first receipt (for image
    # research) but never let it look like a text snapshot.
    media_only = payload.get("media_only", False)
    if not isinstance(media_only, bool) or (media_only and not payload.get("has_photo")):
        raise ValueError("media_only must be a boolean on a post with a photo")
    text = payload.get("original_text", record.get("content"))
    if not isinstance(text, str) or (text.strip() == "") != media_only:
        raise ValueError("Nonempty original message text is required unless the post is media-only")
    edited = payload.get("telegram_edited", False)
    truncated = payload.get("text_truncated", False)
    if not isinstance(edited, bool) or not isinstance(truncated, bool):
        raise ValueError("Edit/truncation flags must be booleans")
    raw_publication = record.get("published_at")
    try:
        publication = _utc(raw_publication).isoformat()
    except (ValueError, TypeError, OverflowError):
        publication = None
    version = {"text": text, "published_at_raw": raw_publication, "telegram_edited": edited, "text_truncated": truncated}
    if media_only:  # only then, so every existing text revision keeps its hash
        version["media_only"] = True
    return {
        "message_key": key, "message_id": message_id, "url": canonical_url,
        "text": text, "text_sha256": _digest(text.encode("utf-8")),
        "revision_sha256": _digest(_json_bytes(version)),
        "published_at_raw": raw_publication, "published_at_utc": publication,
        "telegram_edited": edited, "text_truncated": truncated, "media_only": media_only,
        # Preserve the exact JSON values as supplied, including content distinct
        # from original_text. Do not silently shorten or reformat message text.
        "raw_message": record,
    }


def _eligibility(item: dict[str, Any], observed: datetime, mode: str, is_first: bool) -> list[str]:
    reasons = []
    if mode != "live_public_preview":
        reasons.append("historical_import")
    if not is_first:
        reasons.append("revision_after_first_observation")
    if item["telegram_edited"]:
        reasons.append("visible_edit")
    if item["text_truncated"]:
        reasons.append("truncated_source_text")
    if item.get("media_only"):
        reasons.append("media_only_no_text")
    if item["published_at_utc"] is None:
        reasons.append("invalid_or_timezone_missing_publication")
    else:
        delay = (observed - _utc(item["published_at_utc"])).total_seconds()
        if delay < 0:
            reasons.append("future_publication")
        elif delay > MAX_DELAY_SECONDS:
            reasons.append("first_observation_more_than_10_minutes_late")
    return reasons


def _apply_run(state: dict[str, Any], run: dict[str, Any], channel: str = CHANNEL) -> None:
    if run.get("channel", CHANNEL) != channel:
        raise RuntimeError("Archive run belongs to another channel; preserve files for manual review")
    for revision in run["new_revisions"]:
        checked = _normalise(revision["raw_message"], channel)
        if any(revision[field] != checked[field] for field in ("message_key", "text", "text_sha256", "revision_sha256")):
            raise RuntimeError("Archive revision content/hash is invalid; preserve files for manual review")
        key = revision["message_key"]
        if key not in state:
            state[key] = {"first": revision, "revisions": {}, "latest_revision_sha256": None}
        if revision["revision_sha256"] in state[key]["revisions"]:
            raise RuntimeError("Duplicate revision in immutable archive")
        state[key]["revisions"][revision["revision_sha256"]] = revision
    for observation in run["observations"]:
        item = state[observation["message_key"]]
        item["latest_revision_sha256"] = observation["revision_sha256"]
        item["last_observed_at_utc"] = run["observed_at_utc"]
        item["latest_observation_mode"] = run["mode"]


def _run_files_after(output: Path, sequence: int) -> Iterator[tuple[int, bytes]]:
    """Single run files after `sequence`, until the first one that does not exist (the checkpoint path)."""
    while True:
        path = output / "runs" / f"{sequence + 1:012d}.json"
        if not path.exists():
            return
        sequence += 1
        yield sequence, path.read_bytes()


def _bundle_runs(path: Path) -> Iterator[tuple[int, bytes]]:
    """(sequence, exact run-file bytes) of one bundle; each line carries the run's own SHA-256."""
    match = _BUNDLE_FILE.fullmatch(path.name)
    if not match:
        raise RuntimeError("Unexpected file name in run_bundles/")
    first, last = int(match.group(1)), int(match.group(2))
    previous = first - 1
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            entry = json.loads(line)
            raw = entry["raw"].encode("utf-8")
            number = entry["sequence"]
            if not first <= number <= last or number <= previous or _digest(raw) != entry["sha256"]:
                raise RuntimeError("Archive run bundle is invalid; preserve files for manual review")
            previous = number
            yield number, raw


def _committed_runs(output: Path) -> Iterator[tuple[int, bytes]]:
    """Every committed run in sequence order, merged from gzip bundles and single run files.

    A run present in both (a crash between writing a bundle and deleting its files) must be
    byte-identical; it is yielded once. Missing sequences are left for the caller's chain check.
    """
    files: dict[int, Path] = {}
    for path in (output / "runs").glob("*.json"):
        if not _RUN_FILE.fullmatch(path.name):
            raise RuntimeError("Unexpected file name in runs/; preserve files for manual review")
        files[int(path.stem)] = path
    bundles = sorted((output / RUN_BUNDLES).glob("*.jsonl.gz")) if (output / RUN_BUNDLES).exists() else []
    bundled = (item for bundle in bundles for item in _bundle_runs(bundle))
    numbers = sorted(files)
    index = 0
    item = next(bundled, None)
    last = 0
    while item is not None or index < len(numbers):
        number = numbers[index] if index < len(numbers) else None
        if item is not None and (number is None or item[0] <= number):
            if item[0] <= last:
                raise RuntimeError("Archive run bundles overlap; preserve files for manual review")
            if item[0] == number:
                if files[number].read_bytes() != item[1]:
                    raise RuntimeError("A bundled run differs from its run file; preserve files for manual review")
                index += 1
            last = item[0]
            yield item
            item = next(bundled, None)
        else:
            last = number
            yield number, files[number].read_bytes()
            index += 1


def _load_runs(output: Path, *, verify_full: bool = False, channel: str = CHANNEL) -> tuple[int, dict[str, Any], str | None]:
    """Tail-checked checkpoint + only uncheckpointed runs; full replay on recovery.

    Sequential filenames make the crash-recovery lookup independent of the
    number of prior receipts. --verify deliberately scans the whole chain.
    """
    sequence = 0
    state: dict[str, Any] = {}
    previous_hash = None
    checkpoint = None
    if not verify_full:
        try:
            candidate = json.loads((output / "state.json").read_bytes())
            stored_hash = candidate.pop("checkpoint_sha256")
            if (stored_hash == _digest(_json_bytes(candidate)) and candidate["schema_version"] == 1
                    and isinstance(candidate["run_count"], int) and candidate["run_count"] >= 1
                    and isinstance(candidate["messages"], dict) and candidate.get("channel") == channel
                    and candidate["last_run_file"] == f"runs/{candidate['run_count']:012d}.json"):
                checkpoint = candidate
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            pass
    if checkpoint is not None:
        sequence = checkpoint["run_count"]
        tail = output / checkpoint["last_run_file"]
        if not tail.exists() or _digest(tail.read_bytes()) != checkpoint["last_run_sha256"]:
            raise RuntimeError("Archive checkpoint tail hash chain is invalid; run --verify and preserve files for review")
        state = checkpoint["messages"]
        previous_hash = checkpoint["last_run_sha256"]
        source = _run_files_after(output, sequence)
    else:
        # Missing/corrupt projections are disposable. Verify every committed
        # run when reconstructing, including a missing sequence in the middle.
        source = _committed_runs(output)
    checked_evidence: set[str] = set()
    for number, raw in source:
        run = json.loads(raw)
        if (number != sequence + 1 or run.get("sequence") != sequence + 1
                or run.get("previous_run_sha256") != previous_hash):
            raise RuntimeError("Archive run sequence/hash chain is invalid; preserve files for manual review")
        preview = run.get("raw_preview") or {}
        if preview.get("persisted") and preview.get("sha256") not in checked_evidence:
            if (not isinstance(preview.get("sha256"), str) or len(preview["sha256"]) != 64
                    or any(character not in "0123456789abcdef" for character in preview["sha256"])):
                raise RuntimeError("Archive preview evidence hash is malformed")
            relative = f"evidence/{preview['sha256']}.html"
            if preview.get("file") != relative or not (output / relative).is_file():
                raise RuntimeError("Archive preview evidence is missing or has an invalid path")
            if _digest((output / relative).read_bytes()) != preview["sha256"]:
                raise RuntimeError("Archive preview evidence hash is invalid")
            checked_evidence.add(preview["sha256"])
        _apply_run(state, run, channel)
        sequence += 1
        previous_hash = _digest(raw)
    return sequence, state, previous_hash


def _projections(output: Path, run_count: int, messages: dict[str, Any], last_hash: str | None,
                 channel: str = CHANNEL) -> dict[str, Any]:
    queue = []
    excluded: Counter[str] = Counter()
    for key, item in sorted(messages.items()):
        first = item["first"]
        latest = item["revisions"][item["latest_revision_sha256"]]
        reasons = list(first["exclusion_reasons"])
        # A later edit never overwrites the first snapshot. Conservatively keep
        # revised messages out of the current queue, even if text reverts later.
        if len(item["revisions"]) > 1:
            reasons.append("message_has_observed_revisions")
        if latest["telegram_edited"] and "visible_edit" not in reasons:
            reasons.append("visible_edit")
        item["current_candidate_eligible"] = not reasons
        item["current_candidate_exclusion_reasons"] = reasons
        excluded.update(reasons)
        if not reasons:
            queue.append({
                "message_key": key, "url": first["url"], "text": first["text"],
                "text_sha256": first["text_sha256"], "revision_sha256": first["revision_sha256"],
                "published_at_utc": first["published_at_utc"], "first_observed_at_utc": first["observed_at_utc"],
                "review_status": "human_review_required", "execution_authorized": False,
            })
    summary = {"schema_version": 1, "channel": channel, "run_count": run_count, "message_count": len(messages),
               "revision_count": sum(len(item["revisions"]) for item in messages.values()),
               "candidate_review_count": len(queue), "exclusion_reason_counts": dict(excluded),
               "last_run_sha256": last_hash, "last_run_file": f"runs/{run_count:012d}.json" if run_count else None}
    checkpoint = {**summary, "messages": messages}
    checkpoint["checkpoint_sha256"] = _digest(_json_bytes(checkpoint))
    _atomic_bytes(output / "state.json", _json_bytes(checkpoint))
    _atomic_bytes(output / "candidate_review_queue.json", _json_bytes({
        **summary, "not_a_trading_signal": True,
        "definition": "First text snapshots received live within 600 seconds, unedited and never observed revised; still require human interpretation.",
        "candidates": queue,
    }))
    return summary


def verify_archive(output: Path | str = DEFAULT_OUTPUT, *, channel: str = CHANNEL) -> dict[str, Any]:
    """Fully verify all committed runs and rebuild projections, without a fetch."""
    output = Path(output).resolve()
    with _archive_lock(output):
        channel = _channel(channel)
        count, state, last_hash = _load_runs(output, verify_full=True, channel=channel)
        return {**_projections(output, count, state, last_hash, channel), "verified_full_chain": True}


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _poll_wait(gaps: list[float]) -> dict[str, Any]:
    """Wait from a uniformly timed post to the next live receipt, implied by the poll gaps alone.

    A post lands in a gap with probability proportional to its length, so the
    expected wait is E[g^2] / (2 E[g]); P(wait <= x) = sum(min(g, x)) / sum(g).
    Telegram's own preview lag comes on top and is only visible in real receipts.
    """
    total = sum(gaps)
    if not gaps or total <= 0:
        return {"gap_count": len(gaps)}
    return {"gap_count": len(gaps), "gap_median_seconds": _quantile(gaps, .5), "gap_p90_seconds": _quantile(gaps, .9),
            "gap_max_seconds": max(gaps), "expected_wait_seconds": sum(g * g for g in gaps) / (2 * total),
            "share_received_within_seconds": {str(x): sum(min(g, x) for g in gaps) / total for x in LATENCY_MARKS}}


def receipt_latency(output: Path | str = DEFAULT_OUTPUT, *, now: datetime | str | None = None,
                    channel: str = CHANNEL) -> dict[str, Any]:
    """On-demand latency evidence from committed live runs; no fetch, runs are never edited.

    Reports (1) publication-to-first-receipt delay of every message first seen
    live, and (2) the gaps between consecutive live receipts, which bound how
    late any future post can be seen. Gaps longer than OUTAGE_GAP_SECONDS are
    reported as outages and also summarised separately from normal polling.
    The time since the last live run is reported, never folded into the gaps.
    """
    output = Path(output).resolve()
    current = _utc(now or datetime.now(timezone.utc))
    with _archive_lock(output):
        run_count, state, last_hash = _load_runs(output, verify_full=True, channel=channel)
        live_times = []
        for _, raw in _committed_runs(output):
            run = json.loads(raw)
            if run["mode"] == "live_public_preview":
                live_times.append(_utc(run["observed_at_utc"]))
        samples = []
        for key, item in sorted(state.items()):
            first = item["first"]
            if first["mode"] != "live_public_preview" or first["published_at_utc"] is None:
                continue
            delay = (_utc(first["observed_at_utc"]) - _utc(first["published_at_utc"])).total_seconds()
            samples.append({"message_key": key, "published_at_utc": first["published_at_utc"],
                            "first_observed_at_utc": first["observed_at_utc"], "delay_seconds": delay,
                            "within_eligibility_cutoff": 0 <= delay <= MAX_DELAY_SECONDS})
        gaps = [(b - a).total_seconds() for a, b in zip(live_times, live_times[1:])]
        delays = [s["delay_seconds"] for s in samples]
        report = {
            "schema_version": 1, "channel": channel, "computed_at_utc": current.isoformat(),
            "run_count": run_count, "last_run_sha256": last_hash, "live_run_count": len(live_times),
            "first_live_run_at_utc": live_times[0].isoformat() if live_times else None,
            "last_live_run_at_utc": live_times[-1].isoformat() if live_times else None,
            "seconds_since_last_live_run": (current - live_times[-1]).total_seconds() if live_times else None,
            "outage_gap_seconds": OUTAGE_GAP_SECONDS,
            "outages": [{"from_utc": a.isoformat(), "to_utc": b.isoformat(), "seconds": (b - a).total_seconds()}
                        for a, b in zip(live_times, live_times[1:]) if (b - a).total_seconds() > OUTAGE_GAP_SECONDS],
            "poll_wait_all_gaps": _poll_wait(gaps),
            "poll_wait_excluding_outages": _poll_wait([g for g in gaps if g <= OUTAGE_GAP_SECONDS]),
            "live_first_receipts": {
                "count": len(samples), "median_delay_seconds": _quantile(delays, .5),
                "p90_delay_seconds": _quantile(delays, .9), "max_delay_seconds": max(delays) if delays else None,
                "within_cutoff_count": sum(s["within_eligibility_cutoff"] for s in samples), "samples": samples,
            },
            "definition": "Live receipt timestamps are the host UTC clock after each HTTPS response; "
                          "publication-to-receipt delay includes poll wait and Telegram preview lag.",
        }
        _atomic_bytes(output / "receipt_latency.json", _json_bytes(report))
        return report


def observe_records(
    records: list[dict[str, Any]], output: Path | str = DEFAULT_OUTPUT, *, mode: str = "historical_import",
    observed_at: datetime | str | None = None, evidence: dict[str, Any] | None = None,
    raw_preview: bytes | None = None, channel: str = CHANNEL,
) -> dict[str, Any]:
    """Commit one bounded observation. Timestamp injection is only for offline tests.

    The CLI always reads the host's UTC clock after receipt/import. An input's
    collected_at/observed_at can never become this archive's first seen time.
    """
    if mode not in {"historical_import", "live_public_preview"}:
        raise ValueError("Unsupported observation mode")
    if not isinstance(records, list) or len(records) > MAX_RECORDS:
        raise ValueError(f"At most {MAX_RECORDS} records may be observed per run")
    # A live claim must carry the actual preview evidence; cached JSONL is
    # deliberately unable to opt into prospective eligibility via CLI flags.
    if mode == "live_public_preview" and raw_preview is None:
        raise ValueError("A live observation requires its raw public preview")
    if raw_preview is not None and len(raw_preview) > 2 * 1024 * 1024:
        raise ValueError("Public preview exceeds the 2 MiB evidence bound")
    observed = _utc(observed_at or datetime.now(timezone.utc))
    channel = _channel(channel)
    normalised = [_normalise(record, channel) for record in records]
    output = Path(output).resolve()
    with _archive_lock(output):
        run_count, state, previous_hash = _load_runs(output, channel=channel)
        observations = []
        revisions = []
        pending: dict[str, set[str]] = {}
        for item in normalised:
            key, digest = item["message_key"], item["revision_sha256"]
            existing = state.get(key)
            known_message = existing is not None or key in pending
            if (existing and digest in existing["revisions"]) or digest in pending.get(key, set()):
                status = "duplicate"
            else:
                status = "new_revision" if known_message else "new_message"
                reasons = _eligibility(item, observed, mode, not known_message)
                revision = {**item, "observed_at_utc": observed.isoformat(), "mode": mode,
                            "source_collected_at": item["raw_message"].get("collected_at"),
                            "prospective_eligible_at_first_observation": not reasons, "exclusion_reasons": reasons}
                revisions.append(revision)
                pending.setdefault(key, set()).add(digest)
            observations.append({"message_key": key, "revision_sha256": digest, "status": status,
                                 "source_collected_at": item["raw_message"].get("collected_at")})
        sequence = run_count + 1
        run = {"schema_version": 1, "channel": channel, "sequence": sequence, "run_id": uuid.uuid4().hex,
               "previous_run_sha256": previous_hash, "mode": mode, "observed_at_utc": observed.isoformat(),
               "evidence": evidence or {}, "max_publication_delay_seconds": MAX_DELAY_SECONDS,
               "public_preview_history_reconstruction": False, "observations": observations, "new_revisions": revisions}
        if raw_preview is not None:
            digest = _digest(raw_preview)
            raw_path = output / "evidence" / f"{digest}.html"
            # View counters and page chrome may change every minute. Preserve
            # source HTML for new message/revision evidence; duplicate-only
            # runs keep the received body's hash without another page copy.
            if revisions and not raw_path.exists():
                _atomic_bytes(raw_path, raw_preview)
            elif raw_path.exists() and _digest(raw_path.read_bytes()) != digest:
                raise RuntimeError("Existing preview evidence hash is invalid")
            run["raw_preview"] = {"file": raw_path.relative_to(output).as_posix() if raw_path.exists() else None,
                                  "persisted": raw_path.exists(), "sha256": digest, "bytes": len(raw_preview)}
        path = output / "runs" / f"{sequence:012d}.json"
        raw_run = _json_bytes(run)
        _atomic_bytes(path, raw_run)
        # A crash here leaves a valid committed run. The next invocation repairs
        # only the uncheckpointed run(s); it never edits an older run.
        _apply_run(state, run, channel)
        summary = _projections(output, sequence, state, _digest(raw_run), channel)
        return {**summary, "run_file": path.relative_to(output).as_posix(),
                "observed_at_utc": observed.isoformat(), "mode": mode,
                "run_observation_count": len(observations), "new_revision_count": len(revisions)}


def _manifest(output: Path, channel: str) -> dict[str, Any]:
    path = output / RUN_BUNDLES / "manifest.json"
    if not path.exists():
        return {"schema_version": 1, "channel": channel, "packed_through": 0, "bundles": []}
    manifest = json.loads(path.read_bytes())
    if manifest.get("schema_version") != 1 or manifest.get("channel") != channel:
        raise RuntimeError("Run bundle manifest belongs to another channel or schema")
    return manifest


def compact_runs(output: Path | str = DEFAULT_OUTPUT, *, channel: str = CHANNEL, now: datetime | str | None = None,
                 keep_recent: int = COMPACT_KEEP_RECENT_RUNS, min_age: timedelta = COMPACT_MIN_AGE,
                 min_batch: int = COMPACT_MIN_BATCH, max_batch: int = COMPACT_MAX_BATCH,
                 dry_run: bool = False) -> dict[str, Any]:
    """Pack one batch of old duplicate-only run files into a gzip bundle; never edits a run.

    Only runs older than both the newest `keep_recent` sequences and `min_age`, and without
    new_revisions, are packed. The bundle is written and read back before any run file is
    deleted, and the manifest records progress, so a crash leaves at worst byte-identical
    duplicates that replay accepts and the next call removes. Returns what was (or would be) done.
    """
    output = Path(output).resolve()
    channel = _channel(channel)
    current = _utc(now or datetime.now(timezone.utc))
    with _archive_lock(output):
        manifest = _manifest(output, channel)
        count, _, _ = _load_runs(output, channel=channel)
        bundles = manifest["bundles"]
        if bundles and not bundles[-1]["files_removed"] and not dry_run:
            # Finish an interrupted deletion: only files whose bytes the bundle already holds.
            if bundles[-1]["file"]:
                for number, raw in _bundle_runs(output / bundles[-1]["file"]):
                    path = output / "runs" / f"{number:012d}.json"
                    if path.exists() and path.read_bytes() == raw:
                        path.unlink()
            bundles[-1]["files_removed"] = True
            _atomic_bytes(output / RUN_BUNDLES / "manifest.json", _json_bytes(manifest))
        start = manifest["packed_through"] + 1
        limit = min(count - keep_recent, start + max_batch - 1)
        chosen: list[tuple[int, bytes]] = []
        kept = 0
        last = start - 1
        for number in range(start, limit + 1):
            raw = (output / "runs" / f"{number:012d}.json").read_bytes()
            run = json.loads(raw)
            if run.get("sequence") != number:
                raise RuntimeError("Run file sequence is invalid; preserve files for manual review")
            if current - _utc(run["observed_at_utc"]) < min_age:
                break
            last = number
            if run["new_revisions"]:
                kept += 1  # first receipts and revisions stay files: ledgers look them up by path
            else:
                chosen.append((number, raw))
        result = {"channel": channel, "run_count": count, "scanned": last - start + 1, "packed": len(chosen),
                  "kept_with_revisions": kept, "packed_through": manifest["packed_through"], "dry_run": dry_run}
        if last - start + 1 < min_batch:
            return {**result, "packed": 0, "reason": "fewer old runs than one batch"}
        if dry_run:
            return {**result, "would_pack_through": last}
        entry = {"first_sequence": start, "last_sequence": last, "runs": len(chosen), "kept_with_revisions": kept,
                 "created_at_utc": current.isoformat(), "files_removed": False, "file": None}
        if chosen:
            path = output / RUN_BUNDLES / f"{start:012d}-{last:012d}.jsonl.gz"
            lines = "".join(json.dumps({"sequence": number, "sha256": _digest(raw), "raw": raw.decode("utf-8")},
                                       ensure_ascii=False, sort_keys=True) + "\n" for number, raw in chosen)
            payload = gzip.compress(lines.encode("utf-8"), mtime=0)
            _atomic_bytes(path, payload)
            if list(_bundle_runs(path)) != chosen:
                raise RuntimeError("Run bundle did not read back identically; nothing was deleted")
            entry.update(file=path.relative_to(output).as_posix(), sha256=_digest(payload), bytes=len(payload))
        bundles.append(entry)
        manifest["packed_through"] = last
        _atomic_bytes(output / RUN_BUNDLES / "manifest.json", _json_bytes(manifest))
        for number, _ in chosen:
            (output / "runs" / f"{number:012d}.json").unlink()
        entry["files_removed"] = True
        _atomic_bytes(output / RUN_BUNDLES / "manifest.json", _json_bytes(manifest))
        return {**result, "packed_through": last, "bundle": entry["file"]}


def import_jsonl(path: Path, max_records: int = MAX_RECORDS) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not 1 <= max_records <= MAX_RECORDS:
        raise ValueError(f"max_records must be between 1 and {MAX_RECORDS}")
    with path.open("rb") as stream:
        raw = stream.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError("Historical JSONL exceeds the 20 MiB input limit")
    lines = [line for line in raw.decode("utf-8-sig").splitlines() if line.strip()]
    if len(lines) > max_records:
        raise ValueError("Historical JSONL exceeds max_records; no partial import was written")
    records = [json.loads(line) for line in lines]
    if any(not isinstance(record, dict) for record in records):
        raise ValueError("Each historical JSONL line must be an object")
    return records, {"kind": "cached_jsonl", "filename": path.name, "sha256": _digest(raw), "bytes": len(raw),
                     "cached_timestamps_are_not_first_seen": True}


