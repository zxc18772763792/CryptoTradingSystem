"""Read-only Alastar observation archive, independent of the strategy engine.

Immutable run records are authoritative; state and the human review queue are
replaceable projections. This is neither a scheduler nor an execution signal.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterator
import uuid

ROOT = Path(__file__).resolve().parents[2]

CHANNEL = "alastartrades"
ENDPOINT = "https://t.me/s/alastartrades"
DEFAULT_OUTPUT = ROOT / "data/research/alastar_observation"
MAX_DELAY_SECONDS = 600
MAX_RECORDS = 5000
MAX_INPUT_BYTES = 20 * 1024 * 1024
OUTAGE_GAP_SECONDS = 600     # a live gap longer than the eligibility cutoff is an outage, not polling
LATENCY_MARKS = (60, 180, 300, 600)


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


def _normalise(record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("Each observation record must be an object")
    payload = record.get("payload") or {}
    if not isinstance(payload, dict):
        raise ValueError("Message payload must be an object")
    message_id = payload.get("message_id")
    if not str(message_id or "").isdigit() or not 1 <= int(message_id) <= 10**15 - 1:
        raise ValueError("A positive Telegram message_id is required")
    message_id = int(message_id)
    key = f"telegram:{CHANNEL}:{message_id}"
    if str(payload.get("channel", CHANNEL)).lower() != CHANNEL or record.get("source_id", key) != key:
        raise ValueError("Message is outside the Alastar channel allowlist")
    canonical_url = f"https://t.me/{CHANNEL}/{message_id}"
    if record.get("url", canonical_url) != canonical_url:
        raise ValueError("Message URL does not match its Alastar identity")
    text = payload.get("original_text", record.get("content"))
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Nonempty original message text is required")
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
    return {
        "message_key": key, "message_id": message_id, "url": canonical_url,
        "text": text, "text_sha256": _digest(text.encode("utf-8")),
        "revision_sha256": _digest(_json_bytes(version)),
        "published_at_raw": raw_publication, "published_at_utc": publication,
        "telegram_edited": edited, "text_truncated": truncated,
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
    if item["published_at_utc"] is None:
        reasons.append("invalid_or_timezone_missing_publication")
    else:
        delay = (observed - _utc(item["published_at_utc"])).total_seconds()
        if delay < 0:
            reasons.append("future_publication")
        elif delay > MAX_DELAY_SECONDS:
            reasons.append("first_observation_more_than_10_minutes_late")
    return reasons


def _apply_run(state: dict[str, Any], run: dict[str, Any]) -> None:
    for revision in run["new_revisions"]:
        checked = _normalise(revision["raw_message"])
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


def _load_runs(output: Path, *, verify_full: bool = False) -> tuple[int, dict[str, Any], str | None]:
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
                    and isinstance(candidate["messages"], dict)
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
        paths = None
    else:
        # Missing/corrupt projections are disposable. Verify every committed
        # run when reconstructing, including a missing sequence in the middle.
        paths = sorted((output / "runs").glob("*.json"))
    checked_evidence: set[str] = set()
    while True:
        if paths is not None:
            if sequence >= len(paths):
                break
            path = paths[sequence]
        else:
            path = output / "runs" / f"{sequence + 1:012d}.json"
            if not path.exists():
                break
        raw = path.read_bytes()
        run = json.loads(raw)
        if (path.name != f"{sequence + 1:012d}.json" or run.get("sequence") != sequence + 1
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
        _apply_run(state, run)
        sequence += 1
        previous_hash = _digest(raw)
    return sequence, state, previous_hash


def _projections(output: Path, run_count: int, messages: dict[str, Any], last_hash: str | None) -> dict[str, Any]:
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
    summary = {"schema_version": 1, "channel": CHANNEL, "run_count": run_count, "message_count": len(messages),
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


def verify_archive(output: Path | str = DEFAULT_OUTPUT) -> dict[str, Any]:
    """Fully verify all committed runs and rebuild projections, without a fetch."""
    output = Path(output).resolve()
    with _archive_lock(output):
        count, state, last_hash = _load_runs(output, verify_full=True)
        return {**_projections(output, count, state, last_hash), "verified_full_chain": True}


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


def receipt_latency(output: Path | str = DEFAULT_OUTPUT, *, now: datetime | str | None = None) -> dict[str, Any]:
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
        run_count, state, last_hash = _load_runs(output, verify_full=True)
        live_times = []
        for path in sorted((output / "runs").glob("*.json")):
            run = json.loads(path.read_bytes())
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
            "schema_version": 1, "channel": CHANNEL, "computed_at_utc": current.isoformat(),
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
    raw_preview: bytes | None = None,
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
    normalised = [_normalise(record) for record in records]
    output = Path(output).resolve()
    with _archive_lock(output):
        run_count, state, previous_hash = _load_runs(output)
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
        run = {"schema_version": 1, "sequence": sequence, "run_id": uuid.uuid4().hex,
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
        _apply_run(state, run)
        summary = _projections(output, sequence, state, _digest(raw_run))
        return {**summary, "run_file": path.relative_to(output).as_posix(),
                "observed_at_utc": observed.isoformat(), "mode": mode,
                "run_observation_count": len(observations), "new_revision_count": len(revisions)}


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


