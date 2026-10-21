"""Evidence-driven, hash-guarded UTC parquet repair transactions.

Every row must match an independently obtained UTC OHLCV reference at exactly
one candidate clock. Unknown rows block the file without being dropped.
"""
from __future__ import annotations

import json
import os
import shutil
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from core.data.parquet_lock import parquet_partition_lock

OHLCV = ["open", "high", "low", "close", "volume"]
UTC_ATTRS = {"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1,
             "cts_timestamp_origin": "verified_timezone_repair", "cts_timestamp_status": "verified_utc"}


class RepairConflict(ValueError):
    """Evidence is insufficient, inconsistent, or would lose rows."""


class StalePlan(RepairConflict):
    """Source changed after planning; overwrite is forbidden."""


def sha256(path: str | Path) -> str:
    import hashlib
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _frame_hash(frame: pd.DataFrame) -> str:
    import hashlib
    digest = hashlib.sha256(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    digest.update(json.dumps({"columns": list(frame.columns), "dtypes": [str(x) for x in frame.dtypes],
                              "attrs": frame.attrs, "index_name": frame.index.name},
                             sort_keys=True, default=str).encode())
    return digest.hexdigest()


def _index(frame: pd.DataFrame) -> pd.DatetimeIndex:
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise RepairConflict("DatetimeIndex required; no implicit date coercion")
    idx = frame.index
    if idx.hasnans:
        raise RepairConflict("NaT present; preserve source for review")
    return idx.tz_convert("UTC").tz_localize(None) if idx.tz is not None else idx


def _validate(frame: pd.DataFrame, *, unique: bool = False) -> pd.DataFrame:
    if not set(OHLCV).issubset(frame.columns):
        raise RepairConflict("Source and reference require all five OHLCV columns")
    result = frame.copy()
    result.index = _index(frame)
    if unique and not result.index.is_unique:
        raise RepairConflict("Duplicate reference/output timestamp")
    values = result[OHLCV].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise RepairConflict("Nonfinite OHLCV; no silent removal")
    if (values[:, 4] < 0).any():
        raise RepairConflict("Negative volume")
    return result


def verify_candidates(frame: pd.DataFrame, reference: pd.DataFrame) -> dict:
    source = _validate(frame)
    ref = _validate(reference, unique=True).sort_index()
    values = source[OHLCV].to_numpy(dtype=float)
    matches = []
    for idx in (source.index, source.index - pd.Timedelta(hours=8)):
        found = ref.reindex(idx)[OHLCV].to_numpy(dtype=float)
        close = np.isclose(values, found, rtol=1e-8, atol=1e-10)
        close[:, 4] = np.isclose(values[:, 4], found[:, 4], rtol=1e-7, atol=1e-10)
        matches.append(close.all(axis=1))
    rows = []
    counts: Counter = Counter()
    marked_utc = (frame.attrs.get("cts_timestamp_timezone") == "UTC"
                  and frame.attrs.get("cts_timestamp_schema") == 1)
    for i, ts in enumerate(source.index):
        options = [n for n in (0, 1) if matches[n][i]]
        if marked_utc and 0 in options:
            options = [0]  # Existing verified provenance resolves periodic-price ambiguity.
        status = "unique" if len(options) == 1 else "ambiguous" if options else "unmatched"
        shift = -8 * options[0] if len(options) == 1 else None
        counts[status] += 1
        if shift is not None:
            counts["utc" if shift == 0 else "cst"] += 1
        rows.append({"position": i, "stored": ts.isoformat(), "utc":
                     (ts + pd.Timedelta(hours=shift)).isoformat() if shift is not None else None,
                     "shift_hours": shift, "status": status})
    classification = ("UNKNOWN" if counts["unmatched"] or counts["ambiguous"] or not len(source)
                      else "MIXED" if counts["utc"] and counts["cst"]
                      else "CST" if counts["cst"] else "UTC")
    return {"classification": classification, "rows": rows, "counts": dict(counts),
            "method": "all_rows_exact_two_clock_OHLCV", "rtol_price": 1e-8,
            "rtol_volume": 1e-7, "atol": 1e-10}


def plan_repair(path: str | Path, reference: pd.DataFrame) -> dict:
    path = Path(path).resolve()
    before = sha256(path)
    frame = pd.read_parquet(path)
    if sha256(path) != before:
        raise StalePlan(str(path))
    evidence = verify_candidates(frame, reference)
    plan = {"path": str(path), "pre_hash": before, "evidence": evidence,
            "rows_before": len(frame), "status": "unresolved"}
    if evidence["classification"] == "UNKNOWN":
        return plan
    if frame.attrs.get("cts_timestamp_timezone") == "UTC":
        if evidence["classification"] != "UTC":
            raise RepairConflict("UTC marker contradicts reference; refuse second migration")
        plan["status"] = "already_utc"
        return plan
    result = _validate(frame)
    result.index = pd.DatetimeIndex([r["utc"] for r in evidence["rows"]], name="timestamp")
    if path.parent.name.endswith("_parts") and any(result.index.strftime("%Y-%m-%d") != path.stem):
        plan["reason"] = "cross_day_requires_group_rebucketing"
        return plan
    coalesced = []
    if not result.index.is_unique:
        for ts, positions in pd.Series(range(len(result)), index=result.index).groupby(level=0):
            pos = positions.tolist()
            if len(pos) > 1:
                group = result.iloc[pos]
                if not all(group.iloc[n].equals(group.iloc[0]) for n in range(1, len(group))):
                    raise RepairConflict(f"Conflicting rows map to {ts}; official resolution required")
                coalesced.append({"utc": ts.isoformat(), "positions": pos, "kept": pos[0]})
        result = result.loc[~result.index.duplicated(keep="first")]
    result = result.sort_index()
    result.attrs.update(UTC_ATTRS)
    plan.update(status="ready", result=result, result_hash=_frame_hash(result),
                rows_after=len(result), coalesced=coalesced)
    return plan


def plan_explicit(path: str | Path, result: pd.DataFrame, *, evidence: dict,
                  expected_hash: str | None, rows_before: int,
                  coalesced: list | None = None, utc_certified: bool = True) -> dict:
    """Plan reviewed rebucketing/official resolution; None means absent target."""
    output = _validate(result, unique=True).sort_index()
    output.index.name = "timestamp"
    if utc_certified:
        output.attrs.update(UTC_ATTRS)
    else:
        for key in UTC_ATTRS:
            output.attrs.pop(key, None)
        output.attrs.update(cts_timestamp_status="unverified_segments_remain",
                            cts_timestamp_origin="verified_partial_timezone_repair")
    if not evidence.get("method"):
        raise RepairConflict("Explicit repair requires recorded evidence method")
    path = Path(path).resolve()
    if path.parent.name.endswith("_parts") and any(output.index.strftime("%Y-%m-%d") != path.stem):
        raise RepairConflict("Output rows do not belong to UTC partition filename")
    return {"path": str(path), "pre_hash": expected_hash,
            "status": "ready", "result": output, "result_hash": _frame_hash(output), "evidence": evidence,
            "utc_certified": utc_certified,
            "rows_before": rows_before, "rows_after": len(output),
            "coalesced": coalesced or []}


def _json(path: Path, value, *, exclusive: bool = False) -> None:
    target = path if exclusive else path.with_suffix(path.suffix + ".tmp")
    with target.open("x" if exclusive else "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)
        stream.flush()
        os.fsync(stream.fileno())
    if not exclusive:
        os.replace(target, path)


def _copy_exclusive(source: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, dest.open("xb") as out:
        shutil.copyfileobj(src, out, length=1024 * 1024)
        out.flush()
        os.fsync(out.fileno())


def apply_plan(plan: dict | list[dict], run_dir: str | Path) -> dict:
    from copy import deepcopy
    plans = deepcopy([plan] if isinstance(plan, dict) else list(plan))
    if not plans or any(p["status"] != "ready" for p in plans):
        raise RepairConflict("Only nonempty, fully proven ready plans may be applied")
    if any(_frame_hash(p["result"]) != p["result_hash"] for p in plans):
        raise RepairConflict("Planned output changed after evidence verification")
    paths = [Path(p["path"]).resolve() for p in plans]
    if len(set(paths)) != len(paths):
        raise RepairConflict("Duplicate target in transaction")
    run = Path(run_dir).resolve()
    if any(run == p or run in p.parents for p in paths):
        raise RepairConflict("Backups must not contain repair targets")
    run.mkdir(parents=True, exist_ok=False)
    journal = {"schema": 1, "created_at": datetime.now(timezone.utc).isoformat(),
               "status": "preparing", "files": []}
    _json(run / "manifest.json", journal, exclusive=True)
    with ExitStack() as stack:
        for path in sorted(paths, key=lambda p: str(p).lower()):
            stack.enter_context(parquet_partition_lock(path))
        for p, path in zip(plans, paths):
            current = sha256(path) if path.exists() else None
            if current != p["pre_hash"]:
                journal["status"] = "stale_no_mutation"
                _json(run / "manifest.json", journal)
                raise StalePlan(str(path))
        try:
            for i, (p, path) in enumerate(zip(plans, paths)):
                backup = run / "backups" / f"{i:04d}_{path.name}"
                if p["pre_hash"] is not None:
                    _copy_exclusive(path, backup)
                    if sha256(backup) != p["pre_hash"]:
                        raise RepairConflict("Backup hash mismatch")
                output = _validate(p["result"], unique=True)
                if p.get("utc_certified", True):
                    output.attrs.update(UTC_ATTRS)
                staged = path.with_name(f".{path.name}.{run.name}.{i}.repair_tmp")
                if staged.exists():
                    raise RepairConflict(f"Refuse to overwrite existing stage {staged}")
                path.parent.mkdir(parents=True, exist_ok=True)
                with staged.open("xb") as stream:
                    output.to_parquet(stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                check = pd.read_parquet(staged)
                pd.testing.assert_frame_equal(output, check, check_freq=False)
                if check.attrs != output.attrs:
                    raise RepairConflict("UTC attrs did not survive serialization")
                item = {k: v for k, v in p.items() if k not in ("result", "evidence")}
                _json(run / f"evidence_{i:04d}.json", p["evidence"], exclusive=True)
                item.update(backup=str(backup) if p["pre_hash"] is not None else None,
                            stage=str(staged), post_hash=sha256(staged), applied=False,
                            evidence=f"evidence_{i:04d}.json")
                journal["files"].append(item)
                _json(run / "manifest.json", journal)
            journal["status"] = "prepared"
            _json(run / "manifest.json", journal)
            for item in journal["files"]:
                os.replace(item["stage"], item["path"])
                if sha256(item["path"]) != item["post_hash"]:
                    raise RepairConflict("Post-replace hash mismatch")
                item["applied"] = True
                _json(run / "manifest.json", journal)
            journal["status"] = "complete"
            _json(run / "manifest.json", journal)
        except BaseException as exc:
            journal["status"] = "failed_requires_recovery"
            journal["error"] = f"{type(exc).__name__}: {exc}"
            _json(run / "manifest.json", journal)
            raise
    return journal


def rollback_run(run_dir: str | Path) -> dict:
    run = Path(run_dir).resolve()
    journal = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    if journal["status"] == "rolled_back":
        return journal
    with ExitStack() as stack:
        for item in sorted(journal["files"], key=lambda p: p["path"].lower()):
            stack.enter_context(parquet_partition_lock(Path(item["path"])))
        restore = []
        for item in journal["files"]:
            path = Path(item["path"])
            current = sha256(path) if path.exists() else None
            if current == item["pre_hash"]:
                continue
            if current != item["post_hash"]:
                raise StalePlan(f"Rollback would overwrite later changes: {path}")
            if item["backup"] and sha256(item["backup"]) != item["pre_hash"]:
                raise RepairConflict("Backup changed; rollback refused")
            restore.append(item)
        for i, item in enumerate(restore):
            path = Path(item["path"])
            if item["backup"] is None:
                path.unlink()
            else:
                from uuid import uuid4
                temp = path.with_name(f".{path.name}.{run.name}.{uuid4().hex}.rollback_tmp")
                _copy_exclusive(Path(item["backup"]), temp)
                if sha256(temp) != item["pre_hash"]:
                    raise RepairConflict("Rollback stage hash mismatch")
                os.replace(temp, path)
            item["rolled_back"] = True
        journal["status"] = "rolled_back"
        _json(run / "manifest.json", journal)
    return journal
