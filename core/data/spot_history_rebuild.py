"""Reconstruct damaged spot history from checksummed Binance UTC source data.

This is an explicit replacement workflow, separate from incremental certification.
Unknown original rows remain in the repair engine's hash-verified backups; they
are never guessed into a clock or accepted as reference data.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
from pathlib import Path, PurePosixPath
import re
import threading
import time
from uuid import uuid4
import zipfile

import numpy as np
import pandas as pd
import requests

from core.data.kline_partitions import list_part_files, stem_period
from core.data.parquet_timezone_repair import OHLCV, RepairConflict, plan_explicit, sha256

ARCHIVE_ROOT = "https://data.binance.vision/data/spot"
API_URL = "https://api.binance.com/api/v3/klines"
STEPS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000,
         "4h": 14_400_000, "1d": 86_400_000}


class SourceError(RuntimeError):
    pass


class SourceRefused(SourceError):
    pass


def empty_frame() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=float) for c in OHLCV},
                        index=pd.DatetimeIndex([], name="timestamp"))


def validate_frame(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    result = frame[OHLCV].astype(float).copy()
    result.index = pd.DatetimeIndex(result.index).as_unit("ns")
    if result.index.tz is not None:
        result.index = result.index.tz_convert("UTC").tz_localize(None)
    result.index.name = "timestamp"
    if result.index.hasnans or not result.index.is_unique:
        raise SourceError("Invalid or duplicate official timestamps")
    if np.any((result.index.asi8 // 1_000_000) % STEPS[timeframe]):
        raise SourceError("Official timestamps are off the UTC interval grid")
    values = result.to_numpy()
    if not np.isfinite(values).all():
        raise SourceError("Non-finite official OHLCV")
    if len(result):
        o, h, l, c, v = values.T
        valid = ((np.minimum.reduce([o, h, l, c]) > 0) & (v >= 0)
                 & (h >= np.maximum.reduce([o, l, c])) & (l <= np.minimum.reduce([o, h, c])))
        if not valid.all():
            raise SourceError("Invalid official OHLCV; reference not accepted")
    return result.sort_index()


def parse_rows(rows: list, timeframe: str) -> pd.DataFrame:
    if not rows:
        return empty_frame()
    if any(not isinstance(r, (list, tuple)) or len(r) < 6 for r in rows):
        raise SourceError("Unexpected official candle schema")
    stamps = pd.to_numeric(pd.Series([r[0] for r in rows]), errors="raise").to_numpy(dtype="int64")
    # Binance spot archives switched from ms to us on 2025-01-01.
    nanos = np.where(stamps >= 100_000_000_000_000, stamps * 1000, stamps * 1_000_000)
    frame = pd.DataFrame([r[1:6] for r in rows], columns=OHLCV,
                         index=pd.DatetimeIndex(nanos, name="timestamp"))
    return validate_frame(frame, timeframe)


def parse_zip(payload: bytes, timeframe: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        if len(names) != 1 or not names[0].endswith(".csv"):
            raise SourceError("Unexpected official archive members")
        csv = pd.read_csv(io.BytesIO(archive.read(names[0])), header=None, usecols=range(6), dtype=str)
    if len(csv) and str(csv.iloc[0, 0]).lower() in {"open_time", "open time"}:
        csv = csv.iloc[1:]
    return parse_rows(csv.values.tolist(), timeframe)


def checked_sha(payload: bytes, checksum: str, filename: str) -> str:
    match = re.fullmatch(r"([a-fA-F0-9]{64})\s+\*?([^\r\n]+)\s*", checksum.strip())
    if not match or match[2] != filename:
        raise SourceError("Invalid official checksum filename or format")
    actual = hashlib.sha256(payload).hexdigest()
    if actual != match[1].lower():
        raise SourceError("Official archive SHA-256 mismatch")
    return actual


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        temp.write_bytes(data)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


class OfficialSpotSource:
    def __init__(self, cache: Path):
        self.cache = Path(cache).resolve()
        self.local = threading.local()
        self.stop = threading.Event()
        self.api_lock = threading.Lock()
        self.next_api = 0.0

    def _session(self, public_api: bool) -> requests.Session:
        key = "api" if public_api else "archive"
        value = getattr(self.local, key, None)
        if value is None:
            value = requests.Session()
            value.trust_env = public_api  # archives direct; API uses configured project proxy
            setattr(self.local, key, value)
        return value

    def _get(self, url: str, *, public_api: bool = False, params: dict | None = None):
        for attempt in range(3):
            if self.stop.is_set():
                raise SourceRefused("Source previously refused this batch")
            if public_api:
                with self.api_lock:
                    delay = max(0.0, self.next_api - time.monotonic())
                    self.next_api = max(self.next_api, time.monotonic()) + 0.5
                if delay:
                    time.sleep(delay)
            try:
                response = self._session(public_api).get(url, params=params, timeout=(8, 35))
                if response.status_code in {401, 403, 418, 429}:
                    self.stop.set()
                    raise SourceRefused(f"Official source HTTP {response.status_code}; no retry or exit change")
                if response.status_code == 404 and not public_api:
                    return response
                if response.status_code >= 500:
                    raise requests.ConnectionError(f"Official source HTTP {response.status_code}")
                response.raise_for_status()
                return response
            except SourceRefused:
                raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
                time.sleep(1.0 + attempt)
        raise SourceError("Unreachable request state")

    def archive(self, relative: str, timeframe: str) -> tuple[pd.DataFrame | None, dict]:
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts:
            raise SourceError("Invalid archive path")
        path = self.cache / "archives" / Path(*pure.parts)
        url = ARCHIVE_ROOT + "/" + relative
        checksum_path = path.with_name(path.name + ".CHECKSUM")
        if path.exists() and checksum_path.exists():
            payload, checksum = path.read_bytes(), checksum_path.read_text("utf-8")
        else:
            response = self._get(url)
            if response.status_code == 404:
                return None, {"url": url, "http": 404}
            payload = response.content
            check = self._get(url + ".CHECKSUM")
            if check.status_code != 200:
                raise SourceError("Official archive exists but its checksum is unavailable")
            checksum = check.text
            checked_sha(payload, checksum, pure.name)
            _atomic_bytes(path, payload)
            _atomic_bytes(checksum_path, checksum.encode("utf-8"))
        digest = checked_sha(payload, checksum, pure.name)
        frame = parse_zip(payload, timeframe)
        return frame, {"url": url, "http": 200, "sha256": digest,
                       "checksum": str(checksum_path), "cached_file": str(path), "rows": len(frame)}

    def api(self, symbol: str, timeframe: str, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, list]:
        step = STEPS[timeframe]
        cursor, stop = int(start.value // 1_000_000), int(end.value // 1_000_000)
        frames, evidence = [], []
        while cursor <= stop:
            params = {"symbol": symbol, "interval": timeframe, "startTime": cursor,
                      "endTime": stop, "timeZone": "0", "limit": 1000}
            response = self._get(API_URL, public_api=True, params=params)
            rows = response.json()
            if not isinstance(rows, list):
                raise SourceError("Invalid official API candle payload")
            frame = parse_rows(rows, timeframe)
            if len(frame) and (frame.index.min().value // 1_000_000 < cursor or frame.index.max().value // 1_000_000 > stop):
                raise SourceError("Official API candle outside requested range")
            raw = response.content
            sha = hashlib.sha256(raw).hexdigest()
            path = self.cache / "api" / (sha + ".json")
            _atomic_bytes(path, raw)
            evidence.append({"url": API_URL, "params": params, "sha256": sha,
                             "response": str(path), "received_at": datetime.now(timezone.utc).isoformat(), "rows": len(frame)})
            frames.append(frame)
            if len(rows) < 1000:
                break
            next_cursor = int(frame.index.max().value // 1_000_000) + step
            if next_cursor <= cursor:
                raise SourceError("Official API pagination did not advance")
            cursor = next_cursor
        return (pd.concat(frames) if frames else empty_frame()), evidence

    def reference(self, symbol: str, timeframe: str, start: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
        start = pd.Timestamp(start)
        if start.tz is not None:
            start = start.tz_convert("UTC").tz_localize(None)
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        step = STEPS[timeframe]
        current_open = pd.Timestamp((now.value // 1_000_000 // step) * step, unit="ms")
        closed_end = current_open - pd.Timedelta(milliseconds=step)
        first_params = {"symbol": symbol, "interval": timeframe, "startTime": 0,
                        "endTime": int(now.value // 1_000_000), "timeZone": "0", "limit": 1}
        first_response = self._get(API_URL, public_api=True, params=first_params)
        first = parse_rows(first_response.json(), timeframe)
        if first.empty:
            raise SourceError("No official first spot candle is available")
        raw = first_response.content
        first_sha = hashlib.sha256(raw).hexdigest()
        first_path = self.cache / "api" / (first_sha + ".json")
        _atomic_bytes(first_path, raw)
        first_proof = {"url": API_URL, "params": first_params, "response": str(first_path),
                       "sha256": first_sha, "first_available_utc": first.index.min().isoformat()}
        requested_start = start
        # The old spot store sometimes contains perpetual bars from before the
        # spot listing. The original REST first bar bounds real spot availability.
        start = max(start, first.index.min())
        current_month = now.normalize().replace(day=1)
        current_day = now.normalize()
        cursor = start.normalize().replace(day=1)
        frames, records = [], [first_proof]

        def day(day_start: pd.Timestamp) -> None:
            label = day_start.strftime("%Y-%m-%d")
            relative = f"daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{label}.zip"
            frame, item = self.archive(relative, timeframe)
            records.append(item)
            if frame is None:
                # A missing archive is never interpreted as zero activity: confirm
                # its complete bounded day against the original spot REST endpoint.
                frame, proof = self.api(symbol, timeframe, day_start, day_start + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1))
                records.extend(proof)
            frames.append(frame)

        while cursor < current_month:
            following = cursor + pd.offsets.MonthBegin(1)
            label = cursor.strftime("%Y-%m")
            frame, item = self.archive(f"monthly/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{label}.zip", timeframe)
            records.append(item)
            if frame is None:
                for d in pd.date_range(max(cursor, start.normalize()), following - pd.Timedelta(days=1), freq="D"):
                    day(d)
            else:
                frames.append(frame)
            cursor = following
        for d in pd.date_range(max(cursor, start.normalize()), current_day - pd.Timedelta(days=1), freq="D"):
            day(d)
        tail, proof = self.api(symbol, timeframe, max(start, current_day), current_open - pd.Timedelta(milliseconds=1))
        frames.append(tail)
        records.extend(proof)
        combined = pd.concat(frames) if frames else empty_frame()
        duplicates = combined.index[combined.index.duplicated(keep=False)].unique()
        for stamp in duplicates:
            values = combined.loc[[stamp], OHLCV].to_numpy()
            if not np.all(values == values[0]):
                raise SourceError("Conflicting official candle sources")
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined = validate_frame(combined[(combined.index >= start) & (combined.index <= closed_end)], timeframe)
        if combined.empty:
            raise SourceError("No authoritative spot history; refuse an empty replacement")
        return combined, {"symbol": symbol, "timeframe": timeframe, "source_namespace": "binance_spot",
                          "requested_start_utc": requested_start.isoformat(), "snapshot_at_utc": now.isoformat(),
                          "closed_only": True, "closed_through_utc": closed_end.isoformat(),
                          "snapshot_current_open_utc": current_open.isoformat(),
                          "first_utc": combined.index.min().isoformat(), "last_utc": combined.index.max().isoformat(),
                          "source_records": records, "reference_rows": len(combined)}


def series_files(storage: Path, symbol: str, timeframe: str) -> list[Path]:
    if not re.fullmatch(r"\w+/USDT", symbol) or timeframe not in STEPS:
        raise RepairConflict("Only explicit supported Binance spot USDT series may be rebuilt")
    directory = Path(storage).resolve() / "binance" / symbol.replace("/", "_")
    main = directory / f"{timeframe}.parquet"
    files = ([main] if main.exists() else []) + list_part_files(directory / f"{timeframe}_parts")
    if not files:
        raise RepairConflict("No damaged source files found")
    return files


def reconstruction_plans(files: list[Path], reference: pd.DataFrame, timeframe: str, evidence: dict) -> list[dict]:
    reference = validate_frame(reference, timeframe)
    plans = []
    for path in files:
        before = sha256(path)
        original = pd.read_parquet(path)
        if set(original.columns) != set(OHLCV):
            raise RepairConflict("Unexpected stored columns need a separate preservation plan")
        if not isinstance(original.index, pd.DatetimeIndex):
            raise RepairConflict("Unexpected stored index")
        if sha256(path) != before:
            raise RepairConflict("Source changed during plan read")
        result = reference.copy()
        if path.parent.name.endswith("_parts"):
            period = stem_period(path.stem)
            if period is None:
                raise RepairConflict("Unknown partition filename")
            lower, upper = pd.Timestamp(period[0]), pd.Timestamp(period[1]) + pd.Timedelta(days=1)
            result = result[(result.index >= lower) & (result.index < upper)].copy()
        # Do not roll back newer, concurrently received UTC data to an older
        # download snapshot. The caller must obtain a new source snapshot.
        original_last = pd.Timestamp(original.index.max()) if len(original) else None
        if original_last is not None and original_last.tz is not None:
            original_last = original_last.tz_convert("UTC").tz_localize(None)
        if (len(original) and original.attrs.get("cts_timestamp_timezone") == "UTC"
                and original_last > reference.index.max()):
            current_open = pd.Timestamp(evidence.get("snapshot_current_open_utc")) if evidence.get("closed_only") else None
            # Explicit closed-only reconstruction may archive the currently
            # forming candle. It must never remove a newer closed candle or a
            # candle that first appeared after the source snapshot's interval.
            original_index = pd.DatetimeIndex(original.index)
            if original_index.tz is not None:
                original_index = original_index.tz_convert("UTC").tz_localize(None)
            extra = original_index[original_index > reference.index.max()]
            if (current_open is None or not len(extra) or not (extra == current_open).all()
                    or reference.index.max() >= current_open):
                raise RepairConflict("Newer verified bars appeared after the official snapshot")
        if (original.attrs.get("cts_timestamp_timezone") == "UTC"
                and original.attrs.get("cts_timestamp_schema") == 1
                and original[OHLCV].equals(result)):
            continue
        result.attrs = {"cts_timestamp_origin": "binance_official_rebuild"}
        proof = {"method": "checksummed_official_spot_reconstruction", "source": evidence,
                 "original_sha256": before, "original_rows": len(original),
                 "original_row_policy": "Preserved in transaction backup; active values reconstructed only from official UTC source",
                 "result_rows": len(result)}
        plans.append(plan_explicit(path, result, evidence=proof, expected_hash=before,
                                   rows_before=len(original), utc_certified=True))
    return plans
