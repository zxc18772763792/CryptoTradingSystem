"""Download official UTC spot references and audit clocks without editing sources.

Writes only data/research/timezone_repair_20261020/official_reference/. Public
Binance klines only, timeZone=0, a global request limiter and at most 3 workers.
Any HTTP 403/418/429 stops the entire run without further retries or failover.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
AUDIT = ROOT / "data/research/timezone_repair_20261020"
OUT = AUDIT / "official_reference"
INTERVAL_MS = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
FIELDS = ["open", "high", "low", "close", "volume"]
HOSTS = ("https://api.binance.com", "https://data-api.binance.vision", "https://api1.binance.com")
LOCAL = threading.local()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".{os.getpid()}.{threading.get_ident()}.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(temp, path)


class StopRequested(RuntimeError):
    pass


class PublicRequestError(RuntimeError):
    pass


class Limiter:
    def __init__(self, rate: float):
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.interval = 1.0 / min(3.0, max(0.1, rate))
        self.stop = threading.Event()
        self.stop_reason = None

    def halt(self, reason: str) -> None:
        with self.lock:
            self.stop_reason = self.stop_reason or reason
            self.stop.set()

    def wait(self) -> None:
        with self.lock:
            if self.stop.is_set():
                raise StopRequested(self.stop_reason or "stopped")
            current = time.monotonic()
            delay = max(0.0, self.next_at - current)
            self.next_at = max(current, self.next_at) + self.interval
        if self.stop.wait(delay):
            raise StopRequested(self.stop_reason or "stopped")


class SystemTrustAdapter(HTTPAdapter):
    """Use system trust as well as requests' CA verification; never disable TLS."""

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        kwargs["ssl_context"] = ssl.create_default_context()
        return super().init_poolmanager(connections, maxsize, block=block, **kwargs)


def session() -> requests.Session:
    current = getattr(LOCAL, "session", None)
    if current is None:
        from config.settings import settings

        current = requests.Session()
        current.trust_env = False
        http = settings.HTTP_PROXY or settings.HTTPS_PROXY
        https = settings.HTTPS_PROXY or settings.HTTP_PROXY
        current.proxies = {key: value for key, value in (("http", http), ("https", https)) if value}
        current.mount("https://", SystemTrustAdapter(max_retries=0))
        current.headers["User-Agent"] = "CryptoResearch-UTC-Provenance-Audit/1.0"
        LOCAL.session = current
    return current


def fetch_page(params: dict, limiter: Limiter, *, timeout: float = 15) -> tuple[bytes, dict]:
    """No exception text containing proxy configuration is exposed or persisted."""
    errors = []
    for host in HOSTS:
        limiter.wait()
        url = host + "/api/v3/klines"
        requested = now_iso()
        try:
            response = session().get(url, params=params, timeout=(6, timeout))
        except requests.RequestException as exc:
            errors.append({"url": url, "error_type": type(exc).__name__})
            continue
        received = now_iso()
        if response.status_code in {403, 418, 429}:
            limiter.halt(f"HTTP {response.status_code} from {host}")
            raise StopRequested(limiter.stop_reason)
        if response.status_code >= 500:
            errors.append({"url": url, "http_status": response.status_code})
            continue
        if response.status_code != 200:
            raise PublicRequestError(f"HTTP {response.status_code} for public klines")
        raw = response.content
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise PublicRequestError("Public klines response is not a list")
        return raw, {"url": url, "params": params, "requested_at": requested, "received_at": received,
                     "http_status": 200, "raw_sha256": hashlib.sha256(raw).hexdigest(), "rows": len(payload)}
    raise PublicRequestError("All official hosts unavailable: " + json.dumps(errors))


def targets(symbols: set[str] | None = None) -> list[dict]:
    evidence = json.loads((AUDIT / "source_audit.json").read_text(encoding="utf-8"))
    inventory = json.loads((AUDIT / "inventory.json").read_text(encoding="utf-8"))["files"]
    by_key = {(x["symbol"], x["kind"]): x for x in inventory if x["exchange"] == "binance" and not x["partition"]}
    selected = {(x["symbol"], x["kind"]): dict(x) for x in evidence["cohort_monolith_candidates"] if x["kind"] in INTERVAL_MS}
    # Some legacy 1h monoliths have no mixed daily companion. Include the
    # actual unnamed-index inventory cohort rather than assuming its members.
    for row in inventory:
        if row["exchange"] == "binance" and not row["partition"] and row["kind"] == "1h" and row["index"] == "__index_level_0__":
            selected.setdefault((row["symbol"], "1h"), dict(row))
    for part in evidence["mispartitioned_files"]:
        if part["exchange"] != "binance" or part["kind"] != "1h":
            continue
        key = (part["symbol"], "1h")
        if key not in selected:
            selected[key] = dict(by_key[key])
        row = selected[key]
        row.setdefault("additional_source_paths", []).append(part["path"])
        row["reference_extra_start"] = "2026-05-18T00:00:00Z"
        row["reference_extra_end"] = "2026-05-20T00:00:00Z"
    rows = list(selected.values())
    if symbols:
        rows = [x for x in rows if x["symbol"].replace("_", "") in symbols]
    # Small BTC/ETH daily references are available first, followed by 4h and 1h.
    return sorted(rows, key=lambda r: (0 if r["symbol"] in {"BTC_USDT", "ETH_USDT"} else 1,
                                      {"1d": 0, "4h": 1, "1h": 2}[r["kind"]], r["symbol"]))


def decode_rows(rows: list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FIELDS, index=pd.DatetimeIndex([], tz="UTC", name="timestamp"))
    values = {pd.Timestamp(int(row[0]), unit="ms", tz="UTC"): [float(row[i]) for i in range(1, 6)] for row in rows}
    return pd.DataFrame.from_dict(values, orient="index", columns=FIELDS).rename_axis("timestamp").sort_index()


def compare_source(source: pd.DataFrame, reference: pd.DataFrame) -> dict:
    """Exact OHLCV evidence for each row's t and t-8h, never a lag search."""
    timestamps = pd.to_datetime(source.index, errors="coerce", utc=True)
    valid_time = ~timestamps.isna()
    own = source[FIELDS].to_numpy(dtype=float)
    present0 = timestamps.isin(reference.index)
    shifted_stamps = timestamps - pd.Timedelta(hours=8)
    present8 = shifted_stamps.isin(reference.index)
    matches0 = present0 & np.all(own == reference.reindex(timestamps)[FIELDS].to_numpy(dtype=float), axis=1)
    matches8 = present8 & np.all(own == reference.reindex(shifted_stamps)[FIELDS].to_numpy(dtype=float), axis=1)
    categories, details = [], []
    counts = {k: 0 for k in ("only_t", "only_t_minus_8h", "both", "conflict", "uncovered", "invalid_timestamp")}
    for index, stamp in enumerate(timestamps):
        if not valid_time[index]:
            category, match0, match8 = "invalid_timestamp", False, False
            at0 = at8 = False
        else:
            at0, at8 = present0[index], present8[index]
            match0, match8 = matches0[index], matches8[index]
            category = ("both" if match0 and match8 else "only_t" if match0 else "only_t_minus_8h" if match8
                        else "conflict" if at0 or at8 else "uncovered")
        counts[category] += 1
        categories.append(category)
        if category not in {"only_t", "only_t_minus_8h"} and len(details) < 100:
            details.append({"row": index, "stored_timestamp": str(stamp), "classification": category,
                            "reference_at_t": bool(at0), "reference_at_t_minus_8h": bool(at8)})
    segments = []
    for i, category in enumerate(categories):
        if not segments or segments[-1]["classification"] != category:
            segments.append({"classification": category, "start_row": i, "end_row": i,
                             "stored_start": str(timestamps[i]), "stored_end": str(timestamps[i]), "rows": 1})
        else:
            segments[-1].update(end_row=i, stored_end=str(timestamps[i]), rows=segments[-1]["rows"] + 1)
    return {"rows": len(source), "counts": counts, "classification_segments": segments,
            "nonunique_or_unmatched_examples": details,
            "source_duplicate_index_rows": int(timestamps.duplicated().sum()),
            "method": "Exact numeric OHLCV at stored t versus t-8h against official native interval, timeZone=0.",
            "warning": "Clock evidence only. Conflict resolution, economic duplicate merging and repair are separate; no source rows were changed."}


def audit_target(target: dict, limiter: Limiter, *, timeout: float = 15) -> dict:
    source_path = ROOT / target["path"]
    source_blob = source_path.read_bytes()
    source_hash = hashlib.sha256(source_blob).hexdigest()
    source = pd.read_parquet(io.BytesIO(source_blob))
    stamps = pd.to_datetime(source.index, errors="coerce", utc=True)
    valid = stamps[~stamps.isna()]
    if not len(valid):
        raise ValueError("No source timestamps")
    symbol, interval = target["symbol"].replace("_", ""), target["kind"]
    directory = OUT / symbol
    raw_dir = directory / f"{interval}_raw_pages"
    raw_dir.mkdir(parents=True, exist_ok=True)
    low = int((valid.min() - pd.Timedelta(hours=8)).value // 1_000_000)
    requested_high = int(valid.max().value // 1_000_000)
    if target.get("reference_extra_start"):
        low = min(low, int(pd.Timestamp(target["reference_extra_start"]).value // 1_000_000))
    if target.get("reference_extra_end"):
        requested_high = max(requested_high, int(pd.Timestamp(target["reference_extra_end"]).value // 1_000_000))
    step = INTERVAL_MS[interval]
    current_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    high = min(requested_high, current_ms // step * step - step)  # completed candles only
    cursor, pages, all_rows, requests_used = low, [], {}, 0
    maximum_pages = (max(0, high - low) // step + 1000) // 1000 + 2
    for page_index in range(maximum_pages):
        if cursor > high:
            break
        if limiter.stop.is_set():
            raise StopRequested(limiter.stop_reason or "stopped")
        params = {"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": high,
                  "timeZone": "0", "limit": 1000}
        page_path = raw_dir / f"{cursor}_{high}.json.gz"
        info_path = page_path.with_suffix(".meta.json")
        if page_path.exists() and info_path.exists():
            raw = gzip.decompress(page_path.read_bytes())
            info = json.loads(info_path.read_text(encoding="utf-8"))
            if info.get("params") != params or hashlib.sha256(raw).hexdigest() != info.get("raw_sha256"):
                raise ValueError("Cached raw response provenance mismatch")
            info["reused_cache"] = True
        else:
            raw, info = fetch_page(params, limiter, timeout=timeout)
            requests_used += 1
            temp = page_path.with_name(page_path.name + ".tmp")
            temp.write_bytes(gzip.compress(raw, mtime=0))
            os.replace(temp, page_path)
            atomic_json(info_path, info)
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise ValueError("Invalid cached response")
        info["raw_path"] = str(page_path.relative_to(ROOT))
        pages.append(info)
        if not payload:
            break
        opens = [int(row[0]) for row in payload]
        if opens != sorted(opens) or len(set(opens)) != len(opens) or min(opens) < cursor or max(opens) > high:
            raise ValueError("Official page has invalid open-time ordering/range")
        for row in payload:
            all_rows[int(row[0])] = row
        if max(opens) < cursor:
            raise ValueError("Official pagination made no progress")
        cursor = max(opens) + step
        if len(payload) < 1000:
            break
    else:
        raise ValueError("Bounded page budget exhausted before completion")
    reference = decode_rows([all_rows[k] for k in sorted(all_rows)])
    reference.attrs = {"cts_timestamp_timezone": "UTC", "cts_timestamp_origin": "official_spot_api_timezone_0",
                       "source_market": "spot", "source_symbol": symbol, "source_interval": interval}
    parquet = directory / f"{interval}.parquet"
    temp = parquet.with_suffix(".parquet.tmp")
    reference.to_parquet(temp)
    os.replace(temp, parquet)
    meta = {"completed_at": now_iso(), "source_path": str(source_path.relative_to(ROOT)), "source_sha256": source_hash,
            "market": "spot", "symbol": symbol, "interval": interval, "timeZone": "0", "complete_fetch": True,
            "requested_start_ms": low, "requested_end_ms": requested_high, "closed_bar_end_ms": high,
            "rows": len(reference), "start": str(reference.index.min()), "end": str(reference.index.max()),
            "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(), "requests_used_this_run": requests_used,
            "pages": pages}
    atomic_json(directory / f"{interval}.meta.json", meta)
    result = compare_source(source, reference)
    result.update(source_path=target["path"], source_sha256=source_hash,
                  reference_path=str(parquet.relative_to(ROOT)), reference_sha256=meta["parquet_sha256"],
                  requested_start_ms=low, requested_end_ms=requested_high,
                  reference_rows=len(reference), requests_used_this_run=requests_used, audited_at=now_iso())
    atomic_json(directory / f"{interval}.validation.json", result)
    part_results = []
    for relative in target.get("additional_source_paths", []):
        blob = (ROOT / relative).read_bytes()
        part_result = compare_source(pd.read_parquet(io.BytesIO(blob)), reference)
        part_result.update(source_path=relative, source_sha256=hashlib.sha256(blob).hexdigest(),
                           reference_path=str(parquet.relative_to(ROOT)), reference_sha256=meta["parquet_sha256"])
        part_results.append(part_result)
    if part_results:
        atomic_json(directory / f"{interval}.parts.validation.json", part_results)
    return {"symbol": symbol, "interval": interval, "status": "complete", "source_rows": len(source),
            "reference_rows": len(reference), "counts": result["counts"], "requests_used_this_run": requests_used,
            "partition_validations": [{"source_path": r["source_path"], "counts": r["counts"]} for r in part_results]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--symbols", help="Comma-separated public spot symbols; default: all mixed-daily cohort")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--rate", type=float, default=3.0)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--intervals", default="1d,4h,1h")
    args = parser.parse_args()
    requested = {x.strip().upper().replace("_", "") for x in args.symbols.split(",")} if args.symbols else None
    chosen_intervals = set(args.intervals.split(","))
    jobs = [r for r in targets(requested) if r["kind"] in chosen_intervals]
    limiter = Limiter(args.rate)
    OUT.mkdir(parents=True, exist_ok=True)
    started, results = now_iso(), []
    print(json.dumps({"started_at": started, "jobs": len(jobs), "workers": min(3, max(1, args.workers)),
                      "requests_per_second": min(3.0, args.rate), "source_mutation": False}), flush=True)
    with ThreadPoolExecutor(max_workers=min(3, max(1, args.workers))) as pool:
        future_map = {pool.submit(audit_target, row, limiter, timeout=args.timeout): row for row in jobs}
        for future in as_completed(future_map):
            target = future_map[future]
            try:
                result = future.result()
            except StopRequested:
                result = {"symbol": target["symbol"], "interval": target["kind"], "status": "stopped",
                          "reason": limiter.stop_reason or "global stop"}
            except Exception as exc:
                # Errors generated by requests can contain proxy addresses, so
                # only controlled messages from this module are kept.
                result = {"symbol": target["symbol"], "interval": target["kind"], "status": "failed",
                          "error_type": type(exc).__name__,
                          "reason": str(exc) if isinstance(exc, PublicRequestError) else "series audit unavailable"}
            results.append(result)
            print(json.dumps(result, ensure_ascii=True), flush=True)
            atomic_json(OUT / "run_status.json", {"started_at": started, "updated_at": now_iso(), "planned_jobs": len(jobs),
                        "finished_jobs": len(results), "global_stop": limiter.stop_reason, "results": results})
            if limiter.stop.is_set():
                for task in future_map:
                    task.cancel()
    return 1 if limiter.stop.is_set() or any(r["status"] != "complete" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
