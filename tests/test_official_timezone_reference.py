from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import audit_official_timezone_reference as audit


def frame(times, values):
    return pd.DataFrame([([value] * 5) for value in values], columns=audit.FIELDS,
                        index=pd.to_datetime(times, utc=True))


def test_exact_row_audit_keeps_mixed_clocks_conflicts_and_ambiguous_rows_separate():
    reference = frame(["2024-01-01T00:00Z", "2024-01-01T08:00Z", "2024-01-01T16:00Z"], [1, 2, 3])
    source = frame(["2024-01-01T00:00Z", "2024-01-01T08:00Z", "2024-01-01T16:00Z", "2024-01-02T00:00Z"], [1, 1, 3, 99])
    out = audit.compare_source(source, reference)
    assert out["counts"] == {"only_t": 2, "only_t_minus_8h": 1, "both": 0, "conflict": 1,
                             "uncovered": 0, "invalid_timestamp": 0}
    assert out["classification_segments"][1]["classification"] == "only_t_minus_8h"
    # A flat fingerprint that matches both clocks cannot justify a shift.
    reference.iloc[1] = reference.iloc[0]
    ambiguous = audit.compare_source(source.iloc[[1]], reference)
    assert ambiguous["counts"]["both"] == 1


def test_rate_or_access_block_stops_globally_without_retry_or_host_failover(monkeypatch):
    calls = []

    class Session:
        def get(self, *args, **kwargs):
            calls.append(args[0])
            return type("Response", (), {"status_code": 429})()

    monkeypatch.setattr(audit, "session", lambda: Session())
    limiter = audit.Limiter(3)
    with pytest.raises(audit.StopRequested):
        audit.fetch_page({"symbol": "BTCUSDT"}, limiter)
    with pytest.raises(audit.StopRequested):
        audit.fetch_page({"symbol": "ETHUSDT"}, limiter)
    assert limiter.stop.is_set() and len(calls) == 1


def test_full_fetch_preserves_sources_and_reuses_hashed_raw_pages(monkeypatch, tmp_path):
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "OUT", tmp_path / "audit")
    source_path = tmp_path / "source.parquet"
    source = frame(["2024-01-01T08:00Z", "2024-01-02T08:00Z"], [11, 12])
    source.to_parquet(source_path)
    original_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    calls = []

    def fetch(params, limiter, **kwargs):
        calls.append(params)
        stamps = pd.to_datetime(["2024-01-01T00:00Z", "2024-01-02T00:00Z"])
        rows = [[int(t.value // 1_000_000), *([str(v)] * 5)] for t, v in zip(stamps, [11, 12])]
        raw = json.dumps(rows).encode()
        return raw, {"url": "https://api.binance.com/api/v3/klines", "params": params,
                     "raw_sha256": hashlib.sha256(raw).hexdigest(), "rows": len(rows)}

    monkeypatch.setattr(audit, "fetch_page", fetch)
    target = {"path": "source.parquet", "symbol": "BTC_USDT", "kind": "1d"}
    result = audit.audit_target(target, audit.Limiter(3))
    assert result["counts"]["only_t_minus_8h"] == 2 and len(calls) == 1
    assert calls[0]["timeZone"] == "0" and calls[0]["limit"] == 1000
    result = audit.audit_target(target, audit.Limiter(3))
    assert result["requests_used_this_run"] == 0 and len(calls) == 1
    assert hashlib.sha256(source_path.read_bytes()).hexdigest() == original_hash
    official = pd.read_parquet(audit.OUT / "BTCUSDT/1d.parquet")
    assert str(official.index.tz) == "UTC"
    assert official.attrs["source_market"] == "spot"


def test_cached_response_hash_mismatch_fails_instead_of_silently_trusting_cache(monkeypatch, tmp_path):
    import gzip

    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "OUT", tmp_path / "audit")
    source_path = tmp_path / "source.parquet"
    source = frame(["2024-01-01T08:00Z"], [11])
    source.to_parquet(source_path)
    raw_dir = audit.OUT / "BTCUSDT/1d_raw_pages"
    raw_dir.mkdir(parents=True)
    low = int(pd.Timestamp("2024-01-01T00:00Z").value // 1_000_000)
    high = int(pd.Timestamp("2024-01-01T08:00Z").value // 1_000_000)
    raw_path = raw_dir / f"{low}_{high}.json.gz"
    raw_path.write_bytes(gzip.compress(b"[]"))
    params = {"symbol": "BTCUSDT", "interval": "1d", "startTime": low, "endTime": high, "timeZone": "0", "limit": 1000}
    raw_path.with_suffix(".meta.json").write_text(json.dumps({"params": params, "raw_sha256": "bad"}), encoding="utf-8")
    with pytest.raises(ValueError, match="provenance mismatch"):
        audit.audit_target({"path": "source.parquet", "symbol": "BTC_USDT", "kind": "1d"}, audit.Limiter(3))
