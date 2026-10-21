"""Manual, bounded v2 paper recording. No automatic loop or real orders."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from core.news.alastar_observation import DEFAULT_OUTPUT, _atomic_bytes, _json_bytes, _utc
from core.research.alastar_forward_paper import (
    ENDPOINT, FUNDING_ENDPOINT, IMPLEMENTATION_RULES, SYMBOL, PaperJournal, pre_entry_null, sha,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAPER = ROOT / "data/research/alastar_paper_v2"
PROTOCOL = ROOT / "data/research/alastar_response_20261021/prospective_paper_protocol_v2.json"


class OfficialEvidence:
    def __init__(self, output):
        import requests
        from config.settings import settings
        from core.utils.proxy_env import ensure_proxy_env
        ensure_proxy_env(settings)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "alastar-local-paper-research/2", "Cache-Control": "no-cache"})
        self.output, self.calls = Path(output), 0
        self.failed = False

    def _fetch(self, endpoint, params):
        if endpoint not in (ENDPOINT, FUNDING_ENDPOINT, "https://fapi.binance.com/fapi/v1/time"):
            raise ValueError("Unsupported public endpoint")
        if self.failed or self.calls >= 80:
            raise RuntimeError("Public data pass stopped or request bound reached")
        self.calls += 1
        requested = datetime.now(timezone.utc)
        try:
            response = self.session.get(endpoint, params=params, timeout=(5, 15))
            response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024:
                raise ValueError("Public response exceeds evidence bound")
            rows = response.json()
        except Exception as exc:
            self.failed = True
            raise RuntimeError(f"Official public data unavailable ({type(exc).__name__})") from None
        received = datetime.now(timezone.utc)
        record = {"endpoint": endpoint, "params": params, "requested_at_utc": requested.isoformat(),
                  "received_at_utc": received.isoformat(), "response_sha256": sha(response.content), "rows": rows}
        raw = _json_bytes(record)
        filename = f"evidence/{sha(raw)}.json"
        path = self.output / filename
        if not path.exists():
            _atomic_bytes(path, raw)
        return rows, {"file": filename, "sha256": sha(raw)}

    def clock_check(self):
        payload, evidence = self._fetch("https://fapi.binance.com/fapi/v1/time", {})
        record = json.loads((self.output / evidence["file"]).read_bytes())
        requested, received = _utc(record["requested_at_utc"]), _utc(record["received_at_utc"])
        midpoint = requested + (received-requested)/2
        offset = payload["serverTime"]/1000 - midpoint.timestamp()
        result = {"offset_seconds": offset, "round_trip_seconds": (received-requested).total_seconds(),
                  "offset_interval_seconds": [payload["serverTime"]/1000-received.timestamp(),
                                              payload["serverTime"]/1000-requested.timestamp()],
                  "checked_at_utc": received.isoformat(), "evidence": evidence}
        result["ok"] = (max(abs(v) for v in result["offset_interval_seconds"]) <= IMPLEMENTATION_RULES["clock_max_abs_offset_seconds"]
                        and result["round_trip_seconds"] <= IMPLEMENTATION_RULES["clock_max_round_trip_seconds"])
        return result

    def klines(self, symbol, start, end):
        if not SYMBOL.fullmatch(symbol):
            raise ValueError("Invalid native Binance symbol")
        start, end = _utc(start), _utc(end)
        if end > datetime.now(timezone.utc) or end < start or end-start > timedelta(days=30):
            raise ValueError("Kline window must be historical and at most30days")
        cursor, stop = int(start.timestamp()*1000), int(end.timestamp()*1000)
        rows, evidence = [], []
        while cursor <= stop:
            finish = min(stop, cursor + 1499 * 300000)
            part, receipt = self._fetch(ENDPOINT, {"symbol": symbol, "interval": "5m", "startTime": cursor,
                                                  "endTime": finish, "limit": 1500})
            if not isinstance(part, list) or len(part) > 1500:
                raise ValueError("Invalid official klines payload")
            if any(not cursor <= int(r[0]) <= finish for r in part):
                raise ValueError("Klines outside the query interval")
            rows.extend(part); evidence.append(receipt)
            cursor = finish + 300000
        return rows, evidence

    def funding(self, symbol, start, end):
        if not SYMBOL.fullmatch(symbol):
            raise ValueError("Invalid native Binance symbol")
        start, end = _utc(start), _utc(end)
        if end-start != timedelta(hours=1) or end > datetime.now(timezone.utc):
            raise ValueError("Only the completed1hour funding window is supported")
        a, b = int(start.timestamp()*1000), int(end.timestamp()*1000)
        rows, evidence = self._fetch(FUNDING_ENDPOINT, {"symbol": symbol, "startTime": a, "endTime": b, "limit": 1000})
        if not isinstance(rows, list) or len(rows) >= 1000:
            raise ValueError("Funding query is not proven complete")
        stamps = [int(r["fundingTime"]) for r in rows]
        if (len(set(stamps)) != len(stamps) or any(not a <= t <= b for t in stamps)
                or any(r["symbol"] != symbol for r in rows)):
            raise ValueError("Funding identity/interval mismatch")
        return rows, {**evidence, "complete_query_coverage": True, "start_ms": a, "end_ms": b}


def reference_audit(protocol):
    out = []
    for relative, expected in protocol.get("historical_reference_sha256", {}).items():
        path = (ROOT / relative).resolve()
        if not path.is_relative_to(ROOT):
            raise ValueError("Reference outside workspace")
        actual = sha(path.read_bytes()) if path.exists() else None
        out.append({"path": relative, "expected_sha256": expected, "current_sha256": actual,
                    "matches": actual == expected,
                    "mutable_poll_report": path.name == "receipt_latency.json"})
    return out


def snapshot(output, value):
    raw = _json_bytes(value)
    filename = f"evidence/{sha(raw)}.json"
    if not (output / filename).exists():
        _atomic_bytes(output / filename, raw)
    return {"file": filename, "sha256": sha(raw)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "activate", "freeze", "tick", "audit-null"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_PAPER)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--protocol", type=Path, default=PROTOCOL)
    parser.add_argument("--label-file", type=Path)
    parser.add_argument("--symbol")
    parser.add_argument("--entry")
    parser.add_argument("--direction", type=int, choices=(-1, 1))
    args = parser.parse_args()
    journal = PaperJournal(args.output_dir, args.archive_dir, args.protocol)
    if args.action == "status":
        status = journal.status()
        status.pop("events")
        result = {**status, "collector_health": journal.health(), "reference_audit": reference_audit(journal.protocol)}
        clock_path = args.output_dir / "clock_check.json"
        result["last_clock_check"] = json.loads(clock_path.read_bytes()) if clock_path.exists() else None
        blockers = []
        if not result["collector_health"]["healthy_for_new_freezes"]: blockers.append("collector_health_gate_failed")
        if not result["last_clock_check"] or not result["last_clock_check"]["ok"]: blockers.append("clock_gate_failed_or_unverified")
        validation_path = args.output_dir / "official_null_validation.json"
        audit_path = args.output_dir / "official_null_audit.json"
        if not validation_path.exists() or not audit_path.exists():
            blockers.append("official_null_validation_missing")
        else:
            validation = json.loads(validation_path.read_bytes())
            if not validation.get("ok") or validation.get("audit_sha256") != sha(audit_path.read_bytes()):
                blockers.append("official_null_validation_changed_or_failed")
        if any(not r["matches"] and not r["mutable_poll_report"] for r in result["reference_audit"]):
            blockers.append("immutable_protocol_reference_changed")
        result["activation_blockers"] = blockers
        result["status_is_not_activation_or_live_clock_certification"] = True
        report = args.archive_dir / "receipt_latency.json"
        if report.exists():
            result["latency_snapshot"] = snapshot(args.output_dir, json.loads(report.read_bytes()))
        result["readiness_snapshot"] = snapshot(args.output_dir, result)
        _atomic_bytes(args.output_dir / "readiness.json", _json_bytes(result))
    elif args.action == "freeze":
        if args.label_file is None or args.label_file.stat().st_size > 1024 * 1024:
            parser.error("A bounded --label-file is required")
        result = journal.freeze(json.loads(args.label_file.read_text(encoding="utf-8-sig")))
    else:
        provider = OfficialEvidence(args.output_dir)
        clock = provider.clock_check()
        _atomic_bytes(args.output_dir / "clock_check.json", _json_bytes(clock))
        if args.action != "audit-null" and not clock["ok"]:
            raise RuntimeError("Clock/round-trip check failed; paper activation and fills stopped")
        if args.action == "activate":
            audit = args.output_dir / "official_null_audit.json"
            validation = json.loads((args.output_dir / "official_null_validation.json").read_bytes())
            if (not validation.get("ok") or validation.get("audit_sha256") != sha(audit.read_bytes())
                    or json.loads(audit.read_bytes())["null"]["status"] != "ok"):
                raise RuntimeError("Official null audit is missing, changed or unvalidated")
            references = reference_audit(journal.protocol)
            if any(not r["matches"] and not r["mutable_poll_report"] for r in references):
                raise RuntimeError("Immutable protocol references changed; review before activation")
            result = journal.activate({**validation, "validation_sha256": sha((args.output_dir / "official_null_validation.json").read_bytes())}, clock)
        elif args.action == "tick":
            result = {"clock_check": clock, "actions": journal.tick(provider)}
        else:
            if not args.symbol or not args.entry or args.direction is None:
                parser.error("audit-null requires --symbol --entry --direction")
            entry = _utc(args.entry)
            audit_path = args.output_dir / "official_null_audit.json"
            previous = json.loads(audit_path.read_bytes()) if audit_path.exists() else {}
            if previous.get("symbol") == args.symbol and previous.get("entry_at") == entry.isoformat():
                rows, evidence = [], previous["evidence"]
                for ref in evidence:
                    path = (args.output_dir / ref["file"]).resolve()
                    if not path.is_relative_to(args.output_dir.resolve()) or sha(path.read_bytes()) != ref["sha256"]:
                        raise ValueError("Historical audit evidence path/hash mismatch")
                    saved = json.loads(path.read_bytes())
                    if saved["endpoint"] != ENDPOINT or saved["params"]["symbol"] != args.symbol:
                        raise ValueError("Historical audit evidence identity mismatch")
                    rows.extend(saved["rows"])
            else:
                rows, evidence = provider.klines(args.symbol, entry-timedelta(days=30), entry)
            result = {"clock_check": clock, "symbol": args.symbol, "entry_at": entry.isoformat(), "direction": args.direction,
                      "null": pre_entry_null(rows, entry, args.direction), "evidence": evidence}
            if audit_path.exists():
                old = audit_path.read_bytes()
                history = args.output_dir / "evidence" / f"{sha(old)}.json"
                if not history.exists(): _atomic_bytes(history, old)
            _atomic_bytes(audit_path, _json_bytes(result))
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
