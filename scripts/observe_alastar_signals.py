"""One bounded Alastar public-preview observation or explicitly historical JSONL import."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Re-export the small archive interface for existing offline research callers.
from core.news.alastar_observation import (
    CHANNEL, DEFAULT_OUTPUT, ENDPOINT, MAX_INPUT_BYTES, MAX_RECORDS,
    _archive_lock, import_jsonl, observe_records, receipt_latency, verify_archive,
)


def capture_public_preview(output: Path | str = DEFAULT_OUTPUT) -> dict[str, Any]:
    import requests
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    from core.news.collectors.telegram import TelegramPublicChannelCollector
    from scripts.collect_alastar_history import _fetch_page

    # Match the existing news worker's configured network path. Never print or
    # persist proxy credentials in observation evidence.
    ensure_proxy_env(settings)
    with requests.Session() as session:
        session.headers.update({"User-Agent": "crypto-trading-system-public-observation/1.0"})
        raw = _fetch_page(session, ENDPOINT, time.sleep)
        observed_at = datetime.now(timezone.utc)
    collector = TelegramPublicChannelCollector({"defaults": {"telegram_alastar_channels": [CHANNEL]}})
    # The body is already capped at 2 MiB; retain every extracted text character
    # rather than the news collector's normal 16k/20k display limit.
    collector.max_text_chars = collector.MAX_HTML_BYTES
    records = collector._from_html(raw.decode("utf-8", errors="strict"), channel=CHANNEL, include_media_only=True)
    return observe_records(records, output, mode="live_public_preview", observed_at=observed_at,
                           evidence={"kind": "fresh_https_public_preview", "url": ENDPOINT}, raw_preview=raw)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--input", type=Path, help="Import cached JSONL as historical only; never prospective")
    action.add_argument("--verify", action="store_true", help="Verify every immutable run and rebuild projections; no network read")
    action.add_argument("--latency", action="store_true",
                        help="Verify runs, then write receipt_latency.json (poll gaps, first-receipt delays); no network read")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--max-records", type=int, default=MAX_RECORDS, help="Historical import bound, maximum 5000")
    args = parser.parse_args()
    try:
        if args.verify:
            result = verify_archive(args.output_dir)
        elif args.latency:
            result = receipt_latency(args.output_dir)
            result["live_first_receipts"] = {k: v for k, v in result["live_first_receipts"].items() if k != "samples"}
        elif args.input:
            records, evidence = import_jsonl(args.input, args.max_records)
            result = observe_records(records, args.output_dir, evidence=evidence)
        else:
            result = capture_public_preview(args.output_dir)
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(1, f"Observation stopped: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
