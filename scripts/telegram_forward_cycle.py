"""Run the frozen Telegram research CLI with a public-data compatibility transport.

Binance's edge currently answers ``/fapi/v1/fundingRate?limit>=500`` with an HTML 403 through
the local proxy (limit<=100 and ``/klines?limit=1000`` still work), which made every cycle fail
at the last, market-data step. The frozen script hard-codes ``limit=1000`` and its bytes are part
of the activation implementation hash, so it is NOT edited. This wrapper imports it unchanged and
replaces only ``public_json``: the same rows, funding fetched ``PAGE`` at a time.
The transport reuses verified TLS connections and bounds transient retries;
refusals and rate limits are final. Public response receipts are stored separately.

Remove this wrapper (and point ``core/research/telegram_forward_runner.py`` back at the script)
once ``limit=1000`` works again; nothing here touches the research directory or its protocol.
"""
from __future__ import annotations

from pathlib import Path
import asyncio
from datetime import datetime, timezone
import json
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import scripts.telegram_forward_research as cli  # noqa: E402
from scripts.telegram_market_transport import PublicMarketTransport  # noqa: E402

PAGE = 100
FUNDING = "/fapi/v1/fundingRate"
_transport = PublicMarketTransport(cli.bridge.proxy_config)
_original = _transport.get_json


def paged_public_json(endpoint, params=None):
    if endpoint != FUNDING or not params or int(params.get("limit") or 0) <= PAGE:
        return _original(endpoint, params)
    wanted = int(params["limit"])
    page_params = dict(params, limit=PAGE)
    rows = []
    while len(rows) < wanted:
        page = _original(endpoint, page_params)
        if not isinstance(page, list) or len(page) > page_params["limit"]:
            raise ValueError("Invalid public funding page")
        times = [int(r["fundingTime"]) for r in page]
        if times != sorted(set(times)) or (rows and times and times[0] <= int(rows[-1]["fundingTime"])):
            raise ValueError("Funding pagination duplicated or failed to advance")
        if any(t < int(page_params.get("startTime", 0)) or t > int(page_params.get("endTime", 2**63 - 1)) for t in times):
            raise ValueError("Funding page outside requested window")
        rows.extend(page)
        if len(page) < PAGE:
            break
        page_params = dict(page_params, startTime=int(page[-1]["fundingTime"]) + 1)
    return rows[:wanted]


cli.public_json = paged_public_json

# The outer frozen cycle owns both study.lock and reader.lock. A failed
# connection has already disconnected in collect()'s finally before we retry.
# No RPC refusal, authentication, FloodWait or lock error is retried here.
_original_collect = cli.collect


def _record_collection_retry(error):
    folder = ROOT / "data/research/telegram_transport_recovery"
    folder.mkdir(parents=True, exist_ok=True)
    at = datetime.now(timezone.utc)
    path = folder / (at.strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:12] + ".json")
    with path.open("x", encoding="utf-8") as stream:
        json.dump({"at_utc": at.isoformat(), "error_type": type(error).__name__,
                   "action": "one_reconnect_under_existing_study_and_reader_locks",
                   "new_login": False}, stream, indent=2)


async def collect_with_retry(store, *, historical=False):
    for attempt in range(2):
        try:
            return await _original_collect(store, historical=historical)
        except (asyncio.IncompleteReadError, ConnectionError, TimeoutError) as exc:
            if attempt or historical:
                raise
            # Preserve any genuinely received fragment and its first-received
            # clock. Frozen ingest/difference processing deduplicates the replay;
            # no decision is made until the entire collection returns normally.
            store.db.commit()
            _record_collection_retry(exc)
            await asyncio.sleep(1)


cli.collect = collect_with_retry

# Keep the frozen decision/settlement implementation unchanged. Refresh only
# its derived readout, under the same study lock and inside this child process.
_original_dispatch = cli.dispatch


async def dispatch_with_summary(args, store):
    await _original_dispatch(args, store)
    if args.command == "cycle":
        from scripts.report_telegram_forward import summarize

        summarize(store.root)
        # Independent versioned observation, consuming the completed source
        # checkpoint only. Its failure cannot invalidate the original books.
        from core.research import telegram_sentiment_observer

        if (telegram_sentiment_observer.observer_dir(store.root) / "activation.json").exists():
            try:
                telegram_sentiment_observer.sync(store.root)
            except Exception as exc:
                try:
                    telegram_sentiment_observer.record_error(store.root, exc)
                except Exception:
                    # The original checkpoint is already complete. An optional
                    # observer directory failure must not relabel it as failed.
                    pass
                print(json.dumps({"sentiment_observer": "failed", "error_type": type(exc).__name__}), flush=True)


cli.dispatch = dispatch_with_summary

if __name__ == "__main__":
    try:
        raise SystemExit(cli.main())
    finally:
        _transport.close()
