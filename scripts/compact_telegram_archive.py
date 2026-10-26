"""Pack old duplicate-only Telegram archive polls into gzip bundles (no network, never edits a run).

F: is exFAT with 1 MiB clusters, so the archive's one-file-per-poll layout costs ~0.8 GB a day per
channel. The news collector packs a batch now and then (``telegram_observation_compaction``); this
command does the same on demand, e.g. once for an existing backlog, then fully re-verifies the chain.

    python scripts/compact_telegram_archive.py --dry-run          # every archived channel, plan only
    python scripts/compact_telegram_archive.py                    # pack, then verify each channel
    python scripts/compact_telegram_archive.py --channel cryptoquant_alert
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.news.alastar_observation import (  # noqa: E402
    CHANNEL, OTHER_CHANNELS_ROOT, RUN_BUNDLES, archive_dir, compact_runs, verify_archive,
)

CLUSTER = 1024 * 1024  # F: allocation unit; the on-disk cost of every file below 1 MiB


def channels() -> list[str]:
    found = [CHANNEL] if (archive_dir(CHANNEL) / "runs").exists() else []
    if OTHER_CHANNELS_ROOT.exists():
        found += sorted(p.name for p in OTHER_CHANNELS_ROOT.iterdir() if (p / "runs").exists())
    return found


def footprint(output: Path) -> dict:
    files = list((output / "runs").glob("*.json")) + list((output / RUN_BUNDLES).glob("*"))
    sizes = [p.stat().st_size for p in files]
    return {"files": len(files), "bytes": sum(sizes), "allocated_bytes_at_1mib": sum(-(-max(s, 1) // CLUSTER) * CLUSTER for s in sizes)}


def locked(call, attempts: int = 30):
    """The collector holds the archive lock for a moment each poll; wait it out instead of failing."""
    for attempt in range(attempts):
        try:
            return call()
        except RuntimeError as exc:
            if "archive lock" not in str(exc) or attempt == attempts - 1:
                raise
            time.sleep(2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--channel", action="append", help="channel username (repeatable); default: every archive")
    parser.add_argument("--dry-run", action="store_true", help="report the next batch only; write nothing")
    parser.add_argument("--max-batches", type=int, default=50)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    report = {}
    for channel in args.channel or channels():
        output = archive_dir(channel)
        before = footprint(output)
        batches = []
        for _ in range(1 if args.dry_run else args.max_batches):
            result = locked(lambda: compact_runs(output, channel=channel, dry_run=args.dry_run))
            batches.append(result)
            if args.dry_run or result.get("reason"):  # a reason means nothing was packed
                break
        entry = {"before": before, "batches": batches}
        if not args.dry_run:
            verified = locked(lambda: verify_archive(output, channel=channel))
            entry.update(after=footprint(output), verified_full_chain=verified["verified_full_chain"],
                         run_count=verified["run_count"])
        report[channel] = entry
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
