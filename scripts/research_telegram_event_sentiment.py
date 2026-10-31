"""Freeze and inspect count-based sentiment without touching active ledgers."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.telegram_event_sentiment import PROTOCOL, digest, read_archive, study, utc


def write(path: Path, value) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    tmp.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data/research/telegram_live_20261024")
    parser.add_argument("--output", type=Path, default=ROOT / "data/research/binance_chat_event_time_20261031")
    args = parser.parse_args()
    base, output = args.source.resolve(), args.output.resolve()
    if output == base or base in output.parents or not output.is_relative_to(ROOT / "data/research"):
        raise ValueError("Use a separate research directory outside the active study")
    output.mkdir(parents=True, exist_ok=True)
    protocol_path = output / "protocol.json"
    if protocol_path.exists():
        frozen = json.loads(protocol_path.read_text(encoding="utf-8"))
        if frozen["protocol"] != PROTOCOL:
            raise ValueError("Existing protocol differs; choose a new directory")
    else:
        write(protocol_path, {"frozen_at": datetime.now(timezone.utc).isoformat(), "protocol": PROTOCOL, "protocol_sha256": digest(PROTOCOL)})
    result_path = output / "results.json"
    if result_path.exists():
        result = json.loads(result_path.read_text(encoding="utf-8"))
    else:
        activation = json.loads((base / "activation.json").read_text(encoding="utf-8"))
        status = json.loads((base / "forward_status.json").read_text(encoding="utf-8"))
        messages, metadata, coverage = read_archive(base, activation=activation, status=status)
        result = {"computed_at": datetime.now(timezone.utc).isoformat(), "protocol_sha256": digest(PROTOCOL),
                  "source": metadata, **study(messages, coverage=coverage, as_of=utc(metadata["as_of"]))}
        write(result_path, result)
    compact = [{k: c[k] for k in ("clock", "size", "complete_batches", "qualified_batches", "certified_qualified_batches", "latest_rolling")}
               for c in result["configs"]]
    print(json.dumps({"source": result["source"], "comparison": compact}, ensure_ascii=False))


if __name__ == "__main__":
    main()
