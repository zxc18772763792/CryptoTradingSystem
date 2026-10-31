"""Initialize/refresh the independent sentiment observer without Telegram access."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.telegram_forward_tracker import research_dir
from core.research.telegram_sentiment_observer import record_error, sync


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=research_dir())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        value = sync(args.source, args.output)
    except Exception as error:
        record_error(args.source, error, args.output)
        print(json.dumps({"status": "failed", "error_type": type(error).__name__}))
        return 1
    print(json.dumps({k: value[k] for k in ("version", "activation_at", "updated_at", "new_views_in_segment", "rolling", "books")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
