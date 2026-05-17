from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.env_utils import env_bool
from core.news.collectors.opennews import OpenNewsCollector


def _summary_item(item: Dict[str, Any]) -> Dict[str, Any]:
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    return {
        "title": item.get("title"),
        "url": item.get("url"),
        "published_at": item.get("published_at"),
        "symbols": item.get("symbols") or [],
        "score": payload.get("opennews_ai_score"),
        "signal": payload.get("opennews_ai_signal"),
        "news_type": payload.get("opennews_news_type"),
        "engine_type": payload.get("opennews_engine_type"),
    }


def _print(data: Dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _load_opennews_config() -> Dict[str, Any]:
    path = ROOT / "config" / "news_rules.yaml"
    if not path.exists():
        return {"defaults": {}}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {"defaults": data.get("defaults") if isinstance(data, dict) else {}}


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenNews/6551 collector self-check")
    parser.add_argument("--max-records", type=int, default=5)
    parser.add_argument("--since-minutes", type=int, default=24 * 60)
    parser.add_argument("--query", type=str, default="")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero if NEWS_ENABLE_OPENNEWS or OPENNEWS_TOKEN is missing.",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    enabled = env_bool("NEWS_ENABLE_OPENNEWS", False)
    token_present = bool(str(os.getenv("OPENNEWS_TOKEN") or "").strip())
    configured = enabled and token_present

    if not configured:
        reason_parts: List[str] = []
        if not enabled:
            reason_parts.append("NEWS_ENABLE_OPENNEWS is not true")
        if not token_present:
            reason_parts.append("OPENNEWS_TOKEN is missing")
        payload = {
            "ok": not args.strict,
            "configured": False,
            "enabled": enabled,
            "token_present": token_present,
            "reason": "; ".join(reason_parts),
            "hint": "Set NEWS_ENABLE_OPENNEWS=true and OPENNEWS_TOKEN in .env to enable live OpenNews pulls.",
        }
        _print(payload)
        return 2 if args.strict else 0

    try:
        cfg = _load_opennews_config()
        collector = OpenNewsCollector(cfg)
        items = collector.pull_latest(
            query=args.query or None,
            max_records=max(1, min(int(args.max_records or 5), 20)),
            since_minutes=max(1, int(args.since_minutes or 24 * 60)),
        )
        _print(
            {
                "ok": True,
                "configured": True,
                "enabled": enabled,
                "token_present": token_present,
                "count": len(items),
                "items": [_summary_item(item) for item in items[:5]],
            }
        )
        return 0
    except Exception as exc:
        _print(
            {
                "ok": False,
                "configured": True,
                "enabled": enabled,
                "token_present": token_present,
                "error": str(exc),
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
