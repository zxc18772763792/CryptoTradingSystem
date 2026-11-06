"""Refresh only the two source statuses after the runner's next scheduled cycle."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from core.research.telegram_forward_tracker import summary

result = {"captured_at": datetime.now(timezone.utc).isoformat(), "sources": {}}
for kind in ("cryptoquant", "binance_chat"):
    row = summary(kind)
    result["sources"][kind] = {k: row.get(k) for k in
                               ("tracker_status_label", "runner_active", "source_health", "last_error", "runner")}
Path(__file__).with_name("health_refresh.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(result, ensure_ascii=False))
