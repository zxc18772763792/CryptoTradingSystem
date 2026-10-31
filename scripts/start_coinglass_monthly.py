"""Initialize the independent monthly-universe paper tracker, without backfilling books."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.research import coinglass_composite_tracker as tracker
from core.research import coinglass_monthly_universe as monthly
from core.research import retirement
from core.research.exchange_research_runner import _client


async def main() -> None:
    # Register before any new outcome is available. Existing state is preserved.
    state = tracker.load_state(tracker.MONTHLY_STATE_PATH)
    retirement.register(state, "coinglass_composite_7d_monthly_v1", tracker._now().isoformat())
    tracker.save_state(state, tracker.MONTHLY_STATE_PATH)
    async with _client() as client:
        snapshot = await monthly.refresh(client)
        result = await tracker.tick(client, variant="monthly")
    print(json.dumps({"strategy": "coinglass_composite_7d_monthly_v1", "started_at": result["started_at"],
                      "snapshot": snapshot["id"], "selected_at": snapshot["selected_at"],
                      "coins": len(snapshot["members"]), "completed": result["forward_completed"],
                      "open": result["forward_open"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
