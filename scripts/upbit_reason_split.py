"""Does the stated reason of an Upbit caution designation change the short's result? (round 7)

The research model labels each backtest caution notice with the same prompt
the live tracker uses (core/research/upbit_caution_tracker.REASON_SYSTEM_PROMPT),
reading the Korean body from the Upbit notice API. Then the 7-day perp short
results of scripts/upbit_short_backtest.py are split by that label.
Exploratory: ~31 trades cut into a few groups; nothing here changes the
tracker's pre-registered rules.
"""
import asyncio
import json
import sys
import time
from pathlib import Path

import httpx
import pandas as pd

sys.path.insert(0, ".")
from core.research import upbit_caution_tracker as uc
from core.research.exchange_research_runner import _llm_upbit_reason

OUT = Path("data/research/upbit")


async def main():
    notices = json.loads((OUT / "notices.json").read_text(encoding="utf-8"))
    ids = {n["title"]: n["id"] for n in notices}
    trades = pd.read_csv(OUT / "short_backtest.csv", parse_dates=["date"])
    events = pd.read_csv(OUT / "events.csv", parse_dates=["date"])
    c = trades[(trades.kind == "caution") & (trades.status == "ok")].merge(
        events[events.kind == "caution"][["token", "date", "title"]], on=["token", "date"], how="left")
    cache_path = OUT / "reasons.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    async with httpx.AsyncClient(timeout=30) as client:
        for title in c.title.dropna().unique():
            nid = str(ids.get(title))
            if nid in cache or nid == "None":
                continue
            body = ""
            for attempt in range(4):
                try:
                    body = await uc._notice_body(client, int(nid))
                    if body:
                        break
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(3 * (attempt + 1))
            raw = await _llm_upbit_reason(body[:8000]) if body else {}
            reason = str((raw or {}).get("reason") or "other")
            cache[nid] = {"reason": reason if reason in uc.REASONS else "other", "summary_en": (raw or {}).get("summary_en")}
            cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
            time.sleep(1)
    c["reason"] = [cache.get(str(ids.get(t)), {}).get("reason", "unknown") for t in c.title]
    c["extension"] = c.title.str.contains("연장", na=False)
    print(c.groupby("reason").short_ret.agg(n="count", mean="mean", median="median", wins=lambda x: int((x > 0).sum())).round(3).to_string())
    print()
    print(c[["date", "token", "reason", "extension", "short_ret"]].assign(date=lambda x: x.date.dt.date).sort_values("reason").round(3).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
