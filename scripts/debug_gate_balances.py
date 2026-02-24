"""Debug Gate balance payloads by account type."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.exchanges import EXCHANGE_CONFIGS
from core.exchanges.gate_connector import GateConnector

TYPES_TO_PROBE: List[Optional[str]] = [
    None,
    "spot",
    "funding",
    "margin",
    "cross_margin",
    "isolated",
    "swap",
    "future",
    "futures",
    "unified",
]


def _extract_positive_totals(payload: Dict[str, Any]) -> List[Tuple[str, float]]:
    total_map = payload.get("total") if isinstance(payload, dict) else {}
    if not isinstance(total_map, dict):
        total_map = {}
    rows = []
    for k, v in total_map.items():
        try:
            amount = float(v or 0.0)
        except Exception:
            amount = 0.0
        if amount > 0:
            rows.append((str(k).upper(), amount))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


async def _probe_once(connector: GateConnector, acc_type: Optional[str]) -> None:
    client = connector._client
    label = "default" if acc_type is None else acc_type
    try:
        if acc_type is None:
            bal = await client.fetch_balance()
        else:
            bal = await client.fetch_balance({"type": acc_type})
        positive = _extract_positive_totals(bal)
        print(f"[{label}] positive_assets={len(positive)} top={positive[:12]}")
    except Exception as e:
        print(f"[{label}] error={e}")


async def main() -> None:
    connector = GateConnector(EXCHANGE_CONFIGS["gate"])
    connected = await connector.connect()
    print(f"connected={connected}")
    if not connected:
        return

    try:
        for acc_type in TYPES_TO_PROBE:
            await _probe_once(connector, acc_type)
    finally:
        await connector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
