from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.data.coinglass_client import discover_and_persist_coinglass_capabilities


async def _main() -> None:
    result = await discover_and_persist_coinglass_capabilities()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
