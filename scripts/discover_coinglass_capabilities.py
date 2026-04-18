from __future__ import annotations

import asyncio
import json

from core.data.coinglass_client import discover_and_persist_coinglass_capabilities


async def _main() -> None:
    result = await discover_and_persist_coinglass_capabilities()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
