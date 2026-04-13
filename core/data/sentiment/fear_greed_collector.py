"""Fear & Greed Index collector (Alternative.me)."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

import aiohttp
from loguru import logger


class SentimentClassification(str, Enum):
    EXTREME_FEAR = "Extreme Fear"
    FEAR = "Fear"
    NEUTRAL = "Neutral"
    GREED = "Greed"
    EXTREME_GREED = "Extreme Greed"


@dataclass
class FearGreedIndex:
    value: int
    timestamp: datetime
    classification: str = field(default="")
    time_until_update: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.classification:
            self.classification = self._classify(self.value)

    @staticmethod
    def _classify(value: int) -> str:
        if value <= 25:
            return SentimentClassification.EXTREME_FEAR.value
        if value <= 45:
            return SentimentClassification.FEAR.value
        if value <= 55:
            return SentimentClassification.NEUTRAL.value
        if value <= 75:
            return SentimentClassification.GREED.value
        return SentimentClassification.EXTREME_GREED.value

    @property
    def is_extreme_fear(self) -> bool:
        return self.value <= 25

    @property
    def is_extreme_greed(self) -> bool:
        return self.value >= 75

    @property
    def signal(self) -> str:
        if self.value <= 25:
            return "buy"
        if self.value >= 75:
            return "sell"
        return "neutral"

    @property
    def signal_strength(self) -> float:
        if self.value <= 50:
            return (50 - self.value) / 50
        return (self.value - 50) / 50

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "classification": self.classification,
            "timestamp": self.timestamp.isoformat(),
            "time_until_update": self.time_until_update,
            "signal": self.signal,
            "signal_strength": self.signal_strength,
        }


class FearGreedCollector:
    API_URL = "https://api.alternative.me/fng/"

    def __init__(self, timeout: int = 10):
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._history: List[FearGreedIndex] = []

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, trust_env=True)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def fetch_current(self) -> Optional[FearGreedIndex]:
        session = await self._get_session()
        try:
            async with session.get(self.API_URL, params={"limit": 1}) as resp:
                if resp.status != 200:
                    logger.warning(f"Fear & Greed API returned {resp.status}")
                    return None
                data = await resp.json()
                rows = data.get("data") or []
                if not rows:
                    return None
                item = rows[0]
                return FearGreedIndex(
                    value=int(item["value"]),
                    classification=str(item.get("value_classification") or ""),
                    timestamp=datetime.fromtimestamp(int(item["timestamp"])),
                    time_until_update=int(item.get("time_until_update", 0)) or None,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Fear & Greed fetch error: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Fear & Greed parse error: {e}")
            return None

    async def fetch_history(self, days: int = 30) -> List[FearGreedIndex]:
        session = await self._get_session()
        try:
            async with session.get(self.API_URL, params={"limit": min(max(1, int(days)), 365)}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                rows = data.get("data") or []
                out: List[FearGreedIndex] = []
                for item in rows:
                    out.append(
                        FearGreedIndex(
                            value=int(item["value"]),
                            classification=str(item.get("value_classification") or ""),
                            timestamp=datetime.fromtimestamp(int(item["timestamp"])),
                            time_until_update=int(item.get("time_until_update", 0)) or None,
                        )
                    )
                out.sort(key=lambda x: x.timestamp, reverse=True)
                self._history = out
                return out
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Fear & Greed history fetch error: {e}")
            return []
        except Exception as e:
            logger.error(f"Fear & Greed history parse error: {e}")
            return []

    def get_statistics(self, days: int = 30) -> Dict:
        if not self._history:
            return {}
        recent = self._history[: max(1, int(days))]
        values = [idx.value for idx in recent]
        if not values:
            return {}
        return {
            "current": self._history[0].value if self._history else None,
            "mean": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
            "days_extreme_fear": sum(1 for v in values if v <= 25),
            "days_extreme_greed": sum(1 for v in values if v >= 75),
            "days_in_range": len(values),
        }

    async def start_collection(self, interval: int = 3600, callback=None):
        self._running = True
        logger.info(f"Starting Fear & Greed collection every {interval}s")
        while self._running:
            try:
                index = await self.fetch_current()
                if index and callback:
                    try:
                        await callback(index)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Collection loop error: {e}")
                await asyncio.sleep(60)

    def stop_collection(self):
        self._running = False
        logger.info("Fear & Greed collection stopped")

    @property
    def is_running(self) -> bool:
        return self._running


fear_greed_collector = FearGreedCollector()


if __name__ == "__main__":
    async def _main():
        async with FearGreedCollector() as collector:
            current = await collector.fetch_current()
            print(current.to_dict() if current else "fetch failed")

    asyncio.run(_main())
