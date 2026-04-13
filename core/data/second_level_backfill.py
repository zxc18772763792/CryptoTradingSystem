"""Second-level historical data backfill with resume support."""
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from config.settings import settings
from core.exchanges import exchange_manager
from core.data.path_utils import canonical_symbol_dir, canonical_symbol_dirname


def _to_iso(ts: datetime) -> str:
    return ts.isoformat()


def _from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


@dataclass
class SecondLevelBackfillTask:
    task_id: str
    exchange: str
    symbol: str
    start_time: datetime
    end_time: datetime
    current_time: datetime
    window_days: int = 1
    status: str = "pending"  # pending/running/completed/stopped/failed
    total_days: int = 0
    completed_days: int = 0
    total_trades: int = 0
    total_bars: int = 0
    total_windows: int = 0
    completed_windows: int = 0
    stop_requested: bool = False
    error_count: int = 0
    last_error: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for key in ["start_time", "end_time", "current_time", "created_at", "updated_at"]:
            d[key] = _to_iso(getattr(self, key))
        d["progress_ratio"] = (
            min(1.0, max(0.0, self.completed_days / self.total_days)) if self.total_days > 0 else 0.0
        )
        return d

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SecondLevelBackfillTask":
        obj = dict(payload)
        obj.pop("progress_ratio", None)
        for key in ["start_time", "end_time", "current_time", "created_at", "updated_at"]:
            if isinstance(obj.get(key), str):
                obj[key] = _from_iso(obj[key])
        return cls(**obj)


class SecondLevelBackfillManager:
    def __init__(self):
        self._tasks: Dict[str, SecondLevelBackfillTask] = {}
        self._runners: Dict[str, asyncio.Task] = {}
        self._state_dir = Path(settings.CACHE_PATH) / "second_level_backfill"
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._loaded = False

    def _task_file(self, task_id: str) -> Path:
        return self._state_dir / f"{task_id}.json"

    def _parts_dir(self, exchange: str, symbol: str) -> Path:
        return canonical_symbol_dir(Path(settings.DATA_STORAGE_PATH), exchange, symbol) / "1s_parts"

    def _load_states(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        for fp in self._state_dir.glob("*.json"):
            try:
                payload = json.loads(fp.read_text(encoding="utf-8"))
                task = SecondLevelBackfillTask.from_dict(payload)
                self._tasks[task.task_id] = task
            except Exception as e:
                logger.warning(f"Failed to load second-level backfill state {fp}: {e}")

    def _save_state(self, task: SecondLevelBackfillTask) -> None:
        try:
            task.updated_at = datetime.now(timezone.utc)
            self._task_file(task.task_id).write_text(
                json.dumps(task.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"Failed to save second-level backfill state {task.task_id}: {e}")

    async def _ensure_exchange(self, exchange: str):
        connector = exchange_manager.get_exchange(exchange)
        if connector:
            return connector
        await exchange_manager.initialize([exchange])
        return exchange_manager.get_exchange(exchange)

    async def _fetch_trades_window(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
        max_loops: int = 20000,
    ) -> List[Dict[str, Any]]:
        connector = await self._ensure_exchange(exchange)
        if not connector:
            raise RuntimeError(f"exchange not connected: {exchange}")

        client = getattr(connector, "_client", None)
        fetch_trades = getattr(client, "fetch_trades", None)
        if not callable(fetch_trades):
            raise RuntimeError(f"{exchange} does not support fetch_trades")

        since_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        loops = 0
        out: List[Dict[str, Any]] = []
        seen = set()
        stagnant_loops = 0

        while since_ms < end_ms and loops < max_loops:
            loops += 1
            batch = await fetch_trades(symbol, since=since_ms, limit=limit)
            if not batch:
                break

            batch_ts = [int(t.get("timestamp")) for t in batch if t.get("timestamp") is not None]
            if batch_ts and min(batch_ts) > end_ms:
                # Exchange returned newer trades than target window (likely ignored `since`).
                break

            max_ts_in_batch = since_ms
            added_in_loop = 0
            for t in batch:
                ts = t.get("timestamp")
                if ts is None:
                    continue
                ts = int(ts)
                if ts > end_ms:
                    continue
                key = (ts, t.get("id"), t.get("price"), t.get("amount"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(t)
                added_in_loop += 1
                if ts > max_ts_in_batch:
                    max_ts_in_batch = ts

            if max_ts_in_batch <= since_ms:
                since_ms += 1
            else:
                since_ms = max_ts_in_batch + 1

            if added_in_loop == 0:
                stagnant_loops += 1
                if stagnant_loops >= 3:
                    break
            else:
                stagnant_loops = 0

            # If server returns less than limit, this window likely exhausted.
            if len(batch) < limit:
                break

            await asyncio.sleep(0.08)

        return out

    async def _fetch_1s_klines_window(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
        max_loops: int = 20000,
    ) -> pd.DataFrame:
        connector = await self._ensure_exchange(exchange)
        if not connector:
            return pd.DataFrame()
        client = getattr(connector, "_client", None)
        supported_timeframes = getattr(client, "timeframes", None) or {}
        if isinstance(supported_timeframes, dict) and "1s" not in supported_timeframes:
            return pd.DataFrame()

        since_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        loops = 0
        rows: List[Dict[str, Any]] = []

        while since_ms < end_ms and loops < max_loops:
            loops += 1
            try:
                batch = await connector.get_klines(
                    symbol=symbol,
                    timeframe="1s",
                    since=datetime.fromtimestamp(since_ms / 1000),
                    limit=limit,
                )
            except Exception:
                return pd.DataFrame()

            if not batch:
                break

            max_ts = since_ms
            valid_count = 0
            for k in batch:
                ts_ms = int(k.timestamp.timestamp() * 1000)
                if ts_ms > end_ms:
                    continue
                rows.append(
                    {
                        "timestamp": k.timestamp,
                        "open": float(k.open),
                        "high": float(k.high),
                        "low": float(k.low),
                        "close": float(k.close),
                        "volume": float(k.volume),
                    }
                )
                valid_count += 1
                if ts_ms > max_ts:
                    max_ts = ts_ms

            if max_ts <= since_ms:
                since_ms += 1000
            else:
                since_ms = max_ts + 1000

            if len(batch) < limit or valid_count == 0:
                break

            await asyncio.sleep(0.05)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).set_index("timestamp").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df

    def _trades_to_1s(self, trades: List[Dict[str, Any]]) -> pd.DataFrame:
        if not trades:
            return pd.DataFrame()

        rows = []
        for t in trades:
            ts = t.get("timestamp")
            price = t.get("price")
            amount = t.get("amount")
            if ts is None or price is None or amount is None:
                continue
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(float(ts) / 1000),
                    "price": float(price),
                    "amount": float(amount),
                }
            )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).set_index("timestamp").sort_index()
        ohlc = df["price"].resample("1S").ohlc()
        volume = df["amount"].resample("1S").sum().rename("volume")
        merged = pd.concat([ohlc, volume], axis=1).dropna()
        merged.columns = ["open", "high", "low", "close", "volume"]
        return merged

    def _save_parts(self, exchange: str, symbol: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        parts_dir = self._parts_dir(exchange, symbol)
        parts_dir.mkdir(parents=True, exist_ok=True)
        count = 0

        grouped = df.groupby(df.index.date)
        for day, day_df in grouped:
            fp = parts_dir / f"{day.isoformat()}.parquet"
            write_df = day_df.sort_index()
            if fp.exists():
                old = pd.read_parquet(fp)
                old.index = pd.to_datetime(old.index)
                before = len(old)
                merged = pd.concat([old, write_df])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                merged.to_parquet(fp)
                count += max(0, len(merged) - before)
            else:
                write_df.to_parquet(fp)
                count += len(write_df)

        return count

    async def _run(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        task.status = "running"
        self._save_state(task)

        cursor = task.current_time
        while cursor < task.end_time:
            task = self._tasks.get(task_id)
            if not task:
                return
            if task.stop_requested:
                task.status = "stopped"
                self._save_state(task)
                return

            window_end = min(cursor + timedelta(days=task.window_days), task.end_time)
            try:
                bars_df = await self._fetch_1s_klines_window(
                    exchange=task.exchange,
                    symbol=task.symbol,
                    start_time=cursor,
                    end_time=window_end,
                )
                trades: List[Dict[str, Any]] = []
                if bars_df.empty:
                    trades = await self._fetch_trades_window(
                        exchange=task.exchange,
                        symbol=task.symbol,
                        start_time=cursor,
                        end_time=window_end,
                    )
                    bars_df = self._trades_to_1s(trades)

                inserted = self._save_parts(task.exchange, task.symbol, bars_df)

                task.total_trades += len(trades)
                task.total_bars += int(len(bars_df))
                task.completed_windows += 1
                task.completed_days = min(
                    task.total_days,
                    max(0, (window_end.date() - task.start_time.date()).days + 1),
                )
                task.current_time = window_end
                task.last_error = ""
                logger.info(
                    f"Second-level backfill [{task.task_id}] "
                    f"{cursor.date()} -> {window_end.date()} "
                    f"bars={len(bars_df)} inserted={inserted} trades={len(trades)}"
                )
                self._save_state(task)
                cursor = window_end
            except Exception as e:
                task.error_count += 1
                task.last_error = str(e)
                self._save_state(task)
                logger.warning(f"Second-level backfill error [{task_id}] {cursor} -> {window_end}: {e}")
                await asyncio.sleep(3)
                # skip this window after repeated failures
                if task.error_count >= 3:
                    cursor = window_end
                    task.current_time = window_end
                    task.error_count = 0
                    self._save_state(task)

            await asyncio.sleep(0.05)

        task.status = "completed"
        task.current_time = task.end_time
        self._save_state(task)

    def start_task(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        window_days: int = 1,
    ) -> Dict[str, Any]:
        self._load_states()

        if end_time <= start_time:
            raise ValueError("end_time must be after start_time")

        clean_symbol = canonical_symbol_dirname(symbol) or str(symbol).replace("/", "_")
        task_id = f"{exchange}_{clean_symbol}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
        task = self._tasks.get(task_id)
        if task is None:
            total_days = max(1, int(((end_time - start_time).total_seconds() + 86400 - 1) // 86400))
            total_windows = max(1, (total_days + max(1, int(window_days)) - 1) // max(1, int(window_days)))
            task = SecondLevelBackfillTask(
                task_id=task_id,
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                current_time=start_time,
                window_days=max(1, int(window_days)),
                total_days=total_days,
                total_windows=total_windows,
            )
            self._tasks[task_id] = task
            self._save_state(task)

        # Resume if existing.
        if task.status in {"completed", "failed"}:
            task.status = "pending"
            task.stop_requested = False
            task.current_time = start_time
            task.completed_days = 0
            task.total_trades = 0
            task.total_bars = 0
            task.last_error = ""
            self._save_state(task)

        runner = self._runners.get(task_id)
        if runner and not runner.done():
            return {"task_id": task_id, "started": False, "task": task.to_dict()}

        task.stop_requested = False
        task.status = "running"
        self._save_state(task)
        self._runners[task_id] = asyncio.create_task(self._run(task_id), name=f"seconds_backfill_{task_id}")
        return {"task_id": task_id, "started": True, "task": task.to_dict()}

    def stop_task(self, task_id: str) -> bool:
        self._load_states()
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.stop_requested = True
        if task.status == "pending":
            task.status = "stopped"
        self._save_state(task)
        return True

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        self._load_states()
        task = self._tasks.get(task_id)
        if not task:
            return None
        return task.to_dict()

    def list_tasks(self) -> List[Dict[str, Any]]:
        self._load_states()
        values = [task.to_dict() for task in self._tasks.values()]
        values.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        return values


second_level_backfill_manager = SecondLevelBackfillManager()
