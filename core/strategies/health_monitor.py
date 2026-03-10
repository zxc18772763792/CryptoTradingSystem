"""Strategy runtime health monitor."""
from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from core.audit import audit_logger
from core.notifications import notification_manager
from core.strategies.strategy_manager import strategy_manager


class StrategyHealthMonitor:
    def __init__(self, check_interval_seconds: int = 20, alert_cooldown_seconds: int = 300):
        self.check_interval_seconds = max(5, int(check_interval_seconds))
        self.alert_cooldown_seconds = max(30, int(alert_cooldown_seconds))
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_check_at: Optional[datetime] = None
        self._last_alert_at: Optional[datetime] = None
        self._last_result: Dict[str, Any] = {}
        self._last_error: Optional[str] = None

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _default_channels(self) -> List[str]:
        status = notification_manager.channel_status()
        return [ch for ch, enabled in status.items() if enabled]

    async def check_once(self) -> Dict[str, Any]:
        summary = strategy_manager.get_dashboard_summary(signal_limit=10)
        stale = summary.get("stale_running", []) or []
        stale_count = int(summary.get("stale_running_count", 0) or 0)
        running_count = int(summary.get("running_count", 0) or 0)
        stale_names = [str(item.get("strategy", "")) for item in stale if item.get("strategy")]

        result = {
            "timestamp": self._now().isoformat(),
            "running_count": running_count,
            "stale_running_count": stale_count,
            "stale_strategies": stale_names,
            "alert_sent": False,
        }

        if stale_count > 0:
            allow_alert = True
            if self._last_alert_at:
                elapsed = (self._now() - self._last_alert_at).total_seconds()
                allow_alert = elapsed >= self.alert_cooldown_seconds

            if allow_alert:
                details = []
                for item in stale[:8]:
                    strategy = str(item.get("strategy", "unknown"))
                    lag = item.get("lag_seconds")
                    tf = str(item.get("timeframe", "-"))
                    lag_text = f"{lag}s" if isinstance(lag, int) else "unknown"
                    details.append(f"{strategy}({tf}, lag={lag_text})")

                message = (
                    f"检测到 {stale_count} 个策略可能卡住。\n"
                    f"运行中策略数: {running_count}\n"
                    f"异常策略: {', '.join(details)}"
                )
                channels = self._default_channels()
                send_result = {}
                if channels:
                    send_result = await notification_manager.send_message(
                        title="策略健康告警",
                        message=message,
                        channels=channels,
                    )
                await audit_logger.log(
                    module="strategy",
                    action="health_watchdog_alert",
                    status="success",
                    message=f"stale_count={stale_count}",
                    details={
                        "running_count": running_count,
                        "stale_running_count": stale_count,
                        "stale_strategies": stale_names,
                        "channels": channels,
                        "send_result": send_result,
                    },
                )
                self._last_alert_at = self._now()
                result["alert_sent"] = True

        self._last_check_at = self._now()
        self._last_result = result
        return result

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.check_once()
                self._last_error = None
            except Exception as e:
                self._last_error = str(e)
                logger.warning(f"Strategy health monitor check failed: {e}")
            await asyncio.sleep(self.check_interval_seconds)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="strategy_health_monitor")

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def get_status(self) -> Dict[str, Any]:
        now = self._now()
        next_check_at = (
            (self._last_check_at + timedelta(seconds=self.check_interval_seconds)).isoformat()
            if self._last_check_at
            else None
        )
        return {
            "running": self._running,
            "check_interval_seconds": self.check_interval_seconds,
            "alert_cooldown_seconds": self.alert_cooldown_seconds,
            "last_check_at": self._last_check_at.isoformat() if self._last_check_at else None,
            "next_check_at": next_check_at,
            "last_alert_at": self._last_alert_at.isoformat() if self._last_alert_at else None,
            "last_error": self._last_error,
            "last_result": self._last_result,
            "timestamp": now.isoformat(),
        }

strategy_health_monitor = StrategyHealthMonitor()
