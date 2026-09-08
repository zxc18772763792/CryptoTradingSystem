"""Persisted, bounded LLM research rounds. Produces queued backtests only."""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from config.settings import settings


class ResearchLoopConfig(BaseModel):
    enabled: bool = False
    interval_seconds: int = Field(default=3600, ge=600, le=86400)
    max_rounds_per_day: int = Field(default=6, ge=1, le=24)
    symbols: list[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"], min_length=1, max_length=8)
    timeframes: list[Literal["15m", "1h", "4h"]] = Field(default_factory=lambda: ["1h"] , min_length=1, max_length=3)
    days: int = Field(default=30, ge=7, le=180)
    max_backtest_runs: int = Field(default=24, ge=8, le=80)


class AutonomousResearchLoop:
    def __init__(self, app):
        self.app = app
        self.path = Path(app.state.ai_research_dir) / "autonomous_loop.json"
        self.lock = asyncio.Lock()
        self.revision = 0
        self.state = {"config": ResearchLoopConfig().model_dump(), "rounds": [], "status": "paused"}
        if self.path.exists():
            # A corrupt budget/config must fail closed rather than reset the budget.
            self.state = json.loads(self.path.read_text(encoding="utf-8"))
            ResearchLoopConfig.model_validate(self.state["config"])
            if self.state.get("status") == "generating":
                self.state["status"] = "interrupted"
                self.state["last_error"] = "Previous generation interrupted; its daily attempt remains counted."

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def status(self):
        today = datetime.now(timezone.utc).date().isoformat()
        return {
            **self.state,
            "rounds": list(self.state.get("rounds", []))[-10:],
            "attempts_today": sum(r["started_at"].startswith(today) for r in self.state.get("rounds", [])),
            "configured_model": settings.AI_RESEARCH_MODEL,
            "scope": "LLM hypotheses -> bounded backtests -> candidate feedback; no automatic deployment",
        }

    def configure(self, config: ResearchLoopConfig):
        self.revision += 1
        self.state["config"] = config.model_dump()
        self.state["status"] = "waiting" if config.enabled else "paused"
        self._save()
        return self.status()

    async def _market_context(self, symbol, timeframe, days):
        from core.data import data_storage
        frame = await data_storage.load_klines_from_parquet(
            exchange="binance", symbol=symbol, timeframe=timeframe,
            start_time=datetime.now(timezone.utc) - timedelta(days=days),
            end_time=datetime.now(timezone.utc),
        )
        if frame is None or frame.empty:
            return {"symbol": symbol, "timeframe": timeframe, "available": False}
        import pandas as pd
        ts = pd.Timestamp(frame.index[-1])
        if "timestamp" in frame.columns:
            ts = pd.Timestamp(frame["timestamp"].iloc[-1])
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        age = (pd.Timestamp.now(tz="UTC") - ts).total_seconds()
        close = frame["close"].astype(float)
        return {"symbol": symbol, "timeframe": timeframe, "available": True,
                "bars": len(frame), "data_end": ts.isoformat(), "age_sec": age,
                "last_close": float(close.iloc[-1]),
                "window_return": float(close.iloc[-1] / close.iloc[0] - 1),
                "bar_volatility": float(close.pct_change().std())}

    async def tick(self):
        if self.lock.locked():
            return
        async with self.lock:
            cfg = ResearchLoopConfig.model_validate(self.state["config"])
            if not cfg.enabled:
                return
            now = datetime.now(timezone.utc)
            rounds = self.state.setdefault("rounds", [])
            if self.status()["attempts_today"] >= cfg.max_rounds_per_day:
                self.state["status"] = "daily_budget_reached"
                return
            due = self.state.get("next_run_at")
            if due and now < datetime.fromisoformat(due):
                return
            proposals = self.app.state.ai_proposal_registry.list(limit=None)
            if any(p.status in {"research_queued", "research_running"} for p in proposals) or any(
                j.get("status") in {"pending", "running"} for j in self.app.state.research_jobs.values()
            ):
                self.state["status"] = "waiting_for_research"
                return

            # Count before network work; failures and restarts cannot evade the daily cap.
            round_record = {"started_at": now.isoformat(), "status": "generating"}
            rounds.append(round_record)
            self.state["rounds"] = rounds[-200:]
            self.state["next_run_at"] = (now + timedelta(seconds=cfg.interval_seconds)).isoformat()
            self.state["status"] = "generating"
            self.state["last_error"] = None
            revision = self.revision
            self._save()
            try:
                from core.ai.research_context_generator import generate_research_context
                from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
                from core.deployment.promotion_engine import transition_proposal

                index = len(rounds) - 1
                symbol = cfg.symbols[index % len(cfg.symbols)]
                timeframe = cfg.timeframes[(index // len(cfg.symbols)) % len(cfg.timeframes)]
                market = await self._market_context(symbol, timeframe, cfg.days)
                if not market.get("available") or market.get("bars", 0) < 100:
                    raise ValueError("Market history unavailable or fewer than 100 bars; refresh historical data.")
                max_age = {"15m": 3600, "1h": 14400, "4h": 57600}[timeframe]
                if market.get("age_sec", float("inf")) > max_age:
                    raise ValueError("Market history is stale; refresh historical data before the next round.")
                feedback = [
                    {"candidate_id": c.candidate_id, "strategy": c.strategy, "symbol": c.symbol,
                     "score": c.score, "status": c.status,
                     "validation": c.validation_summary.model_dump(mode="json") if c.validation_summary else None}
                    for c in self.app.state.ai_candidate_registry.list(limit=12)
                ]
                context = {"market": market, "recent_candidates": feedback,
                           "recent_proposals": [{"thesis": p.thesis, "status": p.status, "templates": p.strategy_templates} for p in proposals[:8]],
                           "recent_rounds": [{"status": r.get("status"), "error": r.get("error")} for r in rounds[-5:-1]]}
                goal = "结合当前行情和前轮验证结果提出可证伪的新假设，探索动量、均值回归、突破、量价及事件机制，避免重复均线调参；只产出研究草案。"
                output = await asyncio.wait_for(generate_research_context(context, goals=goal, timeout=120), timeout=260)
                if not output or not output.get("hypothesis") or not output.get("proposed_strategy_changes"):
                    raise ValueError("LLM unavailable or invalid research output; no rule fallback was queued.")
                if revision != self.revision or not self.state["config"]["enabled"]:
                    round_record["status"] = "cancelled_before_queue"
                    return
                fingerprint = hashlib.sha256(json.dumps({
                    "symbol": symbol, "timeframe": timeframe,
                    "changes": [{k: d.get(k) for k in ("strategy", "program", "params")} for d in output["proposed_strategy_changes"]],
                }, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                if any(p.metadata.get("loop_fingerprint") == fingerprint for p in proposals):
                    raise ValueError("Duplicate research program; awaiting a new hypothesis next round.")
                planned = await asyncio.to_thread(generate_research_proposal, PlannerGenerateRequest(
                    goal=goal, symbols=[symbol], timeframes=[timeframe], market_regime="mixed",
                    constraints={"research_mode": "hybrid", "max_templates": 5,
                                 "max_strategy_drafts": 3, "max_backtest_runs": cfg.max_backtest_runs},
                    llm_research_output=output,
                    metadata={"autonomous_research_loop": True, "loop_fingerprint": fingerprint,
                              "llm_generation": output.get("_generation", {}), "loop_market": market,
                              "feedback_candidate_ids": [c["candidate_id"] for c in feedback]},
                ), "research_loop")
                if revision != self.revision or not self.state["config"]["enabled"]:
                    round_record["status"] = "cancelled_before_queue"
                    return
                proposal = planned.proposal
                if proposal.source != "ai":
                    raise ValueError("Planner rejected LLM output; no rule fallback was queued.")
                proposal.metadata["last_research_request"] = {"exchange": "binance", "symbol": symbol,
                                                              "days": cfg.days, "timeframes": [timeframe]}
                transition_proposal(proposal, to_state="research_queued",
                    lifecycle_registry=self.app.state.ai_lifecycle_registry, actor="research_loop",
                    reason="LLM research round queued for bounded backtest")
                self.app.state.ai_proposal_registry.save(proposal)
                round_record.update(status="queued", proposal_id=proposal.proposal_id,
                                    generation=output.get("_generation", {}))
                self.state["status"] = "waiting_for_research"
            except asyncio.CancelledError:
                round_record["status"] = "interrupted"
                self.state["status"] = "interrupted"
                raise
            except Exception as exc:
                round_record.update(status="failed", error=str(exc)[:500])
                self.state.update(status="retry_scheduled", last_error=str(exc)[:500])
            finally:
                self._save()


def get_research_loop(app):
    from core.research.orchestrator import ensure_ai_research_runtime_state
    ensure_ai_research_runtime_state(app)
    if getattr(app.state, "autonomous_research_loop", None) is None:
        app.state.autonomous_research_loop = AutonomousResearchLoop(app)
    return app.state.autonomous_research_loop
