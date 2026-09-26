"""Research loop v2: the LLM proposes cross-sectional feature formulas.

Why v2 (docs/LLM_TRADING_RESEARCH_ROUND2_2026-09-25.md): v1 backtests LLM
strategies on 30 days of BTC/ETH 1h (median 4 trades per candidate), so its
"validated" picks were noise, and re-testing all 85 of its programs on 50
coins x 5 months found none with holdout skill. The only edge this repo has
validated is the weekly cross-sectional pump model over ~110 alts, so v2
searches there, with the statistics that survived earlier audits:

* the LLM writes formulas in a closed JSON DSL (core/research/xs_feature_dsl.py);
  nothing it returns is executed as code;
* it sees only anonymized panel facts and its own DEVELOPMENT-period results;
  holdout numbers and gate verdicts are never shown to it, so it cannot fit
  the holdout over many rounds (the model is also trained on 2025-2026 data
  and could otherwise recall which named coins pumped);
* every distinct formula ever evaluated enters a trial ledger, and the
  holdout z bar is Bonferroni over the ledger size;
* passing formulas are frozen and judged again only on weekly data collected
  after freezing (data/research/xs_panel/weekly_archive).

Research only: never registers strategies, places orders or edits settings.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from core.research import xs_panel
from core.research.xs_evaluation import baseline_scores, bonferroni_z, evaluate_formula_on_panel
from core.research.xs_feature_dsl import FormulaError, describe_grammar, fingerprint, validate_formula

VALIDATED_DRIVERS = (
    "high 30d volatility, high open-interest/market-cap, slightly positive funding, "
    "price near its all-time high, strong 30d momentum, small market cap, young listing, "
    "a prior 2x pump within 120 days"
)

SYSTEM_PROMPT = """You are a quantitative researcher designing cross-sectional features for altcoins.
Each Monday, coins are ranked by your feature; the question is whether the top 20% are more likely
to at least double within the next 30 days than the universe. You never see coin names or dates.
Reply with one JSON object:
{"hypothesis": "<one sentence>", "formulas": [{"name": "<snake_case>", "direction": "high"|"low",
 "thesis": "<why this should carry information the known drivers do not>", "expr": <expression>}]}
Propose falsifiable ideas. Do not claim returns. Do not repeat a formula already evaluated."""


class CrossSectionalLoopConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    interval_seconds: int = Field(default=21600, ge=1800, le=86400)
    max_rounds_per_day: int = Field(default=4, ge=1, le=12)
    formulas_per_round: int = Field(default=3, ge=1, le=5)
    n_perm: int = Field(default=200, ge=50, le=1000)
    model_timeout_seconds: int = Field(default=180, ge=30, le=300)
    min_coverage: float = Field(default=0.6, ge=0.3, le=1.0)
    min_dev_z: float = Field(default=1.645, ge=0.0, le=5.0)
    min_holdout_shortlist_lift: float = Field(default=1.2, ge=1.0, le=3.0)
    min_lift_ex_top3: float = Field(default=1.3, ge=1.0, le=5.0)
    forward_min_weeks: int = Field(default=8, ge=4, le=52)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CrossSectionalResearchLoop:
    def __init__(self, app):
        self.app = app
        self.lock = asyncio.Lock()
        self.task: Optional[asyncio.Task] = None
        self.path = Path(app.state.ai_research_dir) / "cross_sectional_loop.json"
        self.state: Dict[str, Any] = {
            "config": CrossSectionalLoopConfig().model_dump(),
            "status": "paused",
            "rounds": [],
            "ledger": {},
            "frozen": [],
        }
        if self.path.exists():
            self.state.update(json.loads(self.path.read_text(encoding="utf-8")))
            self.state["config"] = CrossSectionalLoopConfig.model_validate(self.state["config"]).model_dump()
            if self.state.get("status") in {"generating", "evaluating", "observing"}:
                self.state.update(status="interrupted", last_error="上次工作中断；台账与已完成结果保留")
        self._panel: Optional[pd.DataFrame] = None
        self._baseline: Optional[pd.DataFrame] = None

    # ── persistence / status ──
    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f"{self.path.name}.{uuid4().hex}.tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2, allow_nan=False, default=str), encoding="utf-8")
        tmp.replace(self.path)

    def _attempts_today(self) -> int:
        today = _utc_now().date().isoformat()
        return sum(str(r.get("started_at", "")).startswith(today) for r in self.state["rounds"])

    def status(self) -> Dict[str, Any]:
        ledger = self.state["ledger"]
        payload = json.loads(json.dumps({k: v for k, v in self.state.items() if k != "ledger"}, default=str))
        ranked = sorted(ledger.values(), key=lambda e: (e.get("holdout") or {}).get("z") or -99, reverse=True)
        payload.update(
            rounds=payload["rounds"][-10:],
            ledger_size=len(ledger),
            holdout_z_bar=bonferroni_z(max(1, len(ledger))),
            attempts_today=self._attempts_today(),
            busy=self.lock.locked(),
            top_formulas=ranked[:10],
            scope="LLM 特征公式 → 周度横截面（开发期反馈给模型，留出期闸门对模型隐藏）→ 冻结后新增周度数据终审；不注册、不下单",
        )
        return payload

    def configure(self, config: CrossSectionalLoopConfig) -> Dict[str, Any]:
        self.state.update(config=config.model_dump(), status="waiting" if config.enabled else "paused")
        self._save()
        return self.status()

    def request_run(self) -> Dict[str, Any]:
        if self.lock.locked() or (self.task and not self.task.done()):
            return {**self.status(), "requested": False}
        self.task = asyncio.create_task(self.tick(force=True), name="ai_research_loop_v2_manual")
        return {**self.status(), "requested": True}

    async def stop(self) -> None:
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    # ── data ──
    def _load_research_data(self):
        if self._panel is None:
            from core.research.pump_precursor import load_model_weights

            panel = xs_panel.load_research_panel()
            self._baseline = baseline_scores(panel, load_model_weights())
            self._panel = panel
        return self._panel, self._baseline

    def _periods(self, panel: pd.DataFrame) -> Dict[str, tuple]:
        return {"dev": (None, xs_panel.DEV_END), "holdout": (xs_panel.DEV_END, panel["date"].max())}

    # ── prompt ──
    def _prompt(self, panel: pd.DataFrame, cfg: CrossSectionalLoopConfig) -> str:
        # Development-period feedback only. Holdout metrics and gate verdicts stay hidden.
        recent = sorted(self.state["ledger"].values(), key=lambda e: e.get("evaluated_at", ""))[-12:]
        feedback = [
            {
                "name": e["name"],
                "direction": e["direction"],
                "expr": e["expr"],
                "dev": {k: (e.get("dev") or {}).get(k) for k in ("lift", "z", "coverage", "corr_with_baseline", "shortlist_lift", "shortlist_z")},
            }
            for e in recent
        ]
        return json.dumps(
            {
                "task": f"Propose {cfg.formulas_per_round} new feature formulas.",
                "grammar": describe_grammar(),
                "panel": xs_panel.anonymized_summary(panel),
                "already_known_drivers": VALIDATED_DRIVERS,
                "goal": (
                    "Find information the known drivers do NOT already carry. 'shortlist_lift' is the key number: inside "
                    "the existing model's top 40% of coins, how much more often your feature's top half pumps (1.0 = no "
                    "new information); 'shortlist_z' is its significance. High 'corr_with_baseline' means you rediscovered "
                    "a known driver. z values are against a coin-shuffled null (about 2 is weak, 3+ is strong)."
                ),
                "your_previous_formulas_development_results": feedback,
            },
            ensure_ascii=False,
        )

    # ── evaluation ──
    def _gate(self, dev: Dict[str, Any], holdout: Dict[str, Any], n_trials: int, cfg: CrossSectionalLoopConfig) -> List[str]:
        reasons = []
        if dev.get("status") != "ok" or holdout.get("status") != "ok":
            return ["insufficient_sample"]
        if min(dev.get("coverage") or 0, holdout.get("coverage") or 0) < cfg.min_coverage:
            reasons.append("low_coverage")
        if (dev.get("z") or -99) < cfg.min_dev_z:
            reasons.append("no_development_signal")
        if (holdout.get("z") or -99) < 1.645:
            reasons.append("no_holdout_signal")
        # The claim that matters is NEW information, so the multiplicity bar applies
        # to the shortlist test, not to the (easily volatility-driven) standalone z.
        if (holdout.get("shortlist_z") or -99) < bonferroni_z(n_trials):
            reasons.append("shortlist_below_multiplicity_bar")
        if (holdout.get("shortlist_lift") or 0) < cfg.min_holdout_shortlist_lift:
            reasons.append("no_gain_over_existing_model")
        if (holdout.get("lift_ex_top3_coins") or 0) < cfg.min_lift_ex_top3:
            reasons.append("carried_by_few_coins")
        return reasons

    def _evaluate(self, formulas: List[Dict[str, Any]], cfg: CrossSectionalLoopConfig) -> List[Dict[str, Any]]:
        panel, baseline = self._load_research_data()
        periods = self._periods(panel)
        out = []
        for formula in formulas:
            metrics = evaluate_formula_on_panel(formula, panel, periods=periods, n_perm=cfg.n_perm, baseline=baseline)
            out.append({**formula, "fingerprint": fingerprint(formula), **metrics})
        return out

    async def _observe_forward(self, cfg: CrossSectionalLoopConfig) -> None:
        """Judge frozen formulas on weekly data collected after they were frozen."""
        pending = [f for f in self.state["frozen"] if f.get("status") == "forward_observing"]
        if not pending:
            return

        def work():
            daily = xs_panel.stitch_weekly_archive()
            if daily.empty:
                return None, None
            from core.research.pump_precursor import load_model_weights

            daily = xs_panel.add_forward_labels(daily)
            return daily, baseline_scores(daily, load_model_weights())

        daily, baseline = await asyncio.to_thread(work)
        now = _utc_now().isoformat()
        for item in pending:
            item["forward_checked_at"] = now
            if daily is None:
                item["forward_note"] = "尚无周度归档数据（generate_pump_watchlist 每周一写入）"
                continue
            metrics = await asyncio.to_thread(
                evaluate_formula_on_panel, item, daily,
                periods={"forward": (pd.Timestamp(item["frozen_at"]).tz_localize(None), None)},
                n_perm=cfg.n_perm, baseline=baseline,
            )
            forward = metrics["forward"]
            item["forward"] = forward
            if forward.get("status") != "ok" or (forward.get("dates") or 0) < cfg.forward_min_weeks:
                item["forward_note"] = f"等待成熟标签：{forward.get('dates') or 0}/{cfg.forward_min_weeks} 周"
                continue
            passed = (
                (forward.get("shortlist_z") or -99) >= 1.645
                and (forward.get("shortlist_lift") or 0) >= cfg.min_holdout_shortlist_lift
                and (forward.get("lift") or 0) >= 1.3
            )
            item["status"] = "forward_passed" if passed else "forward_failed"
            item["forward_note"] = "冻结后新增数据确认，仍需人工决定是否纳入周度模型" if passed else "冻结后新增数据未确认"

    # ── main tick ──
    async def tick(self, force: bool = False) -> None:
        if self.lock.locked():
            return
        async with self.lock:
            cfg = CrossSectionalLoopConfig.model_validate(self.state["config"])
            if not cfg.enabled and not force:
                return
            record: Optional[Dict[str, Any]] = None
            try:
                self.state["status"] = "observing"
                await self._observe_forward(cfg)
                now = _utc_now()
                if self._attempts_today() >= cfg.max_rounds_per_day:
                    self.state["status"] = "daily_budget_reached"
                    return
                due = self.state.get("next_run_at")
                if due and now < datetime.fromisoformat(due) and not force:
                    self.state["status"] = "waiting"
                    return
                panel, _ = await asyncio.to_thread(self._load_research_data)
                record = {"round_id": uuid4().hex[:12], "started_at": now.isoformat(), "status": "generating"}
                self.state["rounds"] = (self.state["rounds"] + [record])[-200:]
                self.state.update(status="generating", last_error=None,
                                  next_run_at=(now + timedelta(seconds=cfg.interval_seconds)).isoformat())
                self._save()

                from core.ai.research_context_generator import generate_json

                output = await asyncio.wait_for(
                    generate_json(self._prompt(panel, cfg), system_prompt=SYSTEM_PROMPT, timeout=cfg.model_timeout_seconds),
                    timeout=cfg.model_timeout_seconds * 2 + 30,
                )
                formulas, invalid, duplicates = [], [], []
                for raw in list(output.get("formulas") or [])[: cfg.formulas_per_round]:
                    try:
                        formula = validate_formula(raw)
                    except FormulaError as exc:
                        invalid.append({"name": str((raw or {}).get("name") if isinstance(raw, dict) else "?")[:80], "error": str(exc)})
                        continue
                    if fingerprint(formula) in self.state["ledger"] or any(fingerprint(f) == fingerprint(formula) for f in formulas):
                        duplicates.append(formula["name"])
                        continue
                    formulas.append(formula)
                record.update(status="evaluating", hypothesis=str(output.get("hypothesis") or "")[:400],
                              invalid=invalid, duplicates=duplicates, generation=output.get("_generation", {}))
                self.state["status"] = "evaluating"
                self._save()

                results = await asyncio.to_thread(self._evaluate, formulas, cfg)
                frozen_ids = []
                for row in results:
                    n_trials = len(self.state["ledger"]) + 1
                    reasons = self._gate(row["dev"], row["holdout"], n_trials, cfg)
                    entry = {
                        "fingerprint": row["fingerprint"], "name": row["name"], "direction": row["direction"],
                        "thesis": row["thesis"], "expr": row["expr"], "round_id": record["round_id"],
                        "evaluated_at": _utc_now().isoformat(), "trial_number": n_trials,
                        "holdout_z_bar": bonferroni_z(n_trials), "dev": row["dev"], "holdout": row["holdout"],
                        "decision": "frozen" if not reasons else "rejected", "reasons": reasons,
                    }
                    self.state["ledger"][row["fingerprint"]] = entry
                    if not reasons:
                        frozen_ids.append(row["fingerprint"])
                        self.state["frozen"].append({
                            **{k: entry[k] for k in ("fingerprint", "name", "direction", "thesis", "expr", "dev", "holdout")},
                            "frozen_at": _utc_now().isoformat(), "status": "forward_observing",
                            "manual_review_required": True,
                        })
                record.update(status="completed", finished_at=_utc_now().isoformat(),
                              evaluated=[r["fingerprint"] for r in results], frozen=frozen_ids)
                self.state["status"] = "waiting"
                logger.info(f"research loop v2 round {record['round_id']}: evaluated {len(results)}, frozen {len(frozen_ids)}, ledger {len(self.state['ledger'])}")
            except asyncio.CancelledError:
                if record:
                    record.update(status="interrupted")
                self.state["status"] = "interrupted"
                raise
            except Exception as exc:
                code = getattr(exc, "code", "research_timeout" if isinstance(exc, asyncio.TimeoutError) else "implementation_error")
                message = str(exc)[:500] if getattr(exc, "code", None) else f"研究执行异常（{type(exc).__name__}）"
                if record:
                    record.update(status="failed", error=message, error_code=code)
                self.state.update(status="retry_scheduled", last_error=message, last_error_code=code,
                                  next_run_at=(_utc_now() + timedelta(seconds=cfg.interval_seconds)).isoformat())
                logger.warning(f"research loop v2 failed: {code}: {message}")
            finally:
                if not self.state["config"]["enabled"] and self.state["status"] not in {"interrupted"}:
                    self.state["status"] = "paused"
                self._save()


def get_cross_sectional_loop(app) -> CrossSectionalResearchLoop:
    from core.research.orchestrator import ensure_ai_research_runtime_state

    ensure_ai_research_runtime_state(app)
    if getattr(app.state, "cross_sectional_research_loop", None) is None:
        app.state.cross_sectional_research_loop = CrossSectionalResearchLoop(app)
    return app.state.cross_sectional_research_loop
