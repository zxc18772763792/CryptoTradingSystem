"""Research loop v3: the LLM proposes factor formulas over the full local panel.

Why (docs/RESEARCH_LOOP_V3.md): v2 asks one question (which of ~107 alts double within 30 days)
of one year of five columns; after 156 formulas none had passed its gates and its rounds were
repeating ideas. Since 2026-10-15 the local stores hold every Binance USDT perp since 2020 with
the Coinglass datasets, so v3 asks the question the forward trackers trade: does a formula rank
the next 7 days' returns of the ~150 most liquid perps, beyond the known factors, after costs?

Same contract as v2 (core/ai/research_loop_v2.py):
* the LLM writes formulas in the closed JSON DSL (core/research/xs_feature_dsl.py, with the panel's
  columns); nothing it returns is executed as code;
* it sees anonymized panel facts and only its DEVELOPMENT results (2020-07..2023-12); holdout
  results (2024-01..) and gate verdicts are never shown to it;
* every distinct formula enters a trial ledger, and the holdout bar on the residual IC (the claim
  of new information) is Bonferroni over the ledger size;
* passing formulas are frozen and judged again only on days after freezing, as the local stores
  keep growing (Binance daily, Coinglass weekly).

The work (a ~150 MB panel, permutation nulls) runs in its own process, scripts/research_loop_v3.py,
which the research scheduler starts when a round is due; the web process only reads the state
file and writes the configuration. Research only: never registers strategies, places orders or
edits settings.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data" / "research" / "ai" / "research_loop_v3.json"
LOCK_PATH = PROJECT_ROOT / "data" / "research" / "ai" / "research_loop_v3.lock"
LOG_PATH = PROJECT_ROOT / "logs" / "research_loop_v3.log"
SCRIPT = PROJECT_ROOT / "scripts" / "research_loop_v3.py"
LOCK_STALE = timedelta(minutes=90)
FEEDBACK_ROWS = 15

SYSTEM_PROMPT = """You are a quantitative researcher designing cross-sectional factors for crypto perpetual futures.
Every day the ~150 most liquid USDT perpetuals are ranked by your formula. The question is whether the ranking
orders the NEXT 7 DAYS' returns beyond factors already known, and whether a book long the top fifth and short the
bottom fifth still earns after 0.12% round-trip costs and funding. You never see coin names or dates.
Reply with one JSON object:
{"hypothesis": "<one sentence>", "formulas": [{"name": "<snake_case>", "direction": "high"|"low",
 "thesis": "<why this should carry information the known factors do not>", "expr": <expression>}]}
"direction": "high" means the coins with the largest values should outperform. Propose falsifiable ideas.
Do not claim returns. Do not repeat a formula already evaluated."""

GOAL = (
    "Find information the known factors do NOT already carry. Metrics (development period only): 'ic' = mean daily "
    "rank correlation with the next 7 days' returns; 'z' = its significance against a null where every coin gets "
    "another coin's whole feature history (about 2 is weak, 3+ strong); 'resid_ic' / 'resid_z' = the same after "
    "removing the known factors from your ranking each day - the key numbers; 'nearest_factor' and its correlation "
    "show when you rediscovered a known factor; 'net_per_book' = the 7-day top-minus-bottom fifth book after costs "
    "and funding (a few coins that pump while in the short leg can sink a book even when ic > 0); 'coverage' = share "
    "of the ranked coins your formula has a value for. Values are compared across coins, so scale-free expressions "
    "(ratios, z-scores, percent changes, ts_rank) usually work better than raw USD levels."
)


class ResearchLoopV3Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    interval_seconds: int = Field(default=21600, ge=1800, le=86400)
    max_rounds_per_day: int = Field(default=4, ge=1, le=12)
    formulas_per_round: int = Field(default=3, ge=1, le=5)
    n_perm: int = Field(default=200, ge=50, le=1000)
    model_timeout_seconds: int = Field(default=180, ge=30, le=300)
    min_coverage: float = Field(default=0.5, ge=0.2, le=1.0)
    min_dev_z: float = Field(default=1.645, ge=0.0, le=5.0)
    min_holdout_z: float = Field(default=1.645, ge=0.0, le=5.0)
    min_holdout_net_per_book: float = Field(default=0.0, ge=-0.01, le=0.05)
    forward_min_days: int = Field(default=182, ge=28, le=730)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------------------------------ state
def _empty_state() -> Dict[str, Any]:
    return {"config": ResearchLoopV3Config().model_dump(), "status": "paused", "rounds": [], "ledger": {}, "frozen": []}


def load_state(path: Optional[Path] = None) -> Dict[str, Any]:
    path = path or STATE_PATH
    state = _empty_state()
    if path.exists():
        state.update(json.loads(path.read_text(encoding="utf-8")))
        state["config"] = ResearchLoopV3Config.model_validate(state["config"]).model_dump()
    return state


def update_state(change: Callable[[Dict[str, Any]], None], path: Optional[Path] = None) -> Dict[str, Any]:
    """Read, change and atomically write the state; each side changes only the keys it owns."""
    path = path or STATE_PATH
    state = load_state(path)
    change(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, allow_nan=False, default=str), encoding="utf-8")
    tmp.replace(path)
    return state


def attempts_today(state: Dict[str, Any], now: Optional[datetime] = None) -> int:
    today = (now or _utc_now()).date().isoformat()
    return sum(str(r.get("started_at", "")).startswith(today) for r in state["rounds"])


def lock_age(path: Optional[Path] = None) -> Optional[timedelta]:
    path = path or LOCK_PATH
    if not path.exists():
        return None
    return _utc_now() - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def is_running(path: Optional[Path] = None) -> bool:
    age = lock_age(path)
    return age is not None and age < LOCK_STALE


# ------------------------------------------------------------------------------------------ gate
def gate(dev: Dict[str, Any], holdout: Dict[str, Any], n_trials: int, cfg: ResearchLoopV3Config) -> List[str]:
    from core.research.xs_return_evaluation import bonferroni_z

    if dev.get("status") != "ok" or holdout.get("status") != "ok":
        return ["insufficient_sample"]
    reasons = []
    if min(dev.get("coverage") or 0, holdout.get("coverage") or 0) < cfg.min_coverage:
        reasons.append("low_coverage")
    if (dev.get("z") or -99) < cfg.min_dev_z:
        reasons.append("no_development_signal")
    if (holdout.get("z") or -99) < cfg.min_holdout_z:
        reasons.append("no_holdout_signal")
    # the claim that matters is NEW information: the multiplicity bar applies to the residual IC
    if (holdout.get("resid_z") or -99) < bonferroni_z(n_trials):
        reasons.append("resid_below_multiplicity_bar")
    net = holdout.get("net_per_book")
    if net is None or net <= cfg.min_holdout_net_per_book:
        reasons.append("no_net_edge_after_costs")
    ex3 = holdout.get("net_per_book_ex_top3")
    if ex3 is None or ex3 <= cfg.min_holdout_net_per_book:
        reasons.append("carried_by_few_coins")
    return reasons


# ------------------------------------------------------------------------------------------ prompt
def build_prompt(state: Dict[str, Any], panel, cfg: ResearchLoopV3Config) -> str:
    from core.research.xs_daily_panel import COLUMNS, FACTORS, anonymized_summary
    from core.research.xs_feature_dsl import describe_grammar
    from core.research.xs_return_evaluation import DEV_END

    ledger = sorted(state["ledger"].values(), key=lambda e: e.get("evaluated_at", ""))
    dev_keys = ("ic", "z", "resid_ic", "resid_z", "nearest_factor", "nearest_factor_corr", "net_per_book", "coverage")
    feedback = [
        {"name": e["name"], "direction": e["direction"], "expr": e["expr"],
         "dev": {k: (e.get("dev") or {}).get(k) for k in dev_keys}}
        for e in ledger[-FEEDBACK_ROWS:]
    ]
    return json.dumps(
        {
            "task": f"Propose {cfg.formulas_per_round} new factor formulas.",
            "grammar": describe_grammar(COLUMNS),
            "panel": anonymized_summary(panel, end=DEV_END),
            "known_factors": FACTORS,
            "goal": GOAL,
            "names_already_tried": [e["name"] for e in ledger[:-FEEDBACK_ROWS]][-150:],
            "your_previous_formulas_development_results": feedback,
        },
        ensure_ascii=False,
    )


# ------------------------------------------------------------------------------------------ round
def run_round(*, force: bool = False, log: Callable[[str], None] = print, generate=None) -> Dict[str, Any]:
    """One round in this process: generate, evaluate, gate, record. Holds LOCK_PATH meanwhile."""
    state = load_state()
    cfg = ResearchLoopV3Config.model_validate(state["config"])
    if not force and not cfg.enabled:
        return {"skipped": "disabled"}
    if not force and attempts_today(state) >= cfg.max_rounds_per_day:
        return {"skipped": "daily_budget_reached"}
    if not _acquire_lock():
        return {"skipped": "another_round_running"}
    record: Dict[str, Any] = {"round_id": uuid4().hex[:12], "started_at": _utc_now().isoformat(), "status": "loading"}
    try:
        def start(s: Dict[str, Any]) -> None:
            s["rounds"] = (s["rounds"] + [record])[-300:]
            s.update(status="loading", last_error=None)

        update_state(start)
        from core.research.xs_daily_panel import COLUMNS, load_panel
        from core.research.xs_feature_dsl import FormulaError, fingerprint, validate_formula
        from core.research.xs_return_evaluation import EvalContext, bonferroni_z, evaluate

        panel = load_panel(log=log)
        ctx = EvalContext(panel)
        _set_round(record, status="observing", panel=panel.meta.get("end"))
        observe_forward(ctx, cfg, log=log)

        _set_round(record, status="generating")
        if generate is None:
            from core.ai.research_context_generator import generate_json

            async def generate(prompt: str) -> Dict[str, Any]:
                return await generate_json(prompt, system_prompt=SYSTEM_PROMPT, timeout=cfg.model_timeout_seconds)
        state = load_state()
        output = asyncio.run(asyncio.wait_for(generate(build_prompt(state, panel, cfg)), cfg.model_timeout_seconds * 2 + 30))
        formulas, invalid, duplicates = [], [], []
        for raw in list(output.get("formulas") or [])[: cfg.formulas_per_round]:
            try:
                formula = validate_formula(raw, columns=COLUMNS)
            except FormulaError as exc:
                invalid.append({"name": str((raw or {}).get("name") if isinstance(raw, dict) else "?")[:80], "error": str(exc)})
                continue
            fp = fingerprint(formula)
            if fp in state["ledger"] or any(fingerprint(f) == fp for f in formulas):
                duplicates.append(formula["name"])
                continue
            formulas.append(formula)
        _set_round(record, status="evaluating", hypothesis=str(output.get("hypothesis") or "")[:400], invalid=invalid,
                   duplicates=duplicates, generation=output.get("_generation", {}))

        evaluated, frozen = [], []
        for formula in formulas:
            _touch_lock()
            metrics = evaluate(formula, ctx, n_perm=cfg.n_perm, seed=len(state["ledger"]))
            fp = fingerprint(formula)

            def add(s: Dict[str, Any], formula=formula, metrics=metrics, fp=fp) -> None:
                n_trials = len(s["ledger"]) + 1
                reasons = gate(metrics["dev"], metrics["holdout"], n_trials, cfg)
                entry = {
                    "fingerprint": fp, "name": formula["name"], "direction": formula["direction"],
                    "thesis": formula["thesis"], "expr": formula["expr"], "round_id": record["round_id"],
                    "evaluated_at": _utc_now().isoformat(), "trial_number": n_trials,
                    "resid_z_bar": bonferroni_z(n_trials), "dev": metrics["dev"], "holdout": metrics["holdout"],
                    "decision": "frozen" if not reasons else "rejected", "reasons": reasons,
                }
                s["ledger"][fp] = entry
                if not reasons:
                    frozen.append(fp)
                    s["frozen"].append({**{k: entry[k] for k in ("fingerprint", "name", "direction", "thesis", "expr", "dev", "holdout")},
                                        "frozen_at": _utc_now().isoformat(), "frozen_panel_end": panel.meta.get("end"),
                                        "status": "forward_observing", "manual_review_required": True})

            state = update_state(add)
            evaluated.append(fp)
            log(f"evaluated {formula['name']}: {state['ledger'][fp]['decision']} {state['ledger'][fp]['reasons']}")
        _set_round(record, status="completed", finished_at=_utc_now().isoformat(), evaluated=evaluated, frozen=frozen)
        update_state(lambda s: s.update(status="waiting" if s["config"]["enabled"] else "paused"))
        return record
    except Exception as exc:  # noqa: BLE001 - recorded on the round; the scheduler retries next interval
        code = getattr(exc, "code", "research_timeout" if isinstance(exc, asyncio.TimeoutError) else "implementation_error")
        message = str(exc)[:500] if getattr(exc, "code", None) else f"{type(exc).__name__}: {str(exc)[:300]}"
        _set_round(record, status="failed", error=message, error_code=code, finished_at=_utc_now().isoformat())
        update_state(lambda s: s.update(status="retry_scheduled", last_error=message, last_error_code=code))
        log(f"round failed: {code}: {message}")
        return record
    finally:
        _release_lock()


def _set_round(record: Dict[str, Any], **fields: Any) -> None:
    record.update(fields)

    def change(s: Dict[str, Any]) -> None:
        for i, r in enumerate(s["rounds"]):
            if r.get("round_id") == record["round_id"]:
                s["rounds"][i] = dict(record)
        if fields.get("status") in {"observing", "generating", "evaluating"}:
            s["status"] = fields["status"]

    update_state(change)
    _touch_lock()


def observe_forward(ctx, cfg: ResearchLoopV3Config, log: Callable[[str], None] = print) -> None:
    """Judge frozen formulas on the days after they were frozen."""
    import pandas as pd

    from core.research.xs_return_evaluation import EvalContext, evaluate

    state = load_state()
    pending = [f for f in state["frozen"] if f.get("status") == "forward_observing"]
    if not pending:
        return
    results = {}
    for item in pending:
        start = pd.Timestamp(item.get("frozen_panel_end") or item["frozen_at"][:10]) + pd.Timedelta(days=1)
        end = ctx.dates[(ctx.label_ok.sum(axis=1) >= 30)].max()
        days = int((end - start).days) + 1
        if days < cfg.forward_min_days:
            results[item["fingerprint"]] = {"forward_note": f"等待前向数据：{max(0, days)}/{cfg.forward_min_days} 天", "forward": None}
            continue
        fctx = EvalContext(ctx.panel, periods={"forward": (start, end)})
        forward = evaluate(item, fctx, n_perm=cfg.n_perm)["forward"]
        passed = (forward.get("z") or -99) >= 1.645 and (forward.get("net_per_book") or 0) > 0
        results[item["fingerprint"]] = {
            "forward": forward, "status": "forward_passed" if passed else "forward_failed",
            "forward_note": "冻结后新数据确认，是否建前向跟踪器需人工决定" if passed else "冻结后新数据未确认",
        }
        log(f"forward {item['name']}: {results[item['fingerprint']]['status']}")

    def change(s: Dict[str, Any]) -> None:
        for item in s["frozen"]:
            if item["fingerprint"] in results:
                item.update({k: v for k, v in results[item["fingerprint"]].items() if v is not None or k == "forward"})
                item["forward_checked_at"] = _utc_now().isoformat()

    update_state(change)


def _acquire_lock() -> bool:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists() and not is_running():
        LOCK_PATH.unlink(missing_ok=True)  # a crashed round's lock
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"pid": os.getpid(), "started_at": _utc_now().isoformat()}))
    return True


def _touch_lock() -> None:
    if LOCK_PATH.exists():
        os.utime(LOCK_PATH, None)


def _release_lock() -> None:
    try:
        data = json.loads(LOCK_PATH.read_text(encoding="utf-8"))
        if int(data.get("pid") or -1) == os.getpid():
            LOCK_PATH.unlink(missing_ok=True)
    except (OSError, ValueError):
        pass


# ------------------------------------------------------------------------------------------ web side
def status() -> Dict[str, Any]:
    from core.research.xs_return_evaluation import bonferroni_z

    state = load_state()
    ledger = state["ledger"]
    ranked = sorted(ledger.values(), key=lambda e: (e.get("holdout") or {}).get("resid_z") or -99, reverse=True)
    payload = {k: v for k, v in state.items() if k != "ledger"}
    payload.update(
        rounds=state["rounds"][-10:],
        ledger_size=len(ledger),
        resid_z_bar=bonferroni_z(max(1, len(ledger) + 1)),
        attempts_today=attempts_today(state),
        running=is_running(),
        top_formulas=ranked[:10],
        scope="LLM 因子公式 → 约 150 个流动性最好的永续，预测未来 7 天收益（开发期 2020-07~2023-12 反馈给模型，"
              "2024 年起的留出期和闸门结论对模型隐藏）→ 冻结后用新增数据终审；不注册、不下单",
    )
    return payload


def configure(changes: Dict[str, Any]) -> Dict[str, Any]:
    def change(s: Dict[str, Any]) -> None:
        cfg = ResearchLoopV3Config.model_validate({**s["config"], **changes})
        s["config"] = cfg.model_dump()
        if s.get("status") in {None, "paused", "waiting"}:
            s["status"] = "waiting" if cfg.enabled else "paused"

    update_state(change)
    return status()


_process: Optional[asyncio.subprocess.Process] = None


async def start_round(force: bool = False) -> bool:
    """Start one round in its own process unless one is running; returns whether it started."""
    global _process
    if is_running() or (_process is not None and _process.returncode is None):
        return False
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    args = [sys.executable, str(SCRIPT), "--round"] + (["--force"] if force else [])
    kwargs: Dict[str, Any] = {}
    if os.name == "nt":
        import subprocess

        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    with open(LOG_PATH, "ab") as log_file:
        _process = await asyncio.create_subprocess_exec(*args, cwd=str(PROJECT_ROOT), stdout=log_file,
                                                         stderr=asyncio.subprocess.STDOUT, stdin=asyncio.subprocess.DEVNULL, **kwargs)
    logger.info(f"research loop v3: round started (pid {_process.pid})")
    return True


async def tick() -> None:
    """Called by the research scheduler: start a round when one is due."""
    state = await asyncio.to_thread(load_state)
    cfg = ResearchLoopV3Config.model_validate(state["config"])
    if not cfg.enabled or is_running():
        return
    now = _utc_now()
    if attempts_today(state, now) >= cfg.max_rounds_per_day:
        return
    due = state.get("next_run_at")
    if due and now < datetime.fromisoformat(due):
        return
    if await start_round():
        next_run = (now + timedelta(seconds=cfg.interval_seconds)).isoformat()
        await asyncio.to_thread(update_state, lambda s: s.update(next_run_at=next_run))
