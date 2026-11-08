from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from core.ai import research_loop_v3 as loop
from core.research import xs_daily_panel as xp
from core.research import xs_feature_dsl as dsl
from core.research import xs_return_evaluation as ev


def _frames(rows: int = 220, cols: int = 5, seed: int = 0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2021-01-01", periods=rows)
    names = [f"C{i}" for i in range(cols)]
    frames = {}
    for c in dsl.BASE_COLUMNS:
        a = np.exp(rng.normal(0, 0.05, (rows, cols)).cumsum(0)) * 10
        if c == "funding":
            a = rng.normal(0, 1e-4, (rows, cols))
            a[rng.random((rows, cols)) < 0.2] = 0.0  # ties
        a[:40, 1] = np.nan  # listed late
        a[180:, 2] = np.nan  # delisted
        a[rng.random((rows, cols)) < 0.03] = np.nan
        frames[c] = pd.DataFrame(a, index=idx, columns=names)
    return frames


def test_wide_evaluation_matches_the_per_coin_evaluator():
    frames = _frames()
    formulas = [{"name": op, "direction": "high", "expr": {"op": op, "window": w, "args": [{"col": "close"}]}}
                for op in sorted(dsl.WINDOW_OPS) for w in (1, 3, 30) if w > 1 or op in dsl.SHIFT_OPS]
    formulas += [
        {"name": "mix", "direction": "low", "expr": {"op": "div", "args": [
            {"op": "ts_rank", "window": 20, "args": [{"op": "sub", "args": [{"col": "oi"}, {"op": "lag", "window": 3, "args": [{"col": "oi"}]}]}]},
            {"op": "add", "args": [{"op": "abs", "args": [{"col": "funding"}]}, {"const": 0.5}]}]}},
        {"name": "signs", "direction": "high", "expr": {"op": "mean", "window": 7, "args": [{"op": "sign", "args": [{"col": "funding"}]}]}},
        {"name": "max2", "direction": "high", "expr": {"op": "max2", "args": [{"op": "log", "args": [{"col": "volume"}]},
                                                                               {"op": "zscore", "window": 10, "args": [{"col": "mcap"}]}]}},
    ]
    for raw in formulas:
        formula = dsl.validate_formula(raw)
        wide = dsl.evaluate_formula_wide(formula, frames)
        for coin in wide.columns:
            one = dsl.evaluate_formula(formula, pd.DataFrame({k: v[coin] for k, v in frames.items()}))
            a, b = one.to_numpy(), wide[coin].to_numpy()
            assert ((np.isnan(a) & np.isnan(b)) | np.isclose(a, b, rtol=1e-9, atol=1e-12)).all(), (raw["name"], coin)


def test_v3_formulas_use_the_panel_columns():
    raw = {"name": "liq_to_oi", "direction": "low", "expr": {"op": "div", "args": [{"col": "liq_long"}, {"col": "oi"}]}}
    formula = dsl.validate_formula(raw, columns=xp.COLUMNS)
    assert formula["expr"]["args"][0] == {"col": "liq_long"}
    with pytest.raises(dsl.FormulaError):
        dsl.validate_formula(raw)  # v2's five columns do not include liquidations
    with pytest.raises(dsl.FormulaError):
        dsl.validate_formula({"name": "x", "direction": "high", "expr": {"col": "price"}}, columns=xp.COLUMNS)
    assert "liq_long" in dsl.describe_grammar(xp.COLUMNS) and "liq_long" not in dsl.describe_grammar()


def _panel(days: int = 1700, coins: int = 48, seed: int = 3) -> xp.Panel:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2020-01-01", periods=days)
    names = [f"C{i:02d}USDT" for i in range(coins)]
    close = pd.DataFrame(np.exp(rng.normal(0, 0.04, (days, coins)).cumsum(0)) * 10, index=dates, columns=names)
    columns = {c: pd.DataFrame(np.exp(rng.normal(size=(days, coins))), index=dates, columns=names) for c in xp.COLUMNS}
    columns["close"] = close
    universe = pd.DataFrame(True, index=dates, columns=names)
    universe.iloc[:30] = False
    factors = {f: pd.DataFrame(rng.normal(size=(days, coins)), index=dates, columns=names) for f in xp.FACTORS}
    factors["rev21"] = close / close.ewm(span=21, adjust=False).mean() - 1
    return xp.Panel(columns=columns, universe=universe, fwd7=np.log(close.shift(-7)) - np.log(close),
                    fund7=pd.DataFrame(rng.normal(0, 1e-4, (days, coins)), index=dates, columns=names),
                    factors=factors, meta={"end": str(dates[-1].date())})


def test_a_planted_signal_passes_and_a_known_factor_adds_nothing():
    panel = _panel()
    ctx = ev.EvalContext(panel)
    fwd = panel.fwd7.to_numpy(dtype=float)
    rng = np.random.default_rng(11)
    planted = np.where(np.isfinite(fwd), fwd, 0.0) + 0.5 * np.nanstd(fwd) * rng.normal(size=fwd.shape)
    result = ev.evaluate({"name": "planted", "direction": "high", "expr": {"col": "close"}}, ctx, n_perm=40, feature=planted)
    for period in ("dev", "holdout"):
        assert result[period]["status"] == "ok"
        assert result[period]["z"] > 3 and result[period]["resid_z"] > 3
        assert result[period]["net_per_book"] > 0 and result[period]["net_per_book_ex_top3"] > 0
    copy = ev.evaluate({"name": "copy", "direction": "high", "expr": {"col": "close"}}, ctx, n_perm=40,
                       feature=panel.factors["rev21"].to_numpy(dtype=float))
    assert copy["holdout"]["resid_ic"] == 0.0 and copy["holdout"]["nearest_factor"] == "rev21"
    noise = rng.normal(size=fwd.shape)
    random = ev.evaluate({"name": "noise", "direction": "high", "expr": {"col": "close"}}, ctx, n_perm=40, feature=noise)
    assert abs(random["dev"]["z"]) < 3 and abs(random["holdout"]["z"]) < 3


def test_null_draws_rank_as_many_coins_as_the_observed_day():
    # Half the coins list late. A plain column permutation would hand many early receivers a donor
    # with no history yet and shrink the null's cross-section (23% of 2020-23 cells on the real panel).
    panel = _panel(days=900, coins=40)
    late = panel.coins[::2]
    for frame in (panel.universe, panel.fwd7):
        frame.loc[frame.index[:500], late] = False if frame is panel.universe else np.nan
    ctx = ev.EvalContext(panel)
    own = np.tile(np.arange(len(panel.coins), dtype=float), (len(panel.dates), 1))
    feature = np.where(panel.universe.to_numpy() | ~np.isin(panel.coins, late)[None, :], own, np.nan)
    needed = ctx.slot & np.isfinite(ctx.compact(feature))
    draw = ev.permuted_feature(ctx, feature, needed, np.random.default_rng(5))
    filled = needed & np.isfinite(draw)
    assert filled.sum() / needed.sum() > 0.99
    assert not (draw[filled] == ctx.compact(own)[filled]).any()  # never the coin's own values
    assert not np.isfinite(draw[~needed]).any()
    # once every coin is listed, each coin carries a single donor's trajectory (coin-level persistence)
    later = np.zeros_like(filled)
    later[500:] = True
    for coin in range(len(panel.coins)):
        assert len(np.unique(draw[(ctx.idx == coin) & filled & later])) == 1


def test_gate_requires_new_information_net_of_costs():
    cfg = loop.ResearchLoopV3Config()
    dev = {"status": "ok", "coverage": 0.9, "z": 2.5}
    good = {"status": "ok", "coverage": 0.9, "z": 2.5, "resid_z": 5.0, "net_per_book": 0.004, "net_per_book_ex_top3": 0.003}
    assert loop.gate(dev, good, 10, cfg) == []
    assert loop.gate(dev, {**good, "resid_z": 2.5}, 10, cfg) == ["resid_below_multiplicity_bar"]  # bar is ~2.58 at 10 trials
    assert loop.gate(dev, {**good, "net_per_book": -0.001}, 10, cfg) == ["no_net_edge_after_costs"]
    assert loop.gate(dev, {**good, "net_per_book_ex_top3": 0.0}, 10, cfg) == ["carried_by_few_coins"]
    assert loop.gate({**dev, "z": 1.0}, good, 10, cfg) == ["no_development_signal"]
    assert loop.gate(dev, {"status": "insufficient_sample"}, 10, cfg) == ["insufficient_sample"]


def test_a_round_shows_the_model_development_results_only(monkeypatch, tmp_path):
    monkeypatch.setattr(loop, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(loop, "LOCK_PATH", tmp_path / "round.lock")
    panel = _panel()
    monkeypatch.setattr(xp, "load_panel", lambda **kwargs: panel)
    loop.update_state(lambda s: s["config"].update(formulas_per_round=4, n_perm=50))
    prompts = []
    mom = {"name": "mom", "direction": "high", "expr": {"op": "pct_change", "window": 7, "args": [{"col": "close"}]}}

    def model(formulas):
        async def generate(prompt):
            prompts.append(prompt)
            return {"hypothesis": "test", "formulas": formulas}
        return generate

    assert loop.run_round(log=lambda m: None, generate=model([mom])) == {"skipped": "disabled"}
    record = loop.run_round(force=True, log=lambda m: None, generate=model([
        {"name": "liq_to_oi", "direction": "low", "expr": {"op": "div", "args": [{"col": "liq_long"}, {"col": "oi"}]}},
        mom, {**mom, "name": "mom_again"}, {"name": "bad", "direction": "high", "expr": {"col": "price"}},
    ]))
    assert record["status"] == "completed" and record["duplicates"] == ["mom_again"]
    assert [i["name"] for i in record["invalid"]] == ["bad"]
    state = loop.load_state()
    assert len(state["ledger"]) == 2 and all(e["decision"] in {"frozen", "rejected"} for e in state["ledger"].values())
    assert all(e["holdout"].get("status") for e in state["ledger"].values())
    assert not loop.LOCK_PATH.exists()

    record = loop.run_round(force=True, log=lambda m: None, generate=model([mom]))
    assert record["duplicates"] == ["mom"] and record["evaluated"] == []
    second = json.loads(prompts[-1])
    assert len(second["your_previous_formulas_development_results"]) == 2
    assert "holdout" not in prompts[-1]  # holdout results and verdicts never reach the model
    assert "liq_long" in second["grammar"]

    loop.LOCK_PATH.write_text("{}", encoding="utf-8")  # another round holds the lock
    assert loop.run_round(force=True, log=lambda m: None, generate=model([mom])) == {"skipped": "another_round_running"}


def test_the_prompt_rules_match_the_validator():
    example = {"name": "example", "direction": "low", "expr": loop.DEPTH_EXAMPLE}
    dsl.validate_formula(example, columns=xp.COLUMNS)
    assert loop.expression_shape(loop.DEPTH_EXAMPLE) == (dsl.MAX_DEPTH, 9)  # exactly at the limit, as the rules say
    with pytest.raises(dsl.FormulaError, match="deeper than"):
        dsl.validate_formula({**example, "expr": {"op": "neg", "args": [loop.DEPTH_EXAMPLE]}}, columns=xp.COLUMNS)
    rules = " ".join(loop.formula_rules())
    assert f"deeper than level {dsl.MAX_DEPTH}" in rules and f"at most {dsl.MAX_NODES} nodes" in rules
    two, one = next(s for s in loop.formula_rules() if s.startswith("Arguments:")).split(";")
    assert all(op in two for op in dsl.BINARY_OPS) and all(op in one for op in dsl.UNARY_OPS | dsl.WINDOW_OPS)
    assert "holdout" not in rules
    # the shape is measured on the formula as written, past the first violation and malformed nodes included
    assert loop.expression_shape({"op": "mean", "args": [{"col": "close"}, {"op": "oi"}]}) == (2, 3)
    assert loop.expression_shape("not a node") == (1, 1)


TOO_DEEP = {"name": "too_deep", "direction": "high", "thesis": "t", "expr": {"op": "zscore", "window": 30, "args": [
    {"op": "div", "args": [{"op": "mean", "window": 7, "args": [{"op": "add", "args": [
        {"op": "abs", "args": [{"col": "liq_long"}]}, {"const": 1.0}]}]}, {"col": "oi"}]}]}}  # 6 levels, 8 nodes
SHALLOW = {"name": "too_deep_fixed", "direction": "high", "thesis": "t", "expr": {"op": "zscore", "window": 30, "args": [
    {"op": "div", "args": [{"op": "mean", "window": 7, "args": [{"col": "liq_long"}]}, {"col": "oi"}]}]}}
WINDOW_AS_ARG = {"name": "window_as_arg", "direction": "low", "expr": {"op": "mean", "args": [{"col": "close"}, {"const": 7}]}}
OP_AS_COLUMN = {"name": "op_as_column", "direction": "low", "expr": {"op": "div", "args": [{"col": "volume"}, {"op": "oi"}]}}
MOM = {"name": "mom", "direction": "high", "expr": {"op": "pct_change", "window": 7, "args": [{"col": "close"}]}}


@pytest.fixture
def scripted_round(monkeypatch, tmp_path):
    """run_round with a scripted model and a stub scorer: this checks the conversation, not the evaluation."""
    monkeypatch.setattr(loop, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(loop, "LOCK_PATH", tmp_path / "round.lock")
    panel = _panel(days=1600, coins=32)
    monkeypatch.setattr(xp, "load_panel", lambda **kwargs: panel)
    dev = {"status": "ok", "ic": 0.01, "z": 0.4, "coverage": 0.9, "period": ["2020-07-01", "2023-12-31"]}
    holdout = {"status": "ok", "z": 0.1, "resid_z": 0.1, "coverage": 0.9, "net_per_book": -0.001, "net_per_book_ex_top3": -0.001}
    monkeypatch.setattr(ev, "evaluate", lambda formula, ctx, **kw: {"dev": dict(dev), "holdout": dict(holdout)})
    calls = []

    def run(propose, repair=None):
        """``propose``: formulas for a new round; ``repair``: formulas returned for each repair request (or an exception)."""
        async def generate(prompt):
            body = json.loads(prompt)
            calls.append(body)
            if "rejected" not in body:
                return {"hypothesis": "h", "formulas": propose}
            if isinstance(repair, Exception):
                raise repair
            return {"formulas": repair or []}
        return loop.run_round(force=True, log=lambda m: None, generate=generate)

    return run, calls


def test_a_round_states_the_limits_and_repairs_rejected_formulas_once(scripted_round):
    run, calls = scripted_round
    assert "formula_rules" in loop.SYSTEM_PROMPT
    record = run([TOO_DEEP, MOM], repair=[SHALLOW, MOM])
    first, repair = calls
    assert first["formula_rules"] == loop.formula_rules() and first["your_recent_formulas_rejected_by_the_validator"] == []
    assert record["invalid"] == [{"name": "too_deep", "error": f"expression deeper than {dsl.MAX_DEPTH}", "depth": 6, "nodes": 8}]
    # the repair request carries the model's own formula, the validator's message and the rules - nothing else
    assert repair["rejected"] == [{"formula": TOO_DEEP, **record["invalid"][0]}]
    assert repair["formula_rules"] == loop.formula_rules() and "holdout" not in json.dumps(repair)
    assert record["repair_attempts"] == 1
    attempt = record["repairs"][0]
    # a repair replaces a rejected formula one for one: MOM in the reply is cut, the round stays at formulas_per_round
    assert attempt["requested"] == ["too_deep"] and attempt["accepted"] == ["too_deep_fixed"]
    assert attempt["invalid"] == [] and attempt["duplicates"] == []
    assert record["status"] == "completed" and len(record["evaluated"]) == 2
    state = loop.load_state()
    assert sorted(e["name"] for e in state["ledger"].values()) == ["mom", "too_deep_fixed"]  # rejected ones are no trials
    assert [e["trial_number"] for e in sorted(state["ledger"].values(), key=lambda e: e["trial_number"])] == [1, 2]

    calls.clear()
    record = run([WINDOW_AS_ARG])  # nothing to repair with: still exactly one repair call
    assert len(calls) == 1 + loop.MAX_REPAIR_ATTEMPTS == 2
    assert record["repair_attempts"] == 1 and record["evaluated"] == [] and record["status"] == "completed"
    assert len(loop.load_state()["ledger"]) == 2

    calls.clear()
    run([MOM])  # a duplicate, nothing invalid: no repair call
    feedback = calls[0]["your_recent_formulas_rejected_by_the_validator"]
    assert len(calls) == 1
    assert feedback == [
        {"name": "too_deep", "error": "expression deeper than 5", "depth": 6, "nodes": 8},
        {"name": "window_as_arg", "error": "mean takes 1 arg", "depth": 2, "nodes": 3},
    ]
    assert "holdout" not in json.dumps(calls[0])
    assert loop.load_state()["rounds"][-1]["repair_attempts"] == 0


def test_repair_retries_stop_at_the_cap(scripted_round, monkeypatch):
    run, calls = scripted_round
    monkeypatch.setattr(loop, "MAX_REPAIR_ATTEMPTS", 2)
    record = run([OP_AS_COLUMN], repair=[OP_AS_COLUMN])  # the model never fixes it
    assert len(calls) == 3 and record["repair_attempts"] == 2
    assert [a["invalid"][0]["error"] for a in record["repairs"]] == ["op 'oi' needs an args list"] * 2
    assert loop.load_state()["ledger"] == {}
    # the second request names only what was still invalid after the first
    assert [r["name"] for r in calls[2]["rejected"]] == ["op_as_column"]
    feedback = loop.recent_invalid(loop.load_state())
    assert [row["name"] for row in feedback] == ["op_as_column"] * 3  # first pass + both repairs


def test_a_failed_repair_call_keeps_the_valid_formulas(scripted_round):
    run, calls = scripted_round
    record = run([TOO_DEEP, MOM], repair=TimeoutError("model timed out"))
    assert len(calls) == 2 and record["status"] == "completed"
    assert record["repair_attempts"] == 1 and record["repairs"][0]["error"].startswith("TimeoutError")
    assert [e["name"] for e in loop.load_state()["ledger"].values()] == ["mom"]


def test_any_started_round_books_the_next_one(monkeypatch, tmp_path):
    import asyncio
    from datetime import datetime, timedelta, timezone

    monkeypatch.setattr(loop, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(loop, "LOCK_PATH", tmp_path / "round.lock")
    monkeypatch.setattr(loop, "LOG_PATH", tmp_path / "round.log")
    monkeypatch.setattr(loop, "_process", None)
    started = []

    class _Proc:
        pid, returncode = 4242, None

    async def fake_exec(*args, **kwargs):
        started.append(args)
        return _Proc()

    monkeypatch.setattr(loop.asyncio, "create_subprocess_exec", fake_exec)
    assert asyncio.run(loop.start_round(force=True)) is True  # a manual run ...
    due = datetime.fromisoformat(loop.load_state()["next_run_at"])
    assert timedelta(hours=5.9) < due - datetime.now(timezone.utc) < timedelta(hours=6.1)  # ... still books the next slot
    assert "--force" in started[0]
    assert asyncio.run(loop.start_round()) is False  # one round at a time
    loop.update_state(lambda s: s["config"].update(enabled=True))
    asyncio.run(loop.tick())  # not due yet, and the first one is still running
    assert len(started) == 1
