# Strategy Signal Clock Fix Plan

Date: 2026-05-18

## Problem

`Signal.timestamp` is sourced from two different clocks across strategies:

- Bar time (`pd.Timestamp(data.index[-1])`) — only `ma_strategy.py`,
  `multi_factor_hf.py` (correct: the signal belongs to the bar that triggered
  it).
- Wall clock (`datetime.now(timezone.utc)`) — ~10 bar-driven strategies
  (wrong: the signal is stamped with the moment the function ran).

Impact (grounded):

1. `strategy_manager._emit_signals` conflict window
   (`age = signal.timestamp - prior.timestamp`, 60s) silently misses real
   cross-strategy conflicts whenever a bar-time strategy and a wall-clock
   strategy fire on the same symbol (age can be ~3600s on 1h bars). The
   `except TypeError → age = window+1` branch is a band-aid for this.
2. Replay / `/{name}/live-vs-backtest` time alignment is corrupted for the
   wall-clock strategies.
3. `pairs_trading` builds `pair_group_id` from the timestamp → non-deterministic
   group ids under replay / same-wall-second calls.

## Design

Add a single shared helper in `core/strategies/strategy_base.py`:

```python
def bar_time(data, *, fallback=None):
    # latest datetime-index value, tz-normalized to UTC;
    # wall-clock UTC fallback only when there is no datetime index.
```

- Also expose `StrategyBase._bar_time(self, data)` delegating to it.
- The helper normalizes tz-naive bar indices to UTC, which additionally fixes
  the naive/aware mixing the conflict detector currently swallows.

## Scope

In scope (bar-driven; migrate the `timestamp = datetime.now(...)` that feeds
`Signal(timestamp=...)` to `self._bar_time(data)` / `bar_time(data)`):

- `strategies/technical/rsi_strategy.py` (pilot)
- `strategies/quantitative/mean_reversion.py`
- `strategies/quantitative/momentum.py`
- `strategies/quantitative/pairs_trading.py`
- `strategies/quantitative/fama_factor_arbitrage.py`
- `strategies/factor_based/factor_strategies.py` (shared base of ~18 factor
  strategies — one fix covers many)
- `strategies/ai/ml_xgboost_strategy.py`
- `strategies/macro/fund_flow.py`
- `strategies/macro/market_sentiment.py`
- `core/strategies/signal_generator.py` — only if it consumes a bar frame;
  otherwise leave + add a one-line WHY comment.

Explicitly EXCLUDED (wall clock is semantically correct — no triggering bar):

- `strategies/arbitrage/cex_arbitrage.py`, `dex_arbitrage.py` — opportunities
  are real-time orderbook events, not historical bars.
- `core/strategies/strategy_manager.py:737` force-close synthetic signal — a
  real-time operational action by the manager, not a strategy bar evaluation.

Only the timestamp that feeds `Signal(...)` changes. Other `datetime.now()`
uses (lookback cutoffs, 429 backoff, data-freshness checks) are left untouched.

## Stages

- S1: add `bar_time()` + `StrategyBase._bar_time()` + a focused unit test.
- S2: pilot `rsi_strategy.py`; run `test_strategy_signal_regressions.py`
  + `test_strategies.py`.
- S3: migrate the remaining in-scope files in groups
  (quantitative → factor/ai → macro), running the strategy regression after
  each group.
- S4: full strategy regression + 46/46 instantiate-and-run smoke +
  `test_strategy_manager_runtime_data.py` (conflict path).
- S5: completion note here. Do NOT commit (unrelated Codex altcoin WIP is in
  the working tree).

## Acceptance

- `bar_time(df_with_dt_index)` returns the last index ts (tz-aware UTC);
  `bar_time(df_without_dt_index)` returns now(UTC).
- A pilot RSI signal carries the bar timestamp, not wall-clock.
- All previously-passing strategy tests still pass (60+ suite).
- 46/46 strategies still instantiate and run `generate_signals` w/o error.

## Completion Note (2026-05-18)

Status: COMPLETE (S1–S5). Not committed (working tree also holds unrelated
in-progress Codex altcoin-radar work; left untouched).

- S1 — `bar_time()` + `StrategyBase._bar_time()` added to
  `core/strategies/strategy_base.py`. tz-naive bar indices normalized to UTC
  (also fixes the naive/aware mixing the conflict detector band-aided).
  Guards against int/RangeIndex being misread by `pd.Timestamp` as a 1970
  epoch offset → correct wall-clock fallback. Unit test added.
- S2 — pilot `rsi_strategy.py` (both `generate_signals` variants).
- S3 — migrated: `mean_reversion.py` (×2), `momentum.py` (×2),
  `pairs_trading.py`, `fund_flow.py` WhaleActivity (×3),
  `ml_xgboost_strategy.py` in-method (×2), and `factor_strategies.py`
  via `_create_signal` reading `self._active_bar_ts`, set at the top of all
  18 factor `generate_signals`.
- S4 — strategy regression 48 passed; 46/46 instantiate-and-run smoke clean;
  `test_strategy_manager_runtime_data.py` (conflict path) green.
- S5 — this note.

Tests added to `tests/test_strategy_signal_regressions.py`:
`test_bar_time_uses_latest_bar_not_wall_clock`,
`test_rsi_signal_carries_bar_time_not_now`.

S6 (post-review correction) — `fama_factor_arbitrage.py` was initially
deferred, but on re-inspection `_build_fama_rebalance_plan` does have
`close_df` (datetime index) reachable at all 4 `_make_signal` call sites, so
the deferral rationale was wrong. Fixed properly: `_make_signal` takes an
optional `bar_ts`; `_build_fama_rebalance_plan` computes
`rebalance_bar_ts = bar_time(close_df)` once and passes it to all 4 calls;
wall-clock only as fallback. No remaining bar-driven strategy is unfixed.

Excluded by design (wall clock is semantically correct):
- arbitrage (`cex_arbitrage.py`, `dex_arbitrage.py`) — real-time orderbook
  opportunities, no triggering bar.
- `strategy_manager.py:737` force-close synthetic signal — operational
  real-time action by the manager, not a strategy bar evaluation.
- `market_sentiment.py` — uses `self._sentiment_data.get("timestamp", now)`:
  sentiment-event time with a sane fallback, not the lazy wall-clock bug.
- `core/strategies/signal_generator.py` — not a `StrategyBase` strategy
  (separate internal component, outside the strategy-page scope).

Net effect: every bar-driven StrategyBase strategy now stamps `Signal.timestamp`
with the triggering bar's time (tz-aware UTC), so `strategy_manager`'s 60s
cross-strategy conflict window finally compares like-for-like, and replay /
live-vs-backtest time alignment is correct for the migrated strategies.
