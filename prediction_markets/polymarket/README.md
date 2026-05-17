# Polymarket Offline Paper Workflow

This package is independent from the crypto exchange trading engine. Live Polymarket trading is intentionally not implemented; the current workflow uses stored quotes and the local paper account tables.

## Replay and Research

Run a single-token replay:

```powershell
python scripts\research\paper_replay_polymarket.py --token-id <token_id> --since 2026-03-03T00:00:00Z --until 2026-03-03T01:00:00Z
```

Run batch ranking or grid search from stored quotes:

```powershell
python scripts\research\paper_replay_batch_polymarket.py --top-n 20 --min-quotes 10 --strategy threshold --since 2026-03-03T00:00:00Z --until 2026-03-03T06:00:00Z
python scripts\research\paper_replay_grid_polymarket.py --top-n 20 --min-quotes 10 --strategy threshold --since 2026-03-03T00:00:00Z --until 2026-03-03T06:00:00Z
```

Run walk-forward selection:

```powershell
python scripts\research\paper_replay_walk_forward_polymarket.py --top-n 20 --min-quotes 10 --strategy threshold --since 2026-03-03T00:00:00Z --until 2026-03-03T06:00:00Z --train-minutes 120 --test-minutes 60 --step-minutes 60
```

## Promote a Profile

Convert a walk-forward report JSON into a reusable paper strategy profile:

```powershell
python scripts\research\promote_polymarket_profile.py --report data\reports\polymarket_replay_walk_forward.json --token-ids <token_id> --account-id default --output data\reports\polymarket_paper_strategy_profile.json
```

Promotion applies guardrails by default: at least 2 walk-forward segments, at least 1 positive segment, and non-negative total test PnL. Use `--allow-unsafe` only when you want a profile written for inspection; unsafe profiles are marked `safe_to_execute=false`.

## Paper Strategy Pass

Dry-run from latest stored quotes:

```powershell
python scripts\research\paper_strategy_once_polymarket.py --profile data\reports\polymarket_paper_strategy_profile.json
```

Execute against the local paper account:

```powershell
python scripts\research\paper_strategy_once_polymarket.py --profile data\reports\polymarket_paper_strategy_profile.json --execute
```

Equivalent Ops endpoints:

- `POST /ops/polymarket/replay/batch`
- `POST /ops/polymarket/replay/grid`
- `POST /ops/polymarket/replay/walk_forward`
- `POST /ops/polymarket/paper/profile/promote`
- `POST /ops/polymarket/paper/strategy_once`
