# CoinGlass Optional Market Data Implementation Guide

Date: 2026-05-17

Purpose: give Codex a concrete local brief for adding optional CoinGlass datasets beyond the current core derivatives set, while preserving the user's 10 requests/minute CoinGlass plan.

## Ground Rules

- Keep OHLCV/Kline download exchange-first.
- Keep CoinGlass `price_history` as the automatic fallback for Kline download.
- Non-basic market-structure data should prefer CoinGlass when CoinGlass provides it.
- Do not put heavy optional datasets into the default automatic refresh set.
- Respect `COINGLASS_RATE_LIMIT_PER_MIN=10`.
- Non-manual refresh must stay budget-aware and should reserve at least 1 request of minute headroom.
- Manual or scheduled low-frequency refresh may request selected optional datasets explicitly.
- Do not remove exchange fallbacks where the data is exchange-native and already works, but do not use exchange HTTP as the silent primary for premium derivatives/on-chain data.

## Current Known State

Core CoinGlass routing already exists in:

- `core/data/coinglass_registry.py`
- `core/data/coinglass_client.py`
- `core/data/coinglass_feature_builder.py`
- `scripts/refresh_coinglass_incremental.py`
- `web/api/trading.py`
- `core/research/strategy_research.py`
- `core/research/altcoin_radar.py`
- `core/ai/coinglass_signal.py`
- `core/ai/signal_aggregator.py`
- `web/api/ai_research.py`

Current default lightweight datasets should remain default:

- `open_interest_exchange_list`
- `open_interest_history`
- `funding_rate_exchange_list`
- `funding_rate_history`
- `taker_buy_sell_volume_exchange_list`
- `taker_buy_sell_volume_history`
- `liquidation_history`
- `global_long_short_account_ratio_history`
- `funding_arbitrage`

Current optional datasets already considered or partially wired:

- `open_interest_aggregated_history`
- `open_interest_stablecoin_margin_history`
- `top_long_short_account_ratio_history`
- `top_long_short_position_ratio_history`
- `net_position_history`
- `liquidation_aggregated_history`
- `liquidation_aggregated_map`
- `liquidation_aggregated_heatmap_model1`
- `futures_orderbook_aggregated_ask_bids_history`
- `coinbase_premium_index`
- `option_max_pain`
- `bitcoin_etf_flow_history`

## Recommended Priority

1. Orderbook Heatmap / Liquidity Heatmap
2. Spot Inflow / Outflow
3. Exchange Balance / On-chain
4. Options Data

## 1. Orderbook Heatmap / Liquidity Heatmap

Priority: high.

Why:

- Directly improves short-term structure judgment.
- Useful for research page, altcoin radar, and AI autonomous agent.
- Complements existing liquidation map and taker imbalance.

Candidate CoinGlass datasets/endpoints:

- Existing optional: `futures_orderbook_aggregated_ask_bids_history`
- Existing optional: `liquidation_aggregated_map`
- Existing optional: `liquidation_aggregated_heatmap_model1`
- If CoinGlass exposes dedicated orderbook/liquidity heatmap endpoints in the API spec, add them as optional manifests.

Suggested normalized fields:

- `orderbook_agg_bid_usd`
- `orderbook_agg_ask_usd`
- `orderbook_agg_imbalance`
- `orderbook_wall_above_usd`
- `orderbook_wall_below_usd`
- `liquidity_heatmap_total_usd`
- `liquidity_heatmap_above_usd`
- `liquidity_heatmap_below_usd`
- `liquidity_wall_nearest_above_price`
- `liquidity_wall_nearest_below_price`
- `liquidity_void_score`
- `heatmap_pressure_score`

Integration targets:

- `core/data/coinglass_client.py`: normalize rows.
- `core/data/coinglass_feature_builder.py`: consume rows into `DerivativesSnapshot.payload`.
- `web/api/trading.py`: expose in microstructure `derivatives_context`.
- `core/research/altcoin_radar.py`: include in `metrics` and `derivatives_context`.
- `core/research/strategy_research.py`: flatten into `coinglass_payload_*` features.
- `core/ai/coinglass_signal.py`: use as risk/context flags, initially shadow-only unless already live-gated.

Refresh policy:

- Do not add to default automatic refresh.
- Refresh manually or every 30-60 minutes for BTC/ETH/SOL only.
- For broad altcoin scans, use cached values only.

Example manual refresh:

```powershell
python scripts\refresh_coinglass_incremental.py --manual --symbols BTC/USDT,ETH/USDT,SOL/USDT --datasets futures_orderbook_aggregated_ask_bids_history,liquidation_aggregated_map,liquidation_aggregated_heatmap_model1 --max-symbols 3
```

## 2. Spot Inflow / Outflow

Priority: medium-high.

Why:

- Adds spot-side pressure that derivatives-only data misses.
- Useful as a confirmation filter: exchange net inflow often implies potential sell pressure; net outflow often implies supply tightening.

Current project hints:

- `web/api/trading.py` already has CoinGlass whale transfer and exchange chain transaction logic.
- Existing normalizers classify `inflow` / `outflow`.
- On-chain overview UI already has a place to display whale/on-chain context.

Candidate CoinGlass endpoints/datasets:

- Existing ad hoc calls around `/v4/api/chain/v2/whale-transfer`
- Existing exchange chain transfer logic in `web/api/trading.py`
- If API spec exposes spot exchange flow endpoints, add first-class manifests for them.

Suggested normalized fields:

- `spot_exchange_inflow_usd`
- `spot_exchange_outflow_usd`
- `spot_exchange_netflow_usd`
- `spot_exchange_inflow_count`
- `spot_exchange_outflow_count`
- `spot_netflow_score`
- `exchange_flow_pressure`
- `whale_inflow_usd`
- `whale_outflow_usd`

Integration targets:

- Prefer moving reusable CoinGlass chain/flow fetch and normalization out of `web/api/trading.py` into `core/data/coinglass_client.py` or a dedicated `core/data/coinglass_onchain.py`.
- Feed summary fields into `AnalyticsWhaleSnapshot.payload` or a new normalized CoinGlass dataset.
- Expose on:
  - `/data/onchain/overview`
  - research workbench on-chain module
  - altcoin radar whale/on-chain context
  - AI research source health payload

Refresh policy:

- Low-frequency only, 15-60 minutes depending on endpoint cost.
- For altcoin radar, use cached flow only; no per-row live calls.

## 3. Exchange Balance / On-chain

Priority: medium.

Why:

- Best for medium-term confirmation and risk context.
- Less useful for second-by-second execution, but valuable for AI research and regime checks.

Candidate fields:

- `exchange_balance_btc`
- `exchange_balance_usd`
- `exchange_balance_change_24h`
- `exchange_balance_change_7d`
- `stablecoin_exchange_balance_usd`
- `stablecoin_exchange_balance_change_24h`
- `stablecoin_netflow_usd`
- `onchain_activity_score`
- `exchange_reserve_pressure_score`

Integration targets:

- Add optional CoinGlass manifest(s) if API spec exposes exchange balance/reserve endpoints.
- Add health source under AI research `premium_onchain`.
- Add cached display in on-chain overview.
- Feed compact scores into `strategy_research` and `altcoin_radar`, not raw bulky rows.

Refresh policy:

- 1-4 hours for exchange balances.
- Daily or 4-hourly for slower reserve-style metrics.
- Do not use in default worker refresh.

## 4. Options Data

Priority: medium-low overall, high for BTC/ETH research.

Why:

- Useful for BTC/ETH risk regimes.
- Less useful for most altcoins.
- Should not consume per-symbol budget during radar scans.

Current project hints:

- Existing `option_max_pain` optional dataset.
- Existing Deribit options collector is referenced in AI research health.

Candidate CoinGlass datasets/fields:

- `option_max_pain`
- `put_call_ratio`
- `options_open_interest`
- `options_volume`
- `iv`
- `iv_skew`
- `gamma_exposure` if available

Suggested normalized fields:

- `option_max_pain`
- `option_put_call_ratio`
- `option_open_interest_usd`
- `option_volume_usd`
- `option_iv`
- `option_iv_skew`
- `option_distance_to_max_pain_pct`

Integration targets:

- `core/data/coinglass_registry.py`: manifests.
- `core/data/coinglass_client.py`: normalization.
- `core/data/coinglass_feature_builder.py`: include in snapshot payload.
- `web/api/ai_research.py`: source health under `options`.
- `core/research/strategy_research.py`: use as BTC/ETH-only premium features.

Refresh policy:

- BTC/ETH only.
- 1-4 hour refresh.
- Optional/manual only.

## Implementation Checklist For Codex

1. Inspect CoinGlass API capability/spec cache or run a manual capability discovery with a tiny dataset list.
2. Add any missing manifests in `core/data/coinglass_registry.py`.
3. Keep new heavy manifests in `_COINGLASS_OPTIONAL_DATASETS`.
4. Add robust normalizers in `core/data/coinglass_client.py`.
5. Update `normalize_dataset_response()` so new datasets persist normalized rows.
6. Update `core/data/coinglass_feature_builder.py` to consume optional rows into `DerivativesSnapshot.payload` or on-chain/options summaries.
7. Expose compact fields in:
   - trading microstructure overlay
   - research workbench / research page
   - altcoin radar
   - AI research source health
   - AI autonomous agent via `coinglass_signal` / `signal_aggregator`
8. Add tests:
   - manifest exists and is optional
   - normalizer handles representative response shapes
   - feature builder consumes cached optional rows
   - altcoin radar/research output surfaces compact fields
   - non-manual refresh stays capped by minute headroom
9. Run targeted tests:

```powershell
python -m pytest tests\test_coinglass_dataset_normalization.py tests\test_coinglass_budget_short_circuit.py tests\test_altcoin_radar_derivatives.py tests\test_research_market_state.py tests\test_research_workbench_recommendations_api.py tests\test_coinglass_signal.py tests\test_ai_research_phase2.py -q
```

10. If doing a live check, use only one or two datasets at a time and wait for minute budget reset when needed.

## Non-Goals

- Do not make CoinGlass optional heavy datasets part of every automatic worker cycle.
- Do not replace exchange OHLCV primary download with CoinGlass.
- Do not call one CoinGlass endpoint per altcoin row in radar rendering.
- Do not let AI autonomous agent trade-live on new signals by default; keep them shadow/context unless `COINGLASS_LIVE_GATING_ENABLED=true`.

## Suggested First Patch

Start with Orderbook/Liquidity because it is closest to existing code:

- Harden normalization for `futures_orderbook_aggregated_ask_bids_history`.
- Add optional heatmap fields to `DerivativesSnapshot.payload`.
- Surface fields in `web/api/trading.py` `derivatives_context`.
- Add altcoin radar metrics.
- Add 2-3 focused tests.

This gives immediate value while staying within the 10/minute plan.
