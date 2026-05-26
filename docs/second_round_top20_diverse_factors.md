# 第二轮非重复因子挖掘：20个新因子

- 生成时间 UTC：2026-05-24T16:32:40.278831+00:00
- Python：`/mnt/data/zhengxingchun/miniforge3/envs/EnvForNet/bin/python`
- 数据：本地 Binance OHLCV，timeframes=5m, 15m, 1h
- 区间：`2026-01-01` 到 `latest`
- 交易口径：横截面 top/bottom 20%，信号在 bar t 形成，下一根 bar close 入场，持有到 horizon 后退出。
- 成本：U本位普通用户 taker 5.0 bps/边 + 动态滑点 max(2.0 bps, 0.08 * high-low range bps)，滑点封顶 20.0 bps/边。
- 验证：先训练/测试段筛方向，再用近似非重叠重平衡做月度和半月度稳定性验证。
- 去重：最终每个 `factor_group` 最多入选一个，避免只是同一公式换参数；上一轮 top 因子 ID 已排除。

## 结论

- 本轮最终入选 20 个不同经济含义的因子，其中 priority=12，candidate=5。
- 这批因子主要来自成交量确认、流动性迁移、波动结构、尾部形态、lead/lag 和 beta 非对称，而不是上一轮的纯 24h/48h 反转。
- 实盘上仍建议先 paper：这批新因子比第一轮价格反转更分散，但很多依赖成交额和盘口冲击代理，真实滑点会更关键。

| rank | factor_id | factor_group | timeframe | horizon_label | execution_mode | family | net_mean_bps | stress_1_5x_net_bps | total_return_pct | month_positive_ratio | halfmonth_positive_ratio | win_rate | tstat | max_drawdown_pct | trades | live_verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | return_entropy_4h | return_entropy | 1h | 48h | spread_low_minus_high | nonparametric_chop | 174.0612 | 147.3383 | 122.4456 | 0.7500 | 0.7500 | 0.4483 | 1.4237 | -18.8303 | 58 | priority |
| 2 | false_breakout_supply_24h | false_breakout_supply | 1h | 72h | short_low | liquidity_rejection | 105.9084 | 93.0438 | 64.4156 | 0.7500 | 0.5000 | 0.5818 | 1.4046 | -18.1869 | 55 | candidate |
| 3 | range_asymmetry_48h | range_asymmetry | 5m | 24h | spread_low_minus_high | intrabar_range_shape | 74.1058 | 58.8259 | 83.2542 | 1.0000 | 0.8571 | 0.6047 | 2.6117 | -13.8511 | 86 | priority |
| 4 | session_asia_flow_24h | session_asia_flow | 5m | 24h | spread_low_minus_high | session_flow | 64.3021 | 46.7227 | 45.4869 | 1.0000 | 0.8571 | 0.6333 | 2.7260 | -3.8180 | 60 | priority |
| 5 | session_flow_rotation_24h | session_flow_rotation | 5m | 24h | spread_low_minus_high | session_flow | 58.6319 | 40.5999 | 38.9387 | 1.0000 | 1.0000 | 0.6271 | 1.8990 | -7.6725 | 59 | priority |
| 6 | volume_weighted_return_24h | volume_weighted_return | 5m | 24h | spread_low_minus_high | volume_confirmed_pressure | 63.0519 | 47.6619 | 43.7039 | 1.0000 | 0.7143 | 0.6230 | 1.8626 | -14.5671 | 61 | priority |
| 7 | wick_imbalance_48h | wick_imbalance | 5m | 24h | short_low | liquidity_rejection | 52.1506 | 44.4815 | 46.5173 | 1.0000 | 0.7143 | 0.5714 | 1.3119 | -20.0116 | 84 | priority |
| 8 | turnover_entropy_48h | turnover_entropy | 15m | 48h | short_high | attention_structure | 60.3287 | 51.5505 | 51.2588 | 0.7500 | 0.5714 | 0.5769 | 1.3982 | -25.5498 | 78 | priority |
| 9 | body_volume_corr_24h | body_volume_correlation | 5m | 24h | spread_low_minus_high | activity_direction_confirmation | 31.9710 | 16.5059 | 20.0408 | 1.0000 | 0.8571 | 0.6885 | 1.2527 | -9.8380 | 61 | priority |
| 10 | corr_breakdown_24h_72h | correlation_breakdown | 5m | 24h | short_high | market_relation_shift | 39.1305 | 30.8520 | 22.7488 | 0.7500 | 0.5714 | 0.5082 | 0.9198 | -15.2105 | 61 | priority |
| 11 | extreme_recency_48h | extreme_recency | 5m | 24h | short_high | breakout_timing | 31.2391 | 23.8068 | 27.1542 | 0.7500 | 0.4286 | 0.5287 | 1.0786 | -16.1855 | 87 | watchlist |
| 12 | up_down_beta_spread_24h_72h | up_down_beta_spread | 5m | 24h | spread_low_minus_high | asymmetric_market_beta | 20.0380 | 3.9455 | 16.8650 | 0.7500 | 0.8571 | 0.5517 | 0.9022 | -15.8985 | 87 | priority |
| 13 | directional_range_efficiency_48h | directional_range_efficiency | 5m | 24h | spread_low_minus_high | liquidity_impact | 18.8896 | 3.2203 | 10.9569 | 0.7500 | 0.7143 | 0.5079 | 0.6816 | -12.7536 | 63 | priority |
| 14 | cross_sectional_stress_4h | cross_sectional_stress | 1h | 72h | spread_low_minus_high | relative_shock | 26.3401 | -2.8686 | 17.7181 | 0.7500 | 0.7500 | 0.5140 | 0.5823 | -35.7108 | 107 | candidate |
| 15 | sign_imbalance_4h | sign_imbalance | 1h | 48h | long_high | nonparametric_trend | 31.0342 | 16.9881 | 7.1695 | 0.7500 | 0.5000 | 0.4516 | 0.4042 | -45.4299 | 93 | candidate |
| 16 | vwap_slope_24h | vwap_slope | 5m | 24h | spread_low_minus_high | volume_price_inventory | 24.6593 | 9.4923 | 14.0369 | 0.5000 | 0.4286 | 0.4915 | 0.8667 | -12.1653 | 59 | watchlist |
| 17 | vwap_gap_48h | vwap_gap | 5m | 24h | spread_low_minus_high | volume_price_inventory | 22.2989 | 7.1986 | 13.4927 | 0.5000 | 0.2857 | 0.5079 | 0.8368 | -12.5754 | 63 | watchlist |
| 18 | relative_vol_shock_24h | relative_vol_shock | 5m | 12h | short_low | relative_volatility | 10.1736 | 2.6022 | 15.1734 | 0.7500 | 0.5714 | 0.4310 | 0.6591 | -22.4594 | 174 | priority |
| 19 | lead_market_response_24h_72h | lead_market_response | 5m | 24h | spread_low_minus_high | lead_lag | 16.7338 | 0.4737 | 9.4649 | 0.5000 | 0.5714 | 0.4590 | 0.6642 | -9.5148 | 61 | candidate |
| 20 | break_count_balance_24h | break_count_balance | 5m | 24h | spread_low_minus_high | breakout_frequency | 6.9043 | -8.5830 | 4.0263 | 0.7500 | 0.7143 | 0.4878 | 0.3026 | -13.9985 | 82 | candidate |

## Top20 等权组合

- 样本：2026-01-01 至 2026-04-20
- 交易点：1198
- 期末权益：`69.8120`
- 1.5x 成本压力期末权益：`12.4386`
- spot 10bps 压力期末权益：`21.1324`

- 组合权益曲线：[top20_equal_weight_combo_equity.png](curves/top20_equal_weight_combo_equity.png)
- 组合月度柱状图：[top20_equal_weight_combo_monthly_bars.png](curves/top20_equal_weight_combo_monthly_bars.png)

## 逐因子分析

### 1. `return_entropy_4h`

- 分组：`return_entropy` / `nonparametric_chop`，与上一轮因子不同点：Return entropy distinguishes clean directional tapes from noisy two-way tapes.
- 公式：-p_up*log(p_up) - p_down*log(p_down)
- 执行：`spread_low_minus_high`，timeframe `1h`，hold `48h`，rebalance 约 `24.0h`。
- 回测：平均净收益 174.06 bps/次，总收益 122.45%，胜率 44.83%，t=1.42，最大回撤 -18.83%，交易点 58。
- 稳定性：月度正收益比例 75%，半月正收益比例 75%，1.5x 成本后 147.34 bps/次，spot10 压力 164.06 bps/次。
- 实盘处理：High entropy means two-way noise; low entropy means one-sided tape.
- 曲线：[equity](curves/01_return_entropy_4h_1h_48h_spread_low_minus_high_equity.png)，[monthly bars](curves/01_return_entropy_4h_1h_48h_spread_low_minus_high_monthly_bars.png)

### 2. `false_breakout_supply_24h`

- 分组：`false_breakout_supply` / `liquidity_rejection`，与上一轮因子不同点：False breakout supply focuses on new-high probes that fail to close strongly.
- 公式：mean(upper_rejection when high exceeds prior rolling high)
- 执行：`short_low`，timeframe `1h`，hold `72h`，rebalance 约 `24.0h`。
- 回测：平均净收益 105.91 bps/次，总收益 64.42%，胜率 58.18%，t=1.40，最大回撤 -18.19%，交易点 55。
- 稳定性：月度正收益比例 75%，半月正收益比例 50%，1.5x 成本后 93.04 bps/次，spot10 压力 95.91 bps/次。
- 实盘处理：Best used with hard stop and funding filter on the short side.
- 曲线：[equity](curves/02_false_breakout_supply_24h_1h_72h_short_low_equity.png)，[monthly bars](curves/02_false_breakout_supply_24h_1h_72h_short_low_monthly_bars.png)

### 3. `range_asymmetry_48h`

- 分组：`range_asymmetry` / `intrabar_range_shape`，与上一轮因子不同点：Range asymmetry separates upside probing from downside probing inside each bar.
- 公式：mean((high/prev_close-1) - (prev_close/low-1))
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 74.11 bps/次，总收益 83.25%，胜率 60.47%，t=2.61，最大回撤 -13.85%，交易点 86。
- 稳定性：月度正收益比例 100%，半月正收益比例 86%，1.5x 成本后 58.83 bps/次，spot10 压力 64.11 bps/次。
- 实盘处理：Captures intrabar probing; combine with turnover filter to avoid bad prints.
- 曲线：[equity](curves/03_range_asymmetry_48h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/03_range_asymmetry_48h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 4. `session_asia_flow_24h`

- 分组：`session_asia_flow` / `session_flow`，与上一轮因子不同点：Asia-session flow isolates whether UTC 00-08 activity carries predictive pressure.
- 公式：rolling_sum(return*dollar_volume during UTC 00-08) / rolling_sum(dollar_volume during UTC 00-08)
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 64.30 bps/次，总收益 45.49%，胜率 63.33%，t=2.73，最大回撤 -3.82%，交易点 60。
- 稳定性：月度正收益比例 100%，半月正收益比例 86%，1.5x 成本后 46.72 bps/次，spot10 压力 54.30 bps/次。
- 实盘处理：Session effects should be traded with fixed rebalance windows; avoid reacting mid-session.
- 曲线：[equity](curves/04_session_asia_flow_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/04_session_asia_flow_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 5. `session_flow_rotation_24h`

- 分组：`session_flow_rotation` / `session_flow`，与上一轮因子不同点：Session flow rotation compares early-day and late-day directional pressure.
- 公式：asia_session_flow - us_session_flow
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 58.63 bps/次，总收益 38.94%，胜率 62.71%，t=1.90，最大回撤 -7.67%，交易点 59。
- 稳定性：月度正收益比例 100%，半月正收益比例 100%，1.5x 成本后 40.60 bps/次，spot10 压力 48.63 bps/次。
- 实盘处理：This is a slow rotation signal, not an intraday execution timer.
- 曲线：[equity](curves/05_session_flow_rotation_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/05_session_flow_rotation_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 6. `volume_weighted_return_24h`

- 分组：`volume_weighted_return` / `volume_confirmed_pressure`，与上一轮因子不同点：Volume-weighted return gives more weight to bars where real participation occurred, unlike simple close-to-close return.
- 公式：rolling_sum(return * dollar_volume) / rolling_sum(dollar_volume)
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 63.05 bps/次，总收益 43.70%，胜率 62.30%，t=1.86，最大回撤 -14.57%，交易点 61。
- 稳定性：月度正收益比例 100%，半月正收益比例 71%，1.5x 成本后 47.66 bps/次，spot10 压力 53.05 bps/次。
- 实盘处理：This should be traded as a cross-sectional basket; single names are noisy around news.
- 曲线：[equity](curves/06_volume_weighted_return_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/06_volume_weighted_return_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 7. `wick_imbalance_48h`

- 分组：`wick_imbalance` / `liquidity_rejection`，与上一轮因子不同点：Wick imbalance tests whether rejected liquidity is mostly below or above the market.
- 公式：mean(lower_wick - upper_wick)
- 执行：`short_low`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 52.15 bps/次，总收益 46.52%，胜率 57.14%，t=1.31，最大回撤 -20.01%，交易点 84。
- 稳定性：月度正收益比例 100%，半月正收益比例 71%，1.5x 成本后 44.48 bps/次，spot10 压力 42.15 bps/次。
- 实盘处理：A rejection signal is fragile during news bars; cap single-bar influence.
- 曲线：[equity](curves/07_wick_imbalance_48h_5m_24h_short_low_equity.png)，[monthly bars](curves/07_wick_imbalance_48h_5m_24h_short_low_monthly_bars.png)

### 8. `turnover_entropy_48h`

- 分组：`turnover_entropy` / `attention_structure`，与上一轮因子不同点：Turnover entropy separates broad, distributed participation from one-bar attention bursts.
- 公式：entropy(dollar_volume over rolling window)
- 执行：`short_high`，timeframe `15m`，hold `48h`，rebalance 约 `24.0h`。
- 回测：平均净收益 60.33 bps/次，总收益 51.26%，胜率 57.69%，t=1.40，最大回撤 -25.55%，交易点 78。
- 稳定性：月度正收益比例 75%，半月正收益比例 57%，1.5x 成本后 51.55 bps/次，spot10 压力 50.33 bps/次。
- 实盘处理：Low entropy is event concentration; impose smaller notional and wider no-trade bands.
- 曲线：[equity](curves/08_turnover_entropy_48h_15m_48h_short_high_equity.png)，[monthly bars](curves/08_turnover_entropy_48h_15m_48h_short_high_monthly_bars.png)

### 9. `body_volume_corr_24h`

- 分组：`body_volume_correlation` / `activity_direction_confirmation`，与上一轮因子不同点：Body/volume correlation captures whether participation aligns with directional candle bodies.
- 公式：corr(body_return, log_dollar_volume)
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 31.97 bps/次，总收益 20.04%，胜率 68.85%，t=1.25，最大回撤 -9.84%，交易点 61。
- 稳定性：月度正收益比例 100%，半月正收益比例 86%，1.5x 成本后 16.51 bps/次，spot10 压力 21.97 bps/次。
- 实盘处理：Use with outlier clipping; one large liquidation bar can dominate the estimate.
- 曲线：[equity](curves/09_body_volume_corr_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/09_body_volume_corr_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 10. `corr_breakdown_24h_72h`

- 分组：`correlation_breakdown` / `market_relation_shift`，与上一轮因子不同点：Correlation breakdown catches coins decoupling from the equal-weight crypto market.
- 公式：corr(asset, market, short) - corr(asset, market, long)
- 执行：`short_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 39.13 bps/次，总收益 22.75%，胜率 50.82%，t=0.92，最大回撤 -15.21%，交易点 61。
- 稳定性：月度正收益比例 75%，半月正收益比例 57%，1.5x 成本后 30.85 bps/次，spot10 压力 29.13 bps/次。
- 实盘处理：Decoupling signals need universe breadth; avoid running on too few symbols.
- 曲线：[equity](curves/10_corr_breakdown_24h_72h_5m_24h_short_high_equity.png)，[monthly bars](curves/10_corr_breakdown_24h_72h_5m_24h_short_high_monthly_bars.png)

### 11. `extreme_recency_48h`

- 分组：`extreme_recency` / `breakout_timing`，与上一轮因子不同点：Extreme recency asks whether the recent high or recent low happened more recently, without using return size.
- 公式：argmax(high)/window - argmin(low)/window
- 执行：`short_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 31.24 bps/次，总收益 27.15%，胜率 52.87%，t=1.08，最大回撤 -16.19%，交易点 87。
- 稳定性：月度正收益比例 75%，半月正收益比例 43%，1.5x 成本后 23.81 bps/次，spot10 压力 21.24 bps/次。
- 实盘处理：This avoids return magnitude but still flags breakout timing; combine with liquidity filters.
- 曲线：[equity](curves/11_extreme_recency_48h_5m_24h_short_high_equity.png)，[monthly bars](curves/11_extreme_recency_48h_5m_24h_short_high_monthly_bars.png)

### 12. `up_down_beta_spread_24h_72h`

- 分组：`up_down_beta_spread` / `asymmetric_market_beta`，与上一轮因子不同点：Up/down beta spread measures asymmetric participation in rallies versus selloffs.
- 公式：beta_up_market - beta_down_market
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 20.04 bps/次，总收益 16.87%，胜率 55.17%，t=0.90，最大回撤 -15.90%，交易点 87。
- 稳定性：月度正收益比例 75%，半月正收益比例 86%，1.5x 成本后 3.95 bps/次，spot10 压力 10.04 bps/次。
- 实盘处理：Asymmetric beta can flip around liquidation cascades; keep turnover low.
- 曲线：[equity](curves/12_up_down_beta_spread_24h_72h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/12_up_down_beta_spread_24h_72h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 13. `directional_range_efficiency_48h`

- 分组：`directional_range_efficiency` / `liquidity_impact`，与上一轮因子不同点：Directional range efficiency asks whether range expansion has a persistent signed direction.
- 公式：mean(sign(return) * (high-low)/close / log1p(dollar_volume))
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 18.89 bps/次，总收益 10.96%，胜率 50.79%，t=0.68，最大回撤 -12.75%，交易点 63。
- 稳定性：月度正收益比例 75%，半月正收益比例 71%，1.5x 成本后 3.22 bps/次，spot10 压力 8.89 bps/次。
- 实盘处理：This is a pressure-per-liquidity measure; cap exposure during abnormal spreads.
- 曲线：[equity](curves/13_directional_range_efficiency_48h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/13_directional_range_efficiency_48h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 14. `cross_sectional_stress_4h`

- 分组：`cross_sectional_stress` / `relative_shock`，与上一轮因子不同点：Cross-sectional stress scores how extreme a coin's move is relative to same-time dispersion.
- 公式：mean((asset_return - cross_sectional_mean_return) / cross_sectional_std_return)
- 执行：`spread_low_minus_high`，timeframe `1h`，hold `72h`，rebalance 约 `24.0h`。
- 回测：平均净收益 26.34 bps/次，总收益 17.72%，胜率 51.40%，t=0.58，最大回撤 -35.71%，交易点 107。
- 稳定性：月度正收益比例 75%，半月正收益比例 75%，1.5x 成本后 -2.87 bps/次，spot10 压力 16.34 bps/次。
- 实盘处理：This is a dispersion shock, not a raw momentum signal; keep it de-correlated from ret factors.
- 曲线：[equity](curves/14_cross_sectional_stress_4h_1h_72h_spread_low_minus_high_equity.png)，[monthly bars](curves/14_cross_sectional_stress_4h_1h_72h_spread_low_minus_high_monthly_bars.png)

### 15. `sign_imbalance_4h`

- 分组：`sign_imbalance` / `nonparametric_trend`，与上一轮因子不同点：Sign imbalance is a non-parametric trend/reversal signal based only on the count of up and down bars.
- 公式：mean(sign(close_return))
- 执行：`long_high`，timeframe `1h`，hold `48h`，rebalance 约 `24.0h`。
- 回测：平均净收益 31.03 bps/次，总收益 7.17%，胜率 45.16%，t=0.40，最大回撤 -45.43%，交易点 93。
- 稳定性：月度正收益比例 75%，半月正收益比例 50%，1.5x 成本后 16.99 bps/次，spot10 压力 21.03 bps/次。
- 实盘处理：More robust than magnitude returns but can lag during violent reversals.
- 曲线：[equity](curves/15_sign_imbalance_4h_1h_48h_long_high_equity.png)，[monthly bars](curves/15_sign_imbalance_4h_1h_48h_long_high_monthly_bars.png)

### 16. `vwap_slope_24h`

- 分组：`vwap_slope` / `volume_price_inventory`，与上一轮因子不同点：VWAP slope captures whether traded inventory is repricing upward or downward across horizons.
- 公式：VWAP(short_window) / VWAP(long_window) - 1
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 24.66 bps/次，总收益 14.04%，胜率 49.15%，t=0.87，最大回撤 -12.17%，交易点 59。
- 稳定性：月度正收益比例 50%，半月正收益比例 43%，1.5x 成本后 9.49 bps/次，spot10 压力 14.66 bps/次。
- 实盘处理：Works best after filtering low turnover names where VWAP is stale.
- 曲线：[equity](curves/16_vwap_slope_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/16_vwap_slope_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 17. `vwap_gap_48h`

- 分组：`vwap_gap` / `volume_price_inventory`，与上一轮因子不同点：VWAP gap measures where price sits versus volume-weighted inventory, not simple moving-average distance.
- 公式：close / rolling_sum(typical_price * volume) * rolling_sum(volume) - 1
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 22.30 bps/次，总收益 13.49%，胜率 50.79%，t=0.84，最大回撤 -12.58%，交易点 63。
- 稳定性：月度正收益比例 50%，半月正收益比例 29%，1.5x 成本后 7.20 bps/次，spot10 压力 12.30 bps/次。
- 实盘处理：Use as a basket signal only; high spread names can have misleading VWAP gaps.
- 曲线：[equity](curves/17_vwap_gap_48h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/17_vwap_gap_48h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 18. `relative_vol_shock_24h`

- 分组：`relative_vol_shock` / `relative_volatility`，与上一轮因子不同点：Relative volatility shock compares a coin's risk expansion to the simultaneous cross-section.
- 公式：z(realized_vol - cross_sectional_average_realized_vol)
- 执行：`short_low`，timeframe `5m`，hold `12h`，rebalance 约 `12.0h`。
- 回测：平均净收益 10.17 bps/次，总收益 15.17%，胜率 43.10%，t=0.66，最大回撤 -22.46%，交易点 174。
- 稳定性：月度正收益比例 75%，半月正收益比例 57%，1.5x 成本后 2.60 bps/次，spot10 压力 0.17 bps/次。
- 实盘处理：If selected, size down because the edge comes with volatile fills.
- 曲线：[equity](curves/18_relative_vol_shock_24h_5m_12h_short_low_equity.png)，[monthly bars](curves/18_relative_vol_shock_24h_5m_12h_short_low_monthly_bars.png)

### 19. `lead_market_response_24h_72h`

- 分组：`lead_market_response` / `lead_lag`，与上一轮因子不同点：Lead market response tests whether a coin tends to move before the broad market.
- 公式：corr(asset_return_t-1, market_return_t)
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 16.73 bps/次，总收益 9.46%，胜率 45.90%，t=0.66，最大回撤 -9.51%，交易点 61。
- 稳定性：月度正收益比例 50%，半月正收益比例 57%，1.5x 成本后 0.47 bps/次，spot10 压力 6.73 bps/次。
- 实盘处理：It can be unstable; treat as basket-level signal, not single-coin prediction.
- 曲线：[equity](curves/19_lead_market_response_24h_72h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/19_lead_market_response_24h_72h_5m_24h_spread_low_minus_high_monthly_bars.png)

### 20. `break_count_balance_24h`

- 分组：`break_count_balance` / `breakout_frequency`，与上一轮因子不同点：Break-count balance counts repeated high breaks versus low breaks instead of measuring the size of those moves.
- 公式：mean(1[new_high] - 1[new_low])
- 执行：`spread_low_minus_high`，timeframe `5m`，hold `24h`，rebalance 约 `24.0h`。
- 回测：平均净收益 6.90 bps/次，总收益 4.03%，胜率 48.78%，t=0.30，最大回撤 -14.00%，交易点 82。
- 稳定性：月度正收益比例 75%，半月正收益比例 71%，1.5x 成本后 -8.58 bps/次，spot10 压力 -3.10 bps/次。
- 实盘处理：Repeated breaks can be trend or exhaustion; rely on selected direction, not intuition.
- 曲线：[equity](curves/20_break_count_balance_24h_5m_24h_spread_low_minus_high_equity.png)，[monthly bars](curves/20_break_count_balance_24h_5m_24h_spread_low_minus_high_monthly_bars.png)

## 多阶段明细

下面只展示前 80 行月度明细；完整数据在 `factor_phase_metrics.csv`。

| factor_id | phase | trades | net_mean_bps | total_return_pct | win_rate | max_drawdown_pct |
| --- | --- | --- | --- | --- | --- | --- |
| vwap_gap_48h | 2026-01 | 9 | -79.9748 | -7.0151 | 0.2222 | -6.9396 |
| vwap_gap_48h | 2026-02 | 23 | -12.5341 | -3.3730 | 0.4783 | -8.0315 |
| vwap_gap_48h | 2026-03 | 12 | 19.5142 | 2.1371 | 0.5000 | -6.0617 |
| vwap_gap_48h | 2026-04 | 19 | 114.6693 | 23.6726 | 0.6842 | -3.1448 |
| lead_market_response_24h_72h | 2026-01 | 9 | -11.9074 | -1.2515 | 0.4444 | -4.4461 |
| lead_market_response_24h_72h | 2026-02 | 22 | 16.6860 | 3.2003 | 0.4091 | -7.7671 |
| lead_market_response_24h_72h | 2026-03 | 11 | -22.0712 | -2.5302 | 0.3636 | -6.1410 |
| lead_market_response_24h_72h | 2026-04 | 19 | 52.8222 | 10.2029 | 0.5789 | -5.8178 |
| wick_imbalance_48h | 2026-01 | 9 | 219.9287 | 21.1904 | 0.6667 | -3.6396 |
| wick_imbalance_48h | 2026-02 | 28 | 30.2652 | 6.1590 | 0.5714 | -20.0116 |
| wick_imbalance_48h | 2026-03 | 28 | 47.1894 | 13.1512 | 0.5357 | -5.5417 |
| wick_imbalance_48h | 2026-04 | 19 | 12.2402 | 0.6478 | 0.5789 | -15.7549 |
| session_asia_flow_24h | 2026-01 | 8 | 72.5647 | 5.8656 | 0.6250 | -2.8808 |
| session_asia_flow_24h | 2026-02 | 23 | 110.9585 | 28.2916 | 0.7826 | -3.8180 |
| session_asia_flow_24h | 2026-03 | 10 | 28.2118 | 2.7614 | 0.5000 | -2.3576 |
| session_asia_flow_24h | 2026-04 | 19 | 23.3391 | 4.2414 | 0.5263 | -3.4178 |
| range_asymmetry_48h | 2026-01 | 9 | 109.2777 | 9.9003 | 0.7778 | -5.0198 |
| range_asymmetry_48h | 2026-02 | 28 | 64.7358 | 18.2008 | 0.5714 | -13.8511 |
| range_asymmetry_48h | 2026-03 | 30 | 88.2672 | 29.1387 | 0.6333 | -6.2569 |
| range_asymmetry_48h | 2026-04 | 19 | 48.8939 | 9.2392 | 0.5263 | -4.7736 |
| volume_weighted_return_24h | 2026-01 | 9 | 64.7617 | 5.7868 | 0.4444 | -2.2080 |
| volume_weighted_return_24h | 2026-02 | 22 | 90.9958 | 21.1541 | 0.6818 | -3.9922 |
| volume_weighted_return_24h | 2026-03 | 11 | 28.4148 | 2.9400 | 0.6364 | -3.2535 |
| volume_weighted_return_24h | 2026-04 | 19 | 49.9389 | 8.9218 | 0.6316 | -14.5671 |
| false_breakout_supply_24h | 2026-01 | 18 | 130.9304 | 21.4676 | 0.6667 | -12.5440 |
| false_breakout_supply_24h | 2026-02 | 11 | 382.1717 | 48.5277 | 0.7273 | -10.5365 |
| false_breakout_supply_24h | 2026-03 | 14 | 71.8860 | 9.3010 | 0.5000 | -11.9265 |
| false_breakout_supply_24h | 2026-04 | 12 | -145.1731 | -16.6221 | 0.4167 | -18.1869 |
| break_count_balance_24h | 2026-01 | 8 | 53.1978 | 4.1586 | 0.6250 | -4.1959 |
| break_count_balance_24h | 2026-02 | 27 | 11.8491 | 2.5398 | 0.5185 | -10.7711 |
| break_count_balance_24h | 2026-03 | 28 | -28.4253 | -8.2608 | 0.4286 | -13.5802 |
| break_count_balance_24h | 2026-04 | 19 | 32.4500 | 6.1698 | 0.4737 | -3.4488 |
| extreme_recency_48h | 2026-01 | 9 | 172.2813 | 16.2079 | 0.6667 | -3.5263 |
| extreme_recency_48h | 2026-02 | 28 | 42.9770 | 11.4223 | 0.5714 | -16.1855 |
| extreme_recency_48h | 2026-03 | 31 | -11.5729 | -4.1359 | 0.4516 | -4.5514 |
| extreme_recency_48h | 2026-04 | 19 | 16.9830 | 2.4393 | 0.5263 | -11.5467 |
| directional_range_efficiency_48h | 2026-01 | 9 | 51.9397 | 4.6845 | 0.6667 | -2.2795 |
| directional_range_efficiency_48h | 2026-02 | 23 | 45.8443 | 10.2993 | 0.6522 | -7.1537 |
| directional_range_efficiency_48h | 2026-03 | 12 | -85.9285 | -9.9852 | 0.3333 | -7.9642 |
| directional_range_efficiency_48h | 2026-04 | 19 | 36.8059 | 6.7542 | 0.3684 | -4.4175 |
| return_entropy_4h | 2026-01 | 17 | 216.7741 | 34.9215 | 0.5882 | -7.3295 |
| return_entropy_4h | 2026-02 | 12 | 378.6044 | 46.0599 | 0.5833 | -6.2494 |
| return_entropy_4h | 2026-03 | 19 | -21.6555 | -5.2829 | 0.2632 | -10.1723 |
| return_entropy_4h | 2026-04 | 10 | 227.8590 | 19.1745 | 0.4000 | -13.3576 |
| body_volume_corr_24h | 2026-01 | 9 | 14.0394 | 1.0622 | 0.5556 | -3.5311 |
| body_volume_corr_24h | 2026-02 | 22 | 63.4542 | 14.5768 | 0.6818 | -3.0169 |
| body_volume_corr_24h | 2026-03 | 11 | 31.8825 | 3.5181 | 0.7273 | -1.4253 |
| body_volume_corr_24h | 2026-04 | 19 | 4.0619 | 0.1445 | 0.7368 | -9.8380 |
| up_down_beta_spread_24h_72h | 2026-01 | 9 | 19.2118 | 1.6307 | 0.7778 | -3.5088 |
| up_down_beta_spread_24h_72h | 2026-02 | 28 | 35.8110 | 9.3253 | 0.4643 | -15.8985 |
| up_down_beta_spread_24h_72h | 2026-03 | 31 | 24.7916 | 7.5399 | 0.5161 | -3.4645 |
| up_down_beta_spread_24h_72h | 2026-04 | 19 | -10.5710 | -2.1931 | 0.6316 | -4.2351 |
| vwap_slope_24h | 2026-01 | 9 | -5.7248 | -0.5487 | 0.4444 | -2.4784 |
| vwap_slope_24h | 2026-02 | 22 | -39.0795 | -8.9569 | 0.3636 | -12.0339 |
| vwap_slope_24h | 2026-03 | 9 | 38.9184 | 3.4752 | 0.6667 | -3.8362 |
| vwap_slope_24h | 2026-04 | 19 | 106.1004 | 21.7170 | 0.5789 | -2.1638 |
| cross_sectional_stress_4h | 2026-01 | 31 | -75.4274 | -25.6116 | 0.4839 | -35.7108 |
| cross_sectional_stress_4h | 2026-02 | 28 | 49.2039 | 13.0587 | 0.5357 | -8.6709 |
| cross_sectional_stress_4h | 2026-03 | 31 | 44.7799 | 12.2969 | 0.4839 | -16.8319 |
| cross_sectional_stress_4h | 2026-04 | 17 | 140.6324 | 24.6425 | 0.5882 | -7.1799 |
| corr_breakdown_24h_72h | 2026-01 | 9 | 155.8799 | 14.5412 | 0.6667 | -4.5182 |
| corr_breakdown_24h_72h | 2026-02 | 22 | 42.9486 | 8.2227 | 0.4545 | -15.2105 |
| corr_breakdown_24h_72h | 2026-03 | 11 | 5.5481 | 0.3388 | 0.5455 | -4.9517 |
| corr_breakdown_24h_72h | 2026-04 | 19 | -1.1505 | -1.3112 | 0.4737 | -15.1505 |
| sign_imbalance_4h | 2026-01 | 27 | 57.9377 | 3.1380 | 0.3704 | -35.3210 |
| sign_imbalance_4h | 2026-02 | 22 | 89.5250 | 13.4858 | 0.3636 | -17.8749 |
| sign_imbalance_4h | 2026-03 | 29 | -34.5975 | -11.0393 | 0.4828 | -13.6608 |
| sign_imbalance_4h | 2026-04 | 15 | 23.7094 | 2.9232 | 0.6667 | -10.0762 |
| session_flow_rotation_24h | 2026-01 | 8 | 53.6798 | 4.2509 | 0.6250 | -3.7555 |
| session_flow_rotation_24h | 2026-02 | 23 | 81.7821 | 19.6835 | 0.7391 | -7.3893 |
| session_flow_rotation_24h | 2026-03 | 8 | 54.6549 | 4.3355 | 0.7500 | -2.5757 |
| session_flow_rotation_24h | 2026-04 | 20 | 35.5807 | 6.7276 | 0.4500 | -7.6725 |
| turnover_entropy_48h | 2026-01 | 9 | 354.9239 | 36.0053 | 0.8889 | -2.7138 |
| turnover_entropy_48h | 2026-02 | 24 | 29.1445 | 4.8627 | 0.5833 | -16.5116 |
| turnover_entropy_48h | 2026-03 | 27 | 77.5688 | 21.2480 | 0.6296 | -21.0975 |
| turnover_entropy_48h | 2026-04 | 18 | -71.2499 | -12.5279 | 0.3333 | -19.3651 |
| relative_vol_shock_24h | 2026-01 | 17 | 85.6937 | 15.2706 | 0.6471 | -4.8158 |
| relative_vol_shock_24h | 2026-02 | 56 | 8.4553 | 3.0529 | 0.4464 | -22.4594 |
| relative_vol_shock_24h | 2026-03 | 62 | 12.6414 | 7.3931 | 0.4194 | -7.0898 |
| relative_vol_shock_24h | 2026-04 | 39 | -24.2013 | -9.7188 | 0.3333 | -16.1250 |

## 输出文件

- `all_mined_factor_results.csv`：训练/测试初筛后的全部候选
- `validation_candidates.csv`：进入非重叠稳定性验证的候选
- `factor_validation_summary.csv`：候选稳定性总表
- `factor_phase_metrics.csv`：月度/半月度分阶段表现
- `top20_diverse_factors.csv`：最终 20 个不同分组因子
- `curves/`：逐因子和 Top20 等权组合曲线、收益序列