# 小资金、无延时优势条件下的三类 Crypto 策略设计

更新日期：2026-05-20  
适用约束：资金体量较小、手续费较高、没有低延时/队列位置优势、不希望高频换手。  
策略范围：Liquidation/OI Crowding、Token Unlock / Supply Event、On-Chain Exchange Flow 2.0。本文是系统设计文档，不构成投资建议。

## 1. 总体判断

在当前约束下，不应该把收益来源建立在“更快成交、更快撤单、更低手续费”上，而应该建立在：

- 低频结构性信息：杠杆拥挤、清算后重置、代币释放、交易所库存变化。
- 组合风控：先减少大亏和错误交易，再追求单策略收益。
- 少换手：宁可错过一部分机会，也不在噪音区间反复进出。
- 可解释信号：每笔交易必须能解释为“杠杆结构、供给冲击、链上库存”中的一种。
- 事件窗交易：用小时到天级别的窗口，而不是秒级抢跑。

因此最合适的策略族不是做市、跨所搬砖、MEV、裸高频，而是：

1. `LiquidationOICrowdingStrategy`：杠杆拥挤度与清算后重置。
2. `SupplyEventStrategy`：token unlock、listing、delisting、airdrop、staking unlock 等供给/交易事件。
3. `OnChainFlowRegimeStrategy`：交易所净流入/净流出、储备变化、稳定币购买力与鲸鱼行为。

三者应当组合使用：

- `Liquidation/OI` 负责衍生品杠杆结构和短中期风险状态。
- `Supply Event` 负责未来已知事件和供给冲击。
- `On-Chain Flow` 负责慢变量趋势背景和仓位权重。

推荐先做成“信号 + gate + 仓位修正”的混合系统，而不是三个彼此孤立的交易机器人。

## 2. 当前系统可复用基础

现有系统已经有不少可直接复用的模块：

- 策略接口：`core/strategies/strategy_base.py`
  - `StrategyBase.generate_signals(data)`
  - `Signal.metadata`
  - `Signal.strength`
- 回测引擎：`core/backtest/backtest_engine.py`
  - 已支持 maker/taker fee、slippage、shorting、funding cashflow。
  - 需要为事件/链上/衍生品特征增加特征列或外部 context feed。
- 衍生品与 CoinGlass：
  - `core/ai/coinglass_signal.py`
  - `core/data/coinglass_feature_builder.py`
  - `core/data/coinglass_onchain.py`
- Funding：
  - `core/data/funding_rate_collector.py`
- 订单簿：
  - `core/data/orderbook/orderbook_collector.py`
- 市场状态：
  - `core/market_state/classifier.py`

优先不要重写这些能力，而是补三个“策略化适配层”：

```text
Data collectors -> Feature snapshots -> Strategy context -> Signals/Gates -> Backtest/Paper/Live
```

## 3. 组合架构

建议新增一个统一的结构性信号上下文：

```python
StructuralMarketContext = {
    "symbol": "BTC/USDT",
    "timestamp": "...",
    "derivatives": {
        "oi_change_z": 0.0,
        "funding_z": 0.0,
        "basis_z": 0.0,
        "long_short_ratio_z": 0.0,
        "taker_imbalance_z": 0.0,
        "liquidation_burst_score": 0.0,
        "crowding_side": "long|short|neutral",
    },
    "events": {
        "active_events": [],
        "event_risk_score": 0.0,
        "supply_pressure_score": 0.0,
    },
    "onchain": {
        "exchange_netflow_z": 0.0,
        "exchange_balance_change_z": 0.0,
        "stablecoin_balance_change_z": 0.0,
        "whale_flow_score": 0.0,
        "onchain_regime": "accumulation|distribution|neutral|unknown",
    },
    "execution": {
        "spread_bps": 0.0,
        "depth_score": 0.0,
        "fee_bps": 0.0,
    },
}
```

这个 context 的输出不是直接等于买卖，而是先生成三层决策：

- `trade_signal`：是否开仓，方向是什么。
- `risk_gate`：是否禁止某些方向，是否降低仓位。
- `position_scalar`：仓位乘数，范围建议 `0.0 - 1.0`。

最终信号合成：

```text
final_bias
= 0.45 * liquidation_oi_bias
+ 0.30 * supply_event_bias
+ 0.25 * onchain_flow_bias

final_position_pct
= base_position_pct
* liquidation_gate_scalar
* event_gate_scalar
* onchain_regime_scalar
* execution_cost_scalar
```

对于小资金和高费率，默认只在强信号时交易：

- `abs(final_bias) < 0.35`：不交易。
- `0.35 <= abs(final_bias) < 0.55`：只允许减仓/风控，不新开仓。
- `abs(final_bias) >= 0.55`：允许小仓开仓。
- `abs(final_bias) >= 0.75`：允许标准仓位。

## 4. 策略一：Liquidation/OI Crowding Strategy

### 4.1 策略定位

这是三者里最优先实现的策略。它的主要价值不是天天开仓，而是识别“杠杆结构危险区”和“清算后重置区”：

- 危险区：OI 快速扩张、funding 极端、long/short 一边倒、盘口变薄。
- 重置区：清算爆发后 OI 下降、funding 回落、价格偏离过度但卖压/买压衰竭。

建议同时做两个模式：

- `Gate mode`：作为全局风控，减少现有策略在拥挤区的仓位。
- `Trade mode`：只在清算后重置或拥挤突破确认时开小仓。

### 4.2 数据输入

优先数据：

- Open Interest：`oi`, `oi_value`, `oi_change_1h`, `oi_change_24h`
- Funding：`funding_rate`, `funding_z`, `funding_percentile`
- Long/Short：`global_long_short_ratio`, `top_trader_position_ratio`
- Taker flow：`taker_buy_sell_ratio`, `taker_imbalance`
- Basis/Premium：`mark_index_basis`, `basis_z`
- Liquidation：`liquidation_long_usd`, `liquidation_short_usd`, `liquidation_burst_score`
- Execution：`spread_bps`, `depth_imbalance`, `depth_score`

Binance 官方 API 中，funding history 使用 `/fapi/v1/fundingRate`，OI statistics 使用 `/futures/data/openInterestHist`，这两个足以先搭出可回测版本。若 CoinGlass 可用，再补 liquidation heatmap 和聚合交易所衍生品快照。

### 4.3 特征定义

基础特征：

```text
oi_change_1h = OI_t / OI_t-1h - 1
oi_change_24h = OI_t / OI_t-24h - 1
oi_change_z = zscore(oi_change_1h, lookback=14d)

funding_z = zscore(funding_rate, lookback=30d)
funding_annualized = funding_rate * 3 * 365

taker_imbalance = (taker_buy_volume - taker_sell_volume) / total_taker_volume

basis_pct = (mark_price - index_price) / index_price
basis_z = zscore(basis_pct, lookback=30d)

liquidation_burst_score = zscore(liquidation_usd, lookback=14d), clipped to 0..1
```

拥挤方向：

```text
crowded_long_score =
  0.30 * clip_z(oi_change_z)
+ 0.25 * clip_z(funding_z)
+ 0.20 * clip_z(long_short_ratio_z)
+ 0.15 * clip_z(basis_z)
+ 0.10 * clip_z(taker_buy_imbalance_z)

crowded_short_score =
  0.30 * clip_z(oi_change_z)
+ 0.25 * clip_z(-funding_z)
+ 0.20 * clip_z(-long_short_ratio_z)
+ 0.15 * clip_z(-basis_z)
+ 0.10 * clip_z(-taker_buy_imbalance_z)
```

盘口风险：

```text
execution_risk_score =
  0.50 * spread_bps_percentile
+ 0.30 * low_depth_percentile
+ 0.20 * abs(depth_imbalance)
```

### 4.4 信号规则

规则 A：拥挤区风控，不主动反手。

```text
if crowded_long_score >= 0.75 and funding_z > 1.5:
    block_new_longs = True
    reduce_long_position_scalar = 0.3 - 0.7

if crowded_short_score >= 0.75 and funding_z < -1.5:
    block_new_shorts = True
    reduce_short_position_scalar = 0.3 - 0.7
```

规则 B：清算后反转。

```text
long_flush_reversal:
    previous crowded_long_score >= 0.70
    long_liquidation_burst_score >= 0.80
    oi_change_after_flush < -threshold
    funding_z falls toward neutral
    price_return_1h < -k * atr_pct
    taker_sell_pressure starts weakening

signal: BUY, small size
```

反向同理：

```text
short_squeeze_reversal:
    previous crowded_short_score >= 0.70
    short_liquidation_burst_score >= 0.80
    OI reset
    funding_z normalizes
    price_return_1h > k * atr_pct
    taker_buy_pressure starts weakening

signal: SELL or CLOSE_LONG depending on shorting availability
```

规则 C：拥挤突破，只做确认后的顺势。

```text
if oi_change_z > 1.5
and funding_z not extreme
and price breaks Donchian/channel
and taker_imbalance confirms direction
and execution_risk_score < 0.65:
    allow trend-following entry
```

对于你的约束，规则 C 仓位要小，优先做规则 A 和 B。

### 4.5 仓位与退出

默认参数：

```yaml
base_position_pct: 0.04
max_position_pct: 0.08
min_signal_strength: 0.55
hard_stop_atr_mult: 1.8
take_profit_atr_mult: 2.4
max_holding_hours: 36
cooldown_hours_after_loss: 12
max_trades_per_symbol_per_day: 2
```

退出：

- 反转失败：价格继续朝不利方向移动 `1.5-2.0 ATR`。
- 重置完成：funding/oi/liquidation 指标回到中性且价格反弹达到目标。
- 时间止损：清算后反转信号 24-36 小时没有兑现。
- 风险止损：spread/depth 恶化或出现二次清算风险。

### 4.6 回测验收

必须单独报告：

- 拥挤区 gate 是否减少现有策略最大回撤。
- 清算后反转交易的胜率、盈亏比、持仓时间。
- funding 极值行情下是否避免追高/杀跌。
- 手续费占 gross pnl 比例，建议低于 `25%-35%`。
- 每月交易次数，不应过高。

上线门槛：

- Paper 运行至少 30 天。
- 最近 50 个信号中，解释字段完整率 100%。
- 任意 7 天内连续亏损次数超过 3 次时自动降权。

## 5. 策略二：Token Unlock / Supply Event Strategy

### 5.1 策略定位

这是最适合小资金的低频策略之一，因为它不要求你比别人快几毫秒，而是要求你：

- 提前知道事件日历。
- 判断事件是否已经被价格消化。
- 只在流动性允许、费用可接受、风险收益比足够时交易。

它不能简化成“unlock 就做空”。更合理的是事件窗模型：

```text
T-30 -> T-14 -> T-7 -> T-1 -> T0 -> T+1 -> T+7 -> T+14
```

不同窗口做不同事：

- 事件前：评估供给压力和市场是否提前定价。
- 事件中：避免公告/开放交易瞬间追单。
- 事件后：观察供给是否被吸收，寻找反转或延续。

### 5.2 事件类型

第一版建议支持：

- `token_unlock`：team、investor、ecosystem、treasury、community unlock。
- `exchange_listing`：spot listing、perp listing、margin listing。
- `exchange_delisting`：delisting、monitoring tag、forced conversion。
- `airdrop_claim`：大规模 claim 开放。
- `staking_unlock`：staking withdrawal、validator unlock、lock period ends。
- `emission_change`：halving、rewards cut、inflation schedule change。

### 5.3 事件 schema

建议新增统一事件记录：

```python
SupplyEvent = {
    "event_id": "ARB_unlock_2026_06_16",
    "symbol": "ARB/USDT",
    "base_asset": "ARB",
    "event_type": "token_unlock",
    "event_time": "2026-06-16T00:00:00Z",
    "first_seen_at": "2026-05-20T00:00:00Z",
    "source": "token_unlocks|exchange_announcement|manual",
    "source_url": "...",
    "unlock_tokens": 0.0,
    "unlock_usd": 0.0,
    "unlock_pct_circ": 0.0,
    "unlock_pct_float": 0.0,
    "recipient_type": "team|investor|ecosystem|community|unknown",
    "confidence": 0.0,
    "metadata": {},
}
```

关键字段是 `first_seen_at`。回测必须用首次可见时间，而不是事后整理好的事件时间。

### 5.4 特征定义

事件压力：

```text
supply_pressure_score =
  0.35 * rank(unlock_pct_float)
+ 0.25 * rank(unlock_usd / avg_daily_volume_30d)
+ 0.20 * recipient_risk_score
+ 0.10 * low_liquidity_score
+ 0.10 * weak_market_regime_score
```

事件是否提前定价：

```text
pre_event_price_move =
  return(T-14 to T-1) relative to sector/index

pre_event_crowding =
  funding_z + oi_change_z + short_interest_proxy

priced_in_score =
  0.50 * abs(pre_event_price_move_z)
+ 0.30 * abs(pre_event_crowding)
+ 0.20 * news_mentions_z
```

事件后吸收：

```text
absorption_score =
  0.35 * price_holds_above_event_low
+ 0.25 * volume_above_average_without_new_low
+ 0.20 * funding_normalizes
+ 0.20 * exchange_netflow_not_worsening
```

### 5.5 信号规则

规则 A：事件前风险 gate。

```text
if supply_pressure_score >= 0.70
and event_time - now <= 14d
and priced_in_score < 0.60:
    block_new_longs = True
    reduce_existing_longs = 0.3 - 0.7
```

这条规则最重要。它可以不赚钱，但要防止在大额 unlock 前被动接盘。

规则 B：事件前小仓顺势。

```text
if supply_pressure_score >= 0.75
and liquidity_score >= 0.60
and borrow_or_perp_available
and market_regime not bullish
and no excessive short crowding:
    signal = SELL
```

注意：如果 short 已经非常拥挤，宁可不做。

规则 C：事件后吸收反转。

```text
if event already passed by 1d-7d
and absorption_score >= 0.70
and price recovers event VWAP
and funding reset or short crowding high:
    signal = BUY
```

这条往往更适合小资金，因为它不需要提前承受事件前不确定性。

规则 D：listing 事件。

```text
spot_listing:
    avoid market order during first minutes/hours
    wait for first volatility compression
    trade only after volume and spread normalize

perp_listing:
    monitor funding, OI expansion, initial basis
    avoid chasing first spike
    prefer post-listing mean reversion after extreme move
```

### 5.6 仓位与退出

默认参数：

```yaml
base_position_pct: 0.03
max_position_pct: 0.06
max_event_risk_per_month: 0.12
event_blackout_hours_before: 2
event_blackout_hours_after: 6
max_holding_days_pre_event: 10
max_holding_days_post_event: 14
```

退出：

- 事件前交易：T-1 到 T0 前逐步减仓，避免事件瞬间流动性风险。
- 事件后交易：吸收失败跌破 event low，立即退出。
- Listing 交易：spread 超阈值或成交量衰竭，退出。
- 一旦新闻源修正事件时间或 unlock 数量，重新计算 score。

### 5.7 回测验收

事件策略容易出现幸存者偏差，验收要更严格：

- 每个事件必须保存 `first_seen_at`。
- 对 delisted/死亡项目不能从样本里删除。
- 同类事件按市值、流动性、recipient_type 分桶评估。
- 不只看平均收益，要看左尾风险和极端亏损。
- 统计 “不交易 gate” 对避免亏损的贡献。

上线门槛：

- 先做 90 天事件回放。
- 只允许交易 `ADV_30d` 足够高、spread 足够低、有永续或可做空渠道的标的。
- 单事件最大损失不超过账户权益 `0.5%-1.0%`。

## 6. 策略三：On-Chain Exchange Flow Strategy 2.0

### 6.1 策略定位

On-chain flow 不适合做很短线的独立开仓信号。它更适合作为：

- 中期 regime 判断。
- 仓位乘数。
- 风险过滤器。
- 事件策略和杠杆拥挤策略的确认因子。

你现在已有 `WhaleActivityStrategy` 和 `coinglass_onchain.py`，但应从“单笔大额转账”升级为“交易所库存 + 稳定币购买力 + 净流向”的结构模型。

### 6.2 数据输入

优先数据：

- Spot exchange netflow：交易所净流入/净流出。
- Exchange balance：交易所持币余额及变化。
- Stablecoin exchange balance：稳定币在交易所的余额变化。
- Whale transfers：大额交易所相关转账。
- Derivatives exchange flow：现货交易所和衍生品交易所之间的迁移。
- Optional：realized profit/loss、SOPR、MVRV、active addresses。

关键注意：

- Glassnode 等链上供应商的 exchange labels 会更新，回测要优先用 Point-in-Time 数据。
- 没有 PiT 时，必须在文档和结果中标记 `lookahead_risk=high`。

### 6.3 特征定义

交易所净流：

```text
exchange_netflow_score =
  zscore(exchange_inflow_usd - exchange_outflow_usd, lookback=180d)

negative netflow = coins leave exchanges = supply tight
positive netflow = coins enter exchanges = potential sell pressure
```

交易所储备：

```text
exchange_reserve_pressure =
  zscore(exchange_balance_change_7d, lookback=180d)
```

稳定币购买力：

```text
stablecoin_buying_power =
  zscore(stablecoin_exchange_balance_change_7d, lookback=180d)
```

综合 regime：

```text
accumulation_score =
  0.35 * clip_z(-exchange_netflow_score)
+ 0.25 * clip_z(-exchange_reserve_pressure)
+ 0.25 * clip_z(stablecoin_buying_power)
+ 0.15 * whale_outflow_score

distribution_score =
  0.35 * clip_z(exchange_netflow_score)
+ 0.25 * clip_z(exchange_reserve_pressure)
+ 0.20 * clip_z(-stablecoin_buying_power)
+ 0.20 * whale_inflow_score
```

regime 判定：

```text
if accumulation_score >= 0.65 and distribution_score < 0.50:
    onchain_regime = accumulation

if distribution_score >= 0.65 and accumulation_score < 0.50:
    onchain_regime = distribution

else:
    onchain_regime = neutral
```

### 6.4 信号规则

On-chain 不建议单独频繁交易。推荐作为权重调整：

```text
if onchain_regime == accumulation:
    allow_longs = True
    long_position_scalar *= 1.15
    short_position_scalar *= 0.75

if onchain_regime == distribution:
    allow_shorts = True
    long_position_scalar *= 0.50
    short_position_scalar *= 1.10

if onchain_regime == neutral:
    no adjustment
```

和前两类策略的组合：

```text
Liquidation flush reversal + accumulation regime:
    increase confidence

Liquidation long-crowding risk + distribution regime:
    reduce longs aggressively

Supply unlock pressure + distribution regime:
    stronger long gate, only small short if liquidity is OK

Supply event absorption + accumulation regime:
    best setup for post-event long
```

### 6.5 仓位与退出

默认参数：

```yaml
lookback_days: 180
min_data_days: 90
regime_refresh_hours: 6
max_scalar_up: 1.20
max_scalar_down: 0.40
stale_data_ttl_hours: 24
```

失效规则：

- 数据超过 TTL：regime 回到 neutral。
- 供应商返回异常跳变：进入 degraded，不允许放大仓位。
- exchange netflow 与 price action 连续背离超过 7-14 天：降低权重。

### 6.6 回测验收

On-chain strategy 的验收重点不是单策略收益，而是组合改善：

- 是否降低最大回撤。
- 是否提高已有趋势/事件策略的 profit factor。
- 是否减少在 distribution regime 中做多的亏损。
- 是否避免在 accumulation regime 中过早做空。
- PiT 与非 PiT 数据结果差异有多大。

上线门槛：

- 至少 180 天历史回测。
- 至少 30 天 shadow 运行。
- 所有 on-chain 数据都带 `source`, `as_of`, `freshness`, `lookahead_risk`。

## 7. 三策略合成规则

推荐最终组合不是简单投票，而是分角色：

```text
Liquidation/OI:
    fast structural risk and post-flush trades

Supply Event:
    scheduled risk and event-window trades

On-Chain Flow:
    slow regime and position scalar
```

信号合成：

```text
base_direction =
    liquidation_trade_signal
    or supply_event_trade_signal
    or HOLD

if onchain_regime conflicts with base_direction:
    reduce strength by 20%-50%

if supply_event_gate blocks direction:
    no new entry

if liquidation_gate says high-risk crowding:
    reduce existing exposure

if execution cost too high:
    skip trade
```

示例：

```text
Case A:
  crowded_long_score = 0.82
  distribution_score = 0.70
  no active supply event

Decision:
  block new longs
  reduce existing long exposure
  no immediate short unless flush/reversal confirmation appears
```

```text
Case B:
  large unlock in 5 days
  supply_pressure_score = 0.78
  priced_in_score = 0.35
  onchain_regime = distribution

Decision:
  block new longs
  allow small short only if liquidity and borrow/perp available
  exit before event window if volatility/spread expands
```

```text
Case C:
  unlock passed 3 days ago
  absorption_score = 0.74
  short crowding high
  onchain_regime = accumulation

Decision:
  allow small long
  max holding 7-14 days
  stop below event low
```

## 8. 实现路线

### Phase 1：先做 context 和 gate

目标：不用马上新增三套完整交易策略，先让系统能识别风险。

新增建议：

- `core/structural/context.py`
- `core/structural/derivatives_crowding.py`
- `core/structural/supply_events.py`
- `core/structural/onchain_flow.py`

输出：

- `StructuralMarketContext`
- `RiskGateDecision`
- `PositionScalarDecision`

测试：

- 数据缺失时必须降级为 neutral。
- stale data 不得放大仓位。
- 所有 decision 都必须有 reason codes。

### Phase 2：实现 Liquidation/OI 策略

新增：

- `strategies/quantitative/liquidation_oi_crowding.py`
- registry entry：`LiquidationOICrowdingStrategy`
- tests：
  - `tests/test_liquidation_oi_crowding_strategy.py`
  - `tests/test_structural_context_degraded.py`

先实现：

- Gate mode。
- 清算后反转小仓。
- 不先做拥挤突破顺势。

### Phase 3：实现 Supply Event 策略

新增：

- `core/events/supply_event_store.py`
- `core/events/supply_event_schema.py`
- `strategies/event_driven/supply_event_strategy.py`
- tests：
  - `tests/test_supply_event_schema.py`
  - `tests/test_supply_event_strategy_windows.py`
  - `tests/test_supply_event_no_first_seen_lookahead.py`

先实现：

- 手动/CSV 事件导入。
- 事件前 long gate。
- 事件后 absorption long。

暂缓：

- 自动抓所有 unlock 源。
- listing 首小时交易。

### Phase 4：实现 On-Chain Flow regime

新增：

- `strategies/macro/onchain_flow_regime.py`
- 或先只做 `core/structural/onchain_flow.py` 给其他策略用。

先实现：

- accumulation/distribution/neutral。
- position scalar。
- stale/degraded handling。

暂缓：

- 复杂 SOPR/MVRV 多指标模型。
- 高频链上监控。

## 9. 推荐默认配置

```yaml
structural_strategy_suite:
  enabled: true
  base_position_pct: 0.04
  max_position_pct: 0.08
  max_total_structural_exposure_pct: 0.12
  min_signal_strength_for_entry: 0.55
  min_signal_strength_for_scale_up: 0.75
  max_trades_per_day: 3
  min_hours_between_entries_same_symbol: 12
  taker_fee_bps_assumption: 6
  slippage_bps_assumption: 5

liquidation_oi:
  gate_mode: true
  trade_mode: true
  crowded_score_enter: 0.75
  crowded_score_exit: 0.55
  liquidation_burst_enter: 0.80
  max_holding_hours: 36
  stop_atr_mult: 1.8
  take_profit_atr_mult: 2.4

supply_events:
  gate_mode: true
  trade_mode: true
  pre_event_window_days: 14
  post_event_window_days: 14
  supply_pressure_enter: 0.70
  absorption_enter: 0.70
  event_blackout_hours_before: 2
  event_blackout_hours_after: 6

onchain_flow:
  regime_mode: true
  trade_mode: false
  lookback_days: 180
  min_data_days: 90
  stale_data_ttl_hours: 24
  accumulation_enter: 0.65
  distribution_enter: 0.65
  max_scalar_up: 1.20
  max_scalar_down: 0.40
```

## 10. 最小可行版本

如果只做一个最小版本，建议这样切：

1. `StructuralMarketContext`：把 derivatives/event/onchain 三类特征统一输出。
2. `LiquidationOICrowdingGate`：先只做 long/short block 和 reduce scalar。
3. `SupplyEventStore`：先支持手动 CSV，字段必须有 `first_seen_at`。
4. `OnChainFlowRegime`：先只做 accumulation/distribution/neutral。
5. 在现有策略执行前加一层 `StructuralRiskGate`。

MVP 不需要一开始自动交易，只要能做到：

- 当前 symbol 是否允许开多。
- 当前 symbol 是否允许开空。
- 当前仓位是否应该降权。
- 为什么做这个决定。

这对你的资金条件最实际。先把错误交易砍掉，收益曲线会比盲目增加新策略更稳。

## 11. 资料与接口依据

- Binance Funding Rate History：`GET /fapi/v1/fundingRate`，支持 funding history 与 mark price。
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
- Binance Open Interest Statistics：`GET /futures/data/openInterestHist`，支持 5m 到 1d 粒度，最近 1 个月 OI 统计。
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
- Glassnode Point-in-Time metrics：PiT 指标适合回测，避免链上标签后修正造成未来函数。
  https://docs.glassnode.com/basic-api/endpoints/pit
- Glassnode exchange metrics caveat：exchange labels 会更新，普通 exchange metrics 回测存在历史可变性。
  https://docs.glassnode.com/basic-api/endpoints/transactions

