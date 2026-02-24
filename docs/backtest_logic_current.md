# 当前回测逻辑说明（`crypto_trading_system`）

更新时间：当前仓库版本（含高频成本模型升级）

## 0. 先说结论（很重要）

你当前仓库里实际上有 **两套回测逻辑**：

1. **Web/API 用的简化向量化回测**（`web/api/backtest.py`）
- 主要用于页面回测、策略对比、快速优化
- 基于仓内生成的 `position` 序列计算收益与成本
- 成本字段较简化（手续费 + 滑点 bps）

2. **`core/backtest/backtest_engine.py` 事件驱动回测引擎**
- 适配 `StrategyBase` 的 `Signal`（BUY/SELL/CLOSE_*）
- 逐 bar 执行信号、维护持仓/资金/权益
- 现在已支持更细成本分解：
  - `maker/taker`
  - 动态滑点
  - 可选资金费率（funding）
  - trade 级 `gross/fee/slippage/funding/net`

建议：
- **研究与高频策略评估**优先使用 `core/backtest/backtest_engine.py`
- **前端快速试错/批量策略对比**继续用 `web/api/backtest.py`

---

## 1. Web/API 简化回测（`web/api/backtest.py`）

## 1.1 核心流程（向量化）

典型入口（页面回测接口）会走 `_run_backtest_core(...)`，流程如下：

1. 加载 OHLCV 数据（`_load_backtest_df`）
2. 根据策略生成仓位序列（`_build_positions(strategy, df, ...)`）
3. 计算 bar 收益：
- `returns = close.pct_change()`
- `gross_returns = position.shift(1) * returns`
4. 计算换手与交易成本：
- `turnover = abs(position.diff())`
- `trade_cost = turnover * (fee_rate + slip_rate)`
5. 得到净收益序列：
- `strategy_returns = gross_returns - trade_cost`
6. 生成权益曲线、回撤、Sharpe、交易统计

## 1.2 特点

优点：
- 快
- 适合 UI 即时回测与多策略对比

限制：
- 不是逐订单事件记账
- 对部分复杂策略/部分成交/资金费率/订单状态不够精细

---

## 2. 事件驱动回测引擎（`core/backtest/backtest_engine.py`）

## 2.1 输入与驱动方式

输入：
- `StrategyBase` 子类实例（输出 `Signal` 列表）
- 单标的 OHLCV DataFrame（DatetimeIndex）

驱动方式：
- 按 bar 逐步推进
- 每根 K 线时：
  1. 更新持仓估值
  2. 让策略读取 `data.iloc[:i+1]`
  3. 生成信号
  4. 执行信号（开仓/平仓）
  5. 记录权益曲线

---

## 2.2 持仓与资金模型（当前实现）

当前实现是“**保证金近似模型**”（适合零售高频研究，不是交易所级逐笔保证金系统）：

- 开仓时：
  - 使用 `position_size_pct * capital` 作为保证金
  - `notional = margin * leverage`
  - 从现金中扣除 `margin + fee`
- 持仓估值：
  - `position_value = margin + unrealized_pnl + funding_pnl`
- 平仓时：
  - 把 `margin + net_pnl` 加回现金

优点：
- 比“全额买入模型”更适合永续/杠杆研究
- 能处理多空与资金费率

限制：
- 没有逐交易所维持保证金/强平逻辑
- 没有真实撮合与部分成交

---

## 2.3 成本模型（升级后）

`BacktestConfig` 新增：

- `fee_model`: `"flat"` | `"maker_taker"`
- `maker_fee`, `taker_fee`
- `slippage_model`: `"flat"` | `"dynamic"`
- `dynamic_slip = {min_slip, k_atr, k_rv, k_spread}`
- `include_funding`
- `funding_source`（当前主要是字段占位）
- `funding_interval_hours`

### 2.3.1 手续费

`flat`：
- 使用 `commission_rate`

`maker_taker`：
- 使用 `maker_fee / taker_fee`
- 默认角色 `taker`
- 后续可通过信号 metadata 指定 `execution_role`

### 2.3.2 动态滑点（无盘口近似）

当前使用代理特征：
- `atr_pct`
- `realized_vol`
- `spread_proxy = (high-low)/close`

公式：

```text
slip_rate = max(min_slip, k_atr*atr_pct + k_rv*realized_vol + k_spread*spread_proxy)
```

说明：
- 这是研究近似，不是盘口级冲击成本模型

---

## 2.4 资金费率（funding）

当前支持（默认可关）：
- 如果数据中存在 `funding_rate` 列，且 `include_funding=True`
- 在资金费率结算边界（默认 8 小时）跨越时记入资金费率现金流

符号约定（当前实现）：
- `funding_rate > 0` 时：
  - `long` 付费（负）
  - `short` 收费（正）

会生成 `trade_stage="funding"` 的账务记录，方便审计与分解。

---

## 2.5 交易记录与 PnL 分解（trade 级）

`BacktestTrade` 关键字段（升级后）：
- `gross_pnl`
- `fee`
- `slippage_cost`
- `funding_pnl`
- `net_pnl`
- `notional`
- `trade_stage` (`open | close | funding`)

兼容旧字段（保留）：
- `commission`
- `slippage`
- `pnl`

说明：
- `pnl` 兼容字段主要用于旧统计逻辑
- 新逻辑建议使用 `net_pnl` 与 `trade_stage`

---

## 2.6 结果汇总（`BacktestResult`）

除了原有指标外，新增：
- `cost_breakdown`
  - `gross_pnl`
  - `fee`
  - `slippage_cost`
  - `funding_pnl`
  - `net_pnl`
  - `realized_total`
- `turnover_notional`

这使得高频研究可以直接看：
- 成本拖累占比
- 手续费 vs 滑点 vs funding 的贡献

---

## 3. 高频策略（当前新增）

`strategies/quantitative/multi_factor_hf.py`

特点：
- YAML 配置驱动（`config/strategy_multi_factor_hf.yaml`）
- 支持多空（输出 `BUY/SELL/CLOSE_LONG/CLOSE_SHORT`）
- 滞回阈值（`enter_th > exit_th`）
- gate 过滤：
  - `realized_vol`
  - `atr_pct`
  - `spread_proxy`
  - `volume_z`
- `cooldown_bars`
- metadata 输出：
  - 因子值
  - score
  - gate 状态
  - 成本估计代理

---

## 4. 当前建议的使用方式

### 用于页面快速对比
- 继续用 `web/api/backtest.py`（快）

### 用于 5m 永续高频研究
- 用 `core/backtest/backtest_engine.py` + `MultiFactorHFStrategy`
- 配合 `scripts/research/all_reports.py` 跑：
  - data QA
  - factor study
  - cost sensitivity
  - walk-forward
  - robustness

---

## 5. 目前已知限制（后续可优化）

1. 事件驱动回测还是单标的 DataFrame 输入（多标的组合回测需额外 orchestration）
2. 资金费率历史现在支持通过 `core/backtest/funding_provider.py` 自动补齐（本地缓存优先，Binance HTTP 公共 funding 接口回填）；若未配置 provider，仍沿用数据列 `funding_rate`
3. 滑点仍为“无盘口代理模型”，未接入真实 orderbook 冲击估计
4. UI 回测与事件驱动回测尚未完全统一到同一记账引擎（字段已开始统一）
