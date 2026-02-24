# 当前回测逻辑说明（crypto_trading_system）

更新时间：2026-02-24

本文档说明当前仓库内两条回测路径的职责、口径差异、成本模型、资金费率处理，以及页面回测（普通回测 / 多策略对比 / 参数优化）的区间控制规则。

## 1. 总览：当前有两套回测路径

当前系统实际存在两条回测实现，定位不同：

1. `web/api/backtest.py`
- 用途：前端页面的快速回测、多策略对比、参数优化
- 特点：响应快、交互友好、适合 UI 高频调用
- 局限：记账口径较简化，不是完整订单事件驱动

2. `core/backtest/backtest_engine.py`
- 用途：研究主线、5m 永续多空、高频成本分析
- 特点：事件驱动、支持成本分解、支持资金费率、兼容 `StrategyBase/Signal`
- 局限：计算更重，适合研究脚本与离线评估

建议：
- 页面试验、策略筛选：优先用 `web/api/backtest.py`
- 高频研究、成本敏感性、PnL 分解：优先用 `core/backtest/backtest_engine.py`

## 2. 页面回测路径（`web/api/backtest.py`）

## 2.1 用途

该模块服务以下前端功能：

- 普通回测（单策略）
- 多策略对比（支持预优化）
- 参数优化（支持可视化结果）
- 回测结果导出

## 2.2 计算方式（简化向量化）

核心思路：

1. 加载 K 线数据（按交易所、交易对、周期）
2. 基于策略逻辑生成仓位序列或交易信号序列
3. 使用 bar 收益计算策略收益
4. 扣除手续费/滑点近似成本
5. 输出净值曲线、收益率、回撤、夏普、胜率等统计

常见近似公式（说明口径，不代表代码逐字实现）：

```text
returns_t = close_t / close_{t-1} - 1
gross_returns_t = position_{t-1} * returns_t
turnover_t = |position_t - position_{t-1}|
cost_t = turnover_t * (fee_rate + slippage_rate)
net_returns_t = gross_returns_t - cost_t
```

## 2.3 当前页面功能增强（已落地）

已支持：

- 多策略对比可选择“策略库全部 / 已注册策略去重类型”
- 多策略对比支持预优化后再对比
- 参数优化支持日期区间（与页面区间一致）
- 多策略排行榜可点击预览指定策略的指定区间回测
- 参数优化结果支持一键回填到策略参数编辑（并支持注册新实例）
- 回测区间精确到分钟（前端 `datetime-local` + 后端解析）

## 2.4 区间锁定（页面显示口径）

页面回测结果会显示“区间锁定标识”，用于提示是否严格按用户选择区间运行。

状态含义：

- `已锁定`：严格使用页面选择的开始/结束时间
- `未锁定`：未输入区间，使用可用历史数据范围
- `已解锁`：个别普通回测场景下样本不足时，为避免失败做了回退扩展（页面会显示说明）

## 2.5 分钟级区间控制规则

后端支持两种输入格式：

- `YYYY-MM-DD`
- `YYYY-MM-DDTHH:MM`

规则：

- 若输入为纯日期：
  - `start_date` 视为当日 `00:00:00`
  - `end_date` 自动扩展到当日 `23:59:59`
- 若输入为分钟级时间：
  - 严格按输入时间裁剪

注意：
- 若回测周期是 `1h/4h/1d`，实际样本会对齐到 K 线边界，这是正常行为

## 3. 研究回测引擎（`core/backtest/backtest_engine.py`）

## 3.1 驱动方式

该引擎按 bar 逐步推进：

1. 更新持仓浮盈亏
2. 将截至当前 bar 的数据切片传给策略（避免未来函数）
3. 接收策略输出 `Signal`
4. 执行开仓/平仓/反手逻辑
5. 记录交易、权益、成本分解

支持信号类型（依策略实现）：

- `BUY`
- `SELL`
- `HOLD`
- `CLOSE_LONG`
- `CLOSE_SHORT`

## 3.2 资金与持仓模型（当前实现）

当前回测采用“研究型保证金近似模型”，适合永续高频研究，但不是交易所逐笔清算模拟。

基本逻辑：

- 开仓：
  - 使用 `position_size_pct * capital` 估算保证金
  - `notional = margin * leverage`
  - 扣除手续费
- 持仓中：
  - 计算浮动盈亏
  - 可选计入资金费率
- 平仓：
  - 返还保证金并计入净盈亏

优点：

- 能处理多空
- 能处理杠杆
- 能做成本/资金费率归因

当前限制：

- 未模拟交易所强平、逐级维持保证金
- 未接入真实盘口冲击成本

## 3.3 成本模型（高频增强版）

`BacktestConfig` 关键参数（当前已支持）：

- `fee_model`: `flat | maker_taker`
- `maker_fee`, `taker_fee`
- `slippage_model`: `flat | dynamic`
- `dynamic_slip`: `{min_slip, k_atr, k_rv, k_spread}`
- `include_funding`: 是否计入资金费率
- `funding_source`: 资金费率来源标识

### 3.3.1 手续费

两种模式：

- `flat`
  - 使用统一费率（兼容旧配置）
- `maker_taker`
  - 使用 `maker_fee / taker_fee`
  - 默认执行角色通常按 `taker` 处理（除非策略或执行 metadata 提供额外信息）

### 3.3.2 动态滑点（无盘口近似）

在没有真实订单簿数据时，使用代理特征近似：

- `atr_pct`
- `realized_vol`
- `spread_proxy = (high - low) / close`

公式：

```text
slippage_rate = max(
  min_slip,
  k_atr * atr_pct + k_rv * realized_vol + k_spread * spread_proxy
)
```

说明：
- 这是研究近似模型，不等于真实成交冲击成本

## 3.4 资金费率（Funding）

当前支持两条路径：

1. 数据已含 `funding_rate` 列
2. 通过 `core/backtest/funding_provider.py` 自动补齐（本地缓存优先，Binance 公共接口回填）

当 `include_funding=True` 时：

- 若持仓跨越资金费率结算边界，按 `funding_rate * notional` 记账
- 多空符号方向正确处理（正 funding 通常多头付费、空头收取）

## 3.5 交易级 PnL 分解（已支持）

交易记录包含以下字段（用于研究与报告）：

- `gross_pnl`
- `fee`
- `slippage_cost`
- `funding_pnl`
- `net_pnl`
- `notional`
- `trade_stage`（如 `open / close / funding`）

说明：
- 旧字段（如 `commission/slippage/pnl`）仍保留兼容展示
- 新研究逻辑应优先使用 `net_pnl + breakdown`

## 3.6 回测结果汇总字段（研究口径）

`BacktestResult` 中已增强：

- `cost_breakdown`
  - `gross_pnl`
  - `fee`
  - `slippage_cost`
  - `funding_pnl`
  - `net_pnl`
- `turnover_notional`

这使得高频研究可以直接分析：

- 成本占毛收益比例
- 手续费与滑点的相对拖累
- funding 对策略净值的贡献/拖累

## 4. 与当前策略系统的关系

## 4.1 高频多因子策略（已接入）

`strategies/quantitative/multi_factor_hf.py`

特点：

- YAML 配置驱动（`config/strategy_multi_factor_hf.yaml`）
- 支持多空与显式平仓信号
- 支持 gate（波动 / ATR / spread_proxy / 量能）
- 支持 cooldown
- metadata 输出因子值、score、gate 状态、成本估计代理

## 4.2 宏观策略“只开不平”问题（已修复）

多个宏观策略此前只有极端条件触发 `BUY/SELL`，中性区间不显式平仓，导致看起来长期只开仓不平仓。

当前已增加：

- 中性回归平仓逻辑（`CLOSE_LONG / CLOSE_SHORT`）
- 方向状态跟踪（上一轮偏多/偏空）

适用策略（已修）：

- `MarketSentimentStrategy`
- `SocialSentimentStrategy`
- `FundFlowStrategy`
- `WhaleActivityStrategy`

## 5. 当前限制与后续建议

## 5.1 仍然存在的限制

1. 页面回测与研究回测尚未统一到底层同一记账引擎
2. 动态滑点仍为无盘口近似
3. 多标的组合回测在研究引擎层仍需额外 orchestration
4. 部分策略在 UI 页面回测中的实现与实盘策略细节不完全一致（为换取响应速度）

## 5.2 建议的后续方向（如继续迭代）

1. 将页面回测逐步接到统一成本模型（至少共用 `cost_models.py`）
2. 引入真实 `orderbook` 特征后升级滑点模型
3. 扩展 compare 接口支持“按已注册实例参数逐个对比”
4. 增加回测结果中的 `execution QoS` 指标（拒单率、信号到成交延迟代理）

