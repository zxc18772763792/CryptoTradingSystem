# 回测与会计审计报告 (2026-05-21)

审计范围：`core/backtest/backtest_engine.py`、`core/accounting/pnl_decomposer.py`、`core/backtest/cost_models.py`、`core/research/strategy_research.py`（回测相关部分）、`core/research/validation_gate.py`

---

## 高优先级 (Bug, 必修)

### B-001 DSR 峰度修正公式错误 (kurtosis-1 应为 kurtosis-3)
**文件**：`core/research/validation_gate.py:75`

**问题**：
```python
adj = 1.0 - skewness * sr + (kurtosis - 1.0) / 4.0 * sr ** 2
```
Bailey & López de Prado (2014) 原公式使用 **(kurtosis - 3)**（超额峰度），因为正态分布峰度为 3。当前使用 `kurtosis - 1`，对于正态收益（kurtosis=3, skewness=0）时 `adj = 1 + 0.5*SR²` 而非期望的 `adj = 1.0`，使得调整后 Sharpe 被系统性虚高。

**实测（python）**：
```
SR=1.5, kurtosis=3(正态), skewness=0 → adj_bug=2.125, adj_correct=1.000
```
`adj_sr` 被放大 √2.125≈1.46 倍 → DSR 数值虚高 → 策略通过 DSR 门控的概率被高估。

**影响**：所有未显式传入 skewness/kurtosis 的调用（即全部调用，因为默认 skewness=0, kurtosis=3）均受影响；DSR 阈值 (_DSR_REJECT_BELOW=0.4, _DSR_DOWNGRADE_OK=0.65) 已按有 bug 的行为调校，修复后可能需要重新调整这两个阈值。

**修复**：
```python
# 将第75行
adj = 1.0 - skewness * sr + (kurtosis - 1.0) / 4.0 * sr ** 2
# 改为
adj = 1.0 - skewness * sr + (kurtosis - 3.0) / 4.0 * sr ** 2
```

---

### B-002 PnLDecomposer 过度平仓时余量未处理 (long-to-short flip via single fill)
**文件**：`core/accounting/pnl_decomposer.py:113-118, 204-247`

**问题**：当一个 `sell` fill 的 qty 大于已有多头仓位总量（即一笔单同时平多并开空），`on_fill` 把它识别为 `is_closing=True`，进入 `_apply_closing_fill`。该方法的 while 循环消耗完所有 lots 后，`remaining > 0` 但 `pos._lots` 为空，循环退出，超出部分 qty 被**静默丢弃**——不会开启空头仓位，也不记录错误。

**影响**：单笔翻仓信号（如 SELL 2x 当前多头仓位）导致：
- 多头被正确平仓并记录 realized PnL
- 超额的空头部分完全消失（零成本开仓失败）
- `self.positions` 中既无多也无空——净暴露低估

**修复**：在 `_apply_closing_fill` 结束后，检测 `remaining > 1e-12`，若有，则在当前 symbol 上重新调用 `on_fill` 开一个反向仓位：
```python
# 在 _apply_closing_fill 末尾（del self.positions[symbol] 之后）：
if not pos._lots and remaining > 1e-12:
    # 超额部分开反向仓位
    new_side = "buy" if pos.side == "short" else "sell"
    excess_fee = fee * (remaining / max(qty_close, 1e-12))
    excess_slip = slippage_cost * (remaining / max(qty_close, 1e-12))
    self.on_fill(symbol, new_side, remaining, close_price,
                 fee=excess_fee, slippage_cost=excess_slip, timestamp=ts)
```
注意：`_apply_closing_fill` 返回前需将 `remaining` 传出（当前签名无此返回值），或改造为在方法内部直接调用 `self.on_fill`。

---

### B-003 PnLDecomposer 存档快照在 del 前调用但位置已经删除
**文件**：`core/accounting/pnl_decomposer.py:238-243`

**问题**：
```python
record = self.position_snapshot(symbol) or {}
record["closed_at"] = ts.isoformat()
self._closed.append(record)
del self.positions[symbol]
```
`position_snapshot(symbol)` 在 `del self.positions[symbol]` 之前被调用（正确），但此时 `pos._lots` 已为空（被 while 循环清空），`avg_entry_price()` 和 `qty` 都变成 0，导致快照里的 `qty=0`, `entry_price=avg_entry_price=0`，丢失有意义的仓位信息。

**影响**：`closed_trades()` 返回的记录 qty、avg_entry_price 均为 0，依赖这些字段做分析的代码（如仓位规模报告）全部失效。

**修复**：在 while 循环开始之前保存 `original_qty = pos.qty` 和 `original_avg_price = pos.avg_entry_price()`，在记录存档时手动回写这两个字段：
```python
# 在 _apply_closing_fill 开头存入:
orig_qty = sum(lot.qty for lot in pos._lots)
orig_avg = pos.avg_entry_price()
# ...（while 循环）...
if not pos._lots:
    record = self.position_snapshot(symbol) or {}
    record["qty"] = orig_qty          # 恢复原始仓位大小
    record["entry_price"] = orig_avg  # 恢复原始均价
    record["avg_entry_price"] = orig_avg
    record["closed_at"] = ts.isoformat()
    self._closed.append(record)
    del self.positions[symbol]
```

---

### B-004 check_exit 使用含当前 bar 的数据（潜在前视偏差）
**文件**：`core/backtest/backtest_engine.py:187, 419`

**问题**：
```python
await self._check_strategy_exit_signals(strategy, current_data.tail(live_window), current_price, current_time)
```
`current_data = data.iloc[: i + 1]`，其中 `iloc[-1]` 是第 i 根 bar（当前 bar）。`check_exit` 因此可以看到当前 bar 的 **close 价格**后决定是否退出，然后退出仍按 `current_price`（即当前 close）成交。这与 `generate_signals` 的处理方式不一致——后者用 `data.iloc[:i]` 排除了当前 bar（见第 194 行注释）。

**影响**：任何在 `check_exit` 里用 `data["close"].iloc[-1]` 或 rolling 指标的退出逻辑均会出现轻微的前视偏差，导致回测 Sharpe 略高于实盘。

**修复**：给 `_check_strategy_exit_signals` 传入 `current_data.iloc[:-1].tail(live_window)` 或 `data.iloc[:i].tail(live_window)`（与 `generate_signals` 一致）：
```python
# 第 187 行改为：
await self._check_strategy_exit_signals(
    strategy, data.iloc[:i].tail(live_window), current_price, current_time
)
```

---

### B-005 `_run_backtest_core` 中 commission_rate 为单程费率但同时计入买开与卖平
**文件**：`core/research/strategy_research.py:2086-2089`

**问题**：
```python
fee_rate = max(0.0, float(commission_rate or 0.0))
slip_rate = max(0.0, float(slippage_bps or 0.0)) / 10000.0
total_cost_rate = fee_rate + slip_rate
trade_cost = turnover * total_cost_rate
```
`turnover` 是 `position.diff().abs()`，每次换仓产生一次 turnover。但 `commission_rate=0.0004` (0.04%) 是**单边费率**，而在该向量化引擎中，一次开仓 (position: 0→1) 产生 turnover=1，一次平仓 (1→0) 产生 turnover=1，共 2 次 turnover，累计收取 2 × 0.0004 = 0.08%，实际上对应的是开仓+平仓的双边成本，这是**正确**的——但如果 commission_rate 被理解为"round-trip"费率，则双倍收费。

当前默认值 `0.0004`（40 bps round-trip）相当于每边 0.02%，低于 Binance taker fee（0.05%），会导致成本低估。

**影响**：策略成本被低估，回测结果高于实盘。建议将默认值从 `0.0004` 改为 `0.001`（单边 0.1%，接近实际 taker fee），或在文档中明确说明这是单边费率。

**修复建议**：在 `ResearchConfig` 的注释中明确 `commission_rate` 是单边费率；将默认值从 0.0004 改为 0.001 以更接近实际单边 taker fee：
```python
commission_rate: float = 0.001   # 单边 fee rate (每笔开/平仓各收一次)
```

---

### B-006 `backtest_engine._calculate_result` Sharpe 使用 `sqrt(365)` 固定值
**文件**：`core/backtest/backtest_engine.py:788`

**问题**：
```python
sharpe = float(np.mean(returns) / np.std(returns) * np.sqrt(365)) if ...
```
`returns` 来自 `equity_curve` 的逐 bar 差分，时间框架由输入数据决定。若数据是 1h bar，应年化因子 `sqrt(365*24=8760)`；若是 4h bar 应是 `sqrt(365*6=2190)`。硬编码 `sqrt(365)` 只对日线数据正确，对小时线等数据Sharpe被严重低估（约 sqrt(24)≈4.9 倍）。

**对比**：`strategy_research._run_backtest_core` 使用 `_annual_factor(timeframe)` 正确处理不同时间框架。

**影响**：`BacktestEngine` 的 Sharpe 与 research 模块的 Sharpe 在相同策略上会产生巨大差异，导致用户对两个系统的结果对比困惑，且实际上 `BacktestEngine` 结果中的 Sharpe 对于任何非日线策略都是错误的。

**修复**：从 `cost_models` 或新增辅助函数借用 `_annual_factor`，或简单地在 `run_backtest` 中传入 `timeframe` 并按实际时间框架计算年化因子：
```python
# 在 run_backtest 中增加 timeframe 参数, 并改为:
bar_seconds = (data.index[-1] - data.index[-2]).total_seconds() if len(data) > 1 else 86400
ann_factor = max(1, int(365 * 86400 / bar_seconds))
sharpe = float(np.mean(returns) / np.std(returns) * np.sqrt(ann_factor)) if ...
```

---

### B-007 `BacktestEngine.daily_returns` 始终为空列表
**文件**：`core/backtest/backtest_engine.py:105, 126, 819`

**问题**：`BacktestResult.daily_returns` 字段声明存在，`self._daily_returns` 在 `_reset()` 初始化为 `[]`，但在整个 `run_backtest` 和 `_calculate_result` 过程中**从未被填充**。任何依赖 `result.daily_returns` 的下游分析（如 Sortino ratio、日度回撤分析）都会得到空列表。

**影响**：中等——`daily_returns` 字段在内部多处声明但无一使用，但如果未来外部调用者依赖它，会静默失败。

**修复**：在 `_calculate_result` 中根据 `equity_array` 计算日度收益序列：
```python
# 在 _calculate_result 中 equity_array 计算后添加:
if len(equity_array) > 1:
    bar_rets = np.diff(equity_array) / np.where(equity_array[:-1] == 0, np.nan, equity_array[:-1])
    self._daily_returns = [float(r) for r in bar_rets if np.isfinite(r)]
```

---

## 中优先级 (性能)

### P-001 `_apply_closing_fill` 每次都对所有 lots 求和计算 `total_close_qty`
**文件**：`core/accounting/pnl_decomposer.py:211`

**问题**：
```python
total_close_qty = min(qty_close, sum(lot.qty for lot in pos._lots))
```
对于有大量 lots 的仓位（频繁加仓场景），每次关闭都要遍历整个 lots 列表。应改为直接从 `pos.qty`（已维护的累计量）取值：
```python
total_close_qty = min(qty_close, pos.qty)
```

---

### P-002 `_optimize_params_scipy_lhs` 中局部精修的最大迭代次数无上限
**文件**：`core/research/strategy_research.py:2288-2315`

**问题**：局部精修 (±1 index) 对 `best_params` 的每个参数都执行两次 `_run_backtest_core`。若参数空间有 N 个参数，精修阶段额外执行 `2*N` 次回测。当策略有 10 个参数时，精修阶段比主采样阶段（30次）多运行 20 次，但并无收益保证。

**建议**：增加 `max_local_refinement_trials: int = 10` 参数，或直接设 `if len(keys) > 5: return (best_params, n_trials, method)` 跳过精修。

---

### P-003 `run_backtest` 内层循环每 bar 调用 `_update_positions` 三次
**文件**：`core/backtest/backtest_engine.py:184-203`

**问题**：
```python
self._update_positions(current_price, symbol)  # line 184
await self._check_position_exits(...)
self._update_positions(current_price, symbol)  # line 186
await self._check_strategy_exit_signals(...)
self._update_positions(current_price, symbol)  # line 188
```
每个 bar 调用三次 `_update_positions`，而每次调用都遍历所有仓位更新 mark price 和 equity。对于多仓位策略（max_positions=5），每 bar 产生 15 次仓位遍历。

**建议**：将三次调用合并为一次（在所有退出检查完成后），或至少减少到两次（bar 开始+bar 结束）。

---

### P-004 `microstructure_proxies` 每次被调用都对 120 bar 做三列 rolling 计算
**文件**：`core/backtest/cost_models.py:37-66`

**问题**：在 `slippage_model="dynamic"` 时，每次买入/卖出/平仓都触发 `_slippage_rate(window)` → `microstructure_proxies(window)`，后者在 120 行上计算 ATR（30 bar rolling max）、realized_vol（60 bar std）、spread_proxy，共产生约 5-6 次 DataFrame 操作。回测 10000 bar、100 次交易时，会进行 200 次 rolling 计算。

**建议**：在 `run_backtest` 的外层循环中每 bar 预计算一次 `microstructure_proxies`，缓存到局部变量，在同 bar 内的多次 `_slippage_rate` 调用中复用。

---

### P-005 `_run_purged_walk_forward` 对全量数据运行 5 折 OOS 回测，但内含嵌套调用
**文件**：`core/research/strategy_research.py:2935-2951`

**问题**：在主研究循环中，每个 (strategy, timeframe) 组合依次执行：
1. LHS 优化（最多 30+2N 次回测在 IS 数据上）
2. 全量回测（1次）
3. OOS 验证（1次）
4. 权益曲线样本（1次，实际是第3次全量回测）
5. walk-forward（5 折 OOS 回测）

对于 37 策略 × 6 时间框架 = 222 组合，walk-forward 本身就要运行 222×5=1110 次回测。

**建议**：增加 `enable_walk_forward: bool = True` 配置项，让快速研究可以跳过 WF；或将 WF 与优化合并（使用优化已经产生的 fold 结果）。

---

## 低优先级 (死代码 / 一致性)

### L-001 `_run_walk_forward` 仅作为向后兼容 wrapper 但外部测试直接依赖它
**文件**：`core/research/strategy_research.py:2394-2417`

**问题**：`_run_walk_forward` 已被 `_run_purged_walk_forward` 取代，并保留为向后兼容 wrapper（仅被 `tests/test_ai_research_phase2.py:910-927` 调用）。主研究循环 (`run_strategy_research`) 直接调用 `_run_purged_walk_forward`，不经过 wrapper。

**建议**：在 wrapper 的 docstring 中加 `.. deprecated::` 注释；将测试改为直接调用 `_run_purged_walk_forward` 并验证其 dict 返回格式。函数本身可以保留以避免破坏兼容性，但应标记为 deprecated。

---

### L-002 `cost_score` 用 35% 阈值但 rejection reason 用 25% 阈值
**文件**：`core/research/validation_gate.py:226, 271`

**问题**：
```python
cost_score = _inverse_score(cost_burden_pct, 35.0)   # 分数满分点=35%
...
if cost_burden_pct > 25:                              # rejection warning=25%
    reasons.append(...)
```
`_inverse_score(x, bad_at=35)` 在 x=25% 时返回约 28.6（非零分），说明 25% 还不到"零分线"，但 reason 警告已触发。这两个阈值的语义不一致：scoring 系统认为 35% 才是"坏的"，但 warning 在 25% 就触发，导致被推荐为 paper 的策略可能同时带有"cost drag 过高"的警告。

**建议**：将 `_inverse_score` 的 `bad_at` 改为 25，与 reason 阈值保持一致：
```python
cost_score = _inverse_score(cost_burden_pct, 25.0)
```

---

### L-003 `BacktestEngine` 的 `notional` 计算在开仓和平仓时定义不一致
**文件**：`core/backtest/backtest_engine.py:459-469, 633`

**问题**：
- 开仓时：`notional = self._capital * position_size_pct`（基于**可用资金**，用于计算 fee）
- 平仓时：`notional = abs(exec_price * quantity)`（基于**实际市值**，用于计算 fee）

由于 `exec_price` 包含滑点，平仓的 notional（和 fee）会因滑点略偏。更重要的是，开仓的 `fee = notional * fee_rate`（其中 notional 不含滑点，为 `capital * pct`），但实际发生的 fee 应基于 exec notional (`exec_price * quantity`)。

**影响**：轻微——对于小仓位小滑点，误差可忽略，但逻辑不一致在高杠杆场景下会产生明显偏差。

**建议**：开仓时也用 `exec_price * quantity` 计算 notional 和 fee：
```python
exec_price = current_price * (1 + slip_rate)
quantity = notional / current_price   # 保持不变（用 current_price 确定 qty）
exec_notional = exec_price * quantity
fee = exec_notional * fee_rate        # 用 exec_notional 计算 fee（而非 current_price * qty）
```

---

### L-004 `_compute_score` 中的 `_tr / _dd` 会在 `_dd` 极小时数值爆炸
**文件**：`core/research/strategy_research.py:2181, 2188`

**问题**：
```python
_dd = max(float(metrics.get("max_drawdown", 0.0) or 0.0), 0.5)
...
+ (_tr / _dd) * 3.0
```
`_dd` 被 clamp 到最小 0.5，避免了除零，但对于 max_drawdown=0（完美无亏损策略），仍会得到 `_tr / 0.5`，让 Calmar-like 项无限放大。同时，当 `_tr < 0` 时，`_tr / _dd * 3.0` 产生负贡献，但 Calmar ratio 通常定义为 abs(return) / max_drawdown，这里没有取绝对值——负 return 策略的该项会是负值，逻辑正确，但语义与 Calmar 不同。

**建议**：更明确的 clamp：
```python
_dd = max(float(metrics.get("max_drawdown", 0.0) or 0.0), 1.0)  # 至少 1%
```

---

### L-005 `BacktestEngine` 的 `max_unrealized_pct` 追踪逻辑对空头方向为负值
**文件**：`core/backtest/backtest_engine.py:685-688`

**问题**：
```python
favorable_pct = unrealized / entry_notional  
pos["max_unrealized_pct"] = max(float(pos.get("max_unrealized_pct", 0.0) or 0.0), favorable_pct)
```
对于空头仓位，`unrealized = (entry_price - mark) * qty`，市价下跌时 `unrealized > 0`（有利），`favorable_pct > 0`，逻辑正确。但 `max` 初始值 `0.0` 意味着对于持续亏损的空头，`max_unrealized_pct` 始终为 0，失去追踪意义。

**建议**：初始值改为 `float("-inf")` 或 `favorable_pct` 本身，改用 `max(pos.get("max_unrealized_pct", float("-inf")), favorable_pct)`，以准确捕获"最大有利浮动"。

---

### L-006 `_run_purged_walk_forward` 的 OOS 折叠缺少 IS 数据训练
**文件**：`core/research/strategy_research.py:2353-2381`

**问题**：当前 walk-forward 只在 OOS 切片上运行回测，但不对每折做单独的 IS 优化——全部折叠共享主循环中 LHS 优化得到的 `best_params`。这是"固定参数 walk-forward"，不是真正的"扩展窗口 walk-forward"（expanding IS → OOS），两者语义不同。

**现状不是 bug**（有代码注释），但文档应明确这是"固定参数稳健性检验"而非参数泛化能力检验。

**建议**：在函数 docstring 中添加 `Note: params are fixed from external IS optimization; this tests robustness, not generalization.`

---

## 备注

1. **已确认无回归的已修复 bug**：
   - 资金费率双重计算（funding double-count）：已正确修复，`_apply_funding_for_bar` 不再直接更新 `self._capital`，只更新 `pos["funding_pnl"]`，并在 `_close_position` 的 `net_pnl` 中通过 `accrued_funding` 一次性结算。已确认无回归。
   - `_funding_boundary` try/except：已正确修复。
   - 信号前视偏差（`generate_signals` 路径）：`data.iloc[:i]` 已正确排除当前 bar，无前视。

2. **`_run_walk_forward` 状态**：仅保留为兼容 wrapper，主循环只调用 `_run_purged_walk_forward`，无外部调用。在测试中被直接导入，建议逐步迁移测试。

3. **DSR n_obs 配置**：当 `best["n_bars"]` 存在时，DSR 正确使用 bar 数而非交易数，n_bars 在主研究循环的 `payload["n_bars"] = len(tf_df)` 处（第 2903 行）已填充，传递链完整。

4. **`cost_score` vs `reasons` 阈值不一致**（L-002）属于已知设计变化（MEMORY.md 记录 "Cost-drag threshold 35→25%"），可能是 scoring 公式未同步更新，建议统一为 25%。

5. **`strategy_research._run_backtest_core` 与 `BacktestEngine`** 是两套独立引擎，前者向量化（无事件循环），后者事件驱动（含 SL/TP/trailing stop）。在策略研究阶段用向量化引擎快速筛选，在仿真/实盘用 BacktestEngine，逻辑合理但需注意两者 Sharpe 的可比性受到 B-006 影响。
