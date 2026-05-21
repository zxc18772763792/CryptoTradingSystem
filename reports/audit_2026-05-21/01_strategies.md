# 策略模块审计报告 (2026-05-21)

审计范围：`core/strategies/`、`strategies/technical/`、`strategies/quantitative/`、`strategies/factor_based/`、`strategies/arbitrage/`、`strategies/macro/`、`strategies/event_driven/`、`strategies/ai/`

---

## 高优先级 (Bug, 必修)

### 1. `core/strategies/signal_generator.py:48,60,123,145,168` — naive `datetime.now()` 用于信号时间戳
**问题**：`SignalFilter.filter()` 使用 `datetime.now()`（无时区）与 `Signal.timestamp`（tz-aware UTC）比较，导致 `TypeError` 异常在 `recent - last_time` 处。`SignalCombiner._weighted_vote/majority/average` 在合并信号时将 `timestamp=datetime.now()`（naive）写入最终信号，破坏跨策略冲突检测窗口（`strategy_manager._emit_signals` 用 `signal.timestamp - prior.timestamp`，混用 naive/aware 时已有 `try/except TypeError` 兜底但会放行所有冲突信号）。
**影响**：信号冷却时间比较失效，组合信号时间戳错误，下游冲突检测退化为"无冲突"（age 计算 TypeError → 放行）。
**建议修复**：将全部 `datetime.now()` 改为 `datetime.now(timezone.utc)`，补充 `from datetime import timezone`。

### 2. `core/strategies/strategy_manager.py:1967` — 状态摘要中 naive `datetime.now()`
**问题**：`get_status()` 返回 `"timestamp": datetime.now().isoformat()`，无时区信息。前端/监控用此字段判断数据新鲜度，与所有其他时间戳（UTC ISO 带 `+00:00`）不一致，可能导致 stale 检测误判。
**影响**：低-中；UI 显示时间异常，若后端按此排序可产生微小误差。
**建议修复**：改为 `datetime.now(timezone.utc).isoformat()`。

### 3. `strategies/factor_based/factor_strategies.py:1292` — `HurstExponentStrategy` 中 `rolling.apply` 含 lambda（性能+lookahead 风险）
**问题**：`var_long = returns.rolling(n).apply(lambda x: np.var(x[::5]) * 5, raw=False)` 使用 `raw=False`（Series 模式），每 bar 调用 Python lambda，时间复杂度 O(n²)。更严重的是：`x[::5]` 在窗口内按步长抽样计算"长周期方差"，该实现并非真正 Hurst 指数（方差比检验需用 `n//k` 步长对齐），数学上偏差明显——variance ratio 接近 1 时 trending/mean-reverting 判断均可能误触发。
**影响**：(1) 对 500 条数据 O(n²) 约 2.5M 次 lambda 调用，在 5m 数据上单次 `generate_signals` 可能延迟数百毫秒；(2) 趋势判断方向可能系统性偏差。
**建议修复**：改用向量化方差比（将 `returns` 按 5 步重采样后计算方差，一次 `rolling`），或替换为简化 Hurst (RS 分析)。

### 4. `strategies/factor_based/factor_strategies.py:1390,1549` — `VaRBreakoutStrategy` 和 `SortinoRatioStrategy` 使用 `rolling.apply` 含自定义 Python 函数（O(n²) 性能）
**问题**：`VaRBreakoutStrategy`（L1390）中 `returns.rolling(n).apply(calc_var, raw=False)` 每 bar 调用 `np.percentile`；`SortinoRatioStrategy`（L1549）中 `returns.rolling(n).apply(calc_sortino, raw=False)` 每 bar 排序下行收益。两处均 `raw=False`，产生 O(n²) 调用。
**影响**：在 500 条数据上，`n=20/30` 分别触发 ~10k-15k 次 Python 函数调用，显著增加 cycle 延迟（尤其与其他因子策略共用同一事件循环时阻塞 asyncio）。
**建议修复**：`VaR` 改用 `rolling.quantile(q)`（pandas 内置 C 实现）；`Sortino` 中下行标准差改用滚动分步骤向量化（`rolling.apply(raw=True)` 至少省去 Series 构建开销）。

### 5. `strategies/factor_based/factor_strategies.py:1724,1765` — `CCIStrategy` 中 `rolling.apply` 计算 MAD（O(n²)）
**问题**：两处（`generate_signals` 和 `check_exit`）均使用 `tp.rolling(n).apply(lambda x: np.abs(x - x.mean()).mean())`。CCI 的 MAD 可向量化：MAD ≈ rolling_std × √(2/π)（正态近似），或用 `rolling(n).std()`。
**影响**：O(n²) Python lambda，与 VaR/Sortino 问题同级；`check_exit` 每次 `check_exit` 调用都重算一遍，若 100 个持仓每 bar 各调用一次，放大效应明显。
**建议修复**：用 `tp.rolling(n).std() * (2/np.pi)**0.5` 近似 MAD（误差 <2%），或 `raw=True` 改为 numpy 计算。

### 6. `strategies/factor_based/factor_strategies.py:47` — `FactorStrategyBase._create_signal` 时间戳有条件依赖 `_active_bar_ts`
**问题**：`timestamp=getattr(self, "_active_bar_ts", None) or datetime.now(timezone.utc)`。当 `_active_bar_ts` 为 `None`（首次调用或策略重置后）时回退到 wall-clock。但所有子类中 `generate_signals` 的首行都调用 `self._active_bar_ts = self._bar_time(data)`，`check_exit` 中则**不**设置 `_active_bar_ts`，因此 `check_exit` 内调用 `_create_signal` 时时间戳总是 wall-clock——与 `generate_signals` 路径不一致，且在历史回测中会写入真实系统时间。
**影响**：回测信号时间错误；`check_exit` 发出的关仓信号时间戳与对应的开仓信号时间戳不匹配，影响性能归因。
**建议修复**：`_create_signal` 改为强制参数 `data` 并调用 `self._bar_time(data)`，或在 `check_exit` 开头也设置 `_active_bar_ts`。

### 7. `strategies/quantitative/mean_reversion.py:194` — `BollingerMeanReversionStrategy.generate_signals` min_bars 不足（off-by-one）
**问题**：`if data.empty or len(data) < self.params["period"]:` — 用 `< period` 而非 `< period + 1`。`data["close"].iloc[-2]`（`prev_close`）在 `len(data) == period` 时，`rolling(period).mean()` 的 `iloc[-2]` 为 `NaN`（rolling 刚好满足 min_periods，但 prev bar 不满足），导致 `prev_close <= prev_lower` 可能误触发。
**影响**：当数据刚好等于 period 条时，prev 指标 NaN 与 close 比较产生 False（pandas NaN 比较），信号漏发而非错发；但下游若解包 `NaN` 信号价格会崩溃。
**建议修复**：改为 `len(data) < self.params["period"] + 1`，与 `BollingerBandsStrategy`（已修复）保持一致。

### 8. `strategies/factor_based/factor_strategies.py:1393` — `VaRBreakoutStrategy` 使用 `var.iloc[-2]`（前一 bar）与 `current_ret`（当前 bar）对比，方向可能错误
**问题**：L1393 `current_var = float(var.iloc[-2])` 使用的是上一 bar 的 VaR，L1392 `current_ret = float(returns.iloc[-1])` 是当前 bar 的收益率。注释说"避免lookahead"，但 BUY 信号条件是 `current_ret >= var_threshold`（正突破→做多），实际这是追涨信号而非突破回归——当收益已经实现，信号仍基于**前一 bar** VaR 作为阈值，可能在波动已衰竭时才触发。此外变量名 `current_var` 实为 prev_bar 的 VaR，命名误导性强。
**影响**：逻辑方向未必错，但命名混淆调试，且 BUY 方向（price already up → long）与 VaR 突破的对冲逻辑不符（通常 negative VaR 突破才做多抄底）。
**建议修复**：明确命名为 `prev_var`；确认业务意图：若是趋势追涨改注释说明，若是均值回归则需翻转方向（负突破才做多）。

---

## 中优先级 (性能 / 并发)

### 9. `strategies/quantitative/multi_factor_hf_fast.py:135` — `rolling.apply(_rank_last, raw=True)` 轻量但仍 O(n²)
**问题**：`rank_dn = dn.rolling(lb, min_periods=5).apply(_rank_last, raw=True)` — `raw=True` 已省去 Series 构建，但 `_rank_last` 仍 O(n) per bar → O(n²) 总体。该函数为回测批量计算路径，对大回测（2000+ bars）有显著影响。
**影响**：`strategy_research._build_positions` 的 `MultiFactorHFStrategy` 回测路径。
**建议修复**：可用 `dn.rolling(lb).rank(pct=True) * 2 - 1`（pandas 内置 rolling rank，C 层实现）完全替换，结果等价。

### 10. `strategies/macro/fund_flow.py:101` — `FundFlowStrategy._fetch_orderbook_flow` 是同步网络 IO 包装为 async
**问题**：`async def _fetch_orderbook_flow` 内部直接 `await connector.get_order_book()`，但 `connector` 可能是阻塞型 ccxt 客户端（取决于实例化方式）。若 `connector.get_order_book` 未正确 await，会在事件循环中阻塞。
**影响**：阻塞 asyncio 事件循环，影响其他策略的 cycle 执行时间。
**建议修复**：在调用前确认 connector 已用 `ccxt.async_support`，或显式包装 `await asyncio.to_thread(connector._client.fetch_order_book, symbol, depth)` 降级为线程池。

### 11. `core/strategies/signal_generator.py:65` — `SignalFilter` 每次 `filter()` 调用都写 `signal.timestamp` 到内部列表，但比较用的是 `datetime.now()`（naive）
**问题**：L65 `self._recent_signals[key].append(signal.timestamp)`，但 L60 `datetime.now() - last_time` 用 wall clock 减去已记录的（可能 tz-aware 的）`signal.timestamp`，若类型不同触发 TypeError 被静默忽略（无 try/except），`filter()` 返回 `True`（通过），等同于冷却时间失效。
**影响**：冷却时间保护完全失效，高频策略产生过量信号。
**建议修复**：记录 `datetime.now(timezone.utc)` 而非 `signal.timestamp`，或统一用 tz-aware 时间比较。

### 12. `strategies/quantitative/momentum.py:43` — `MomentumStrategy` 信号强度无上界保护在特殊参数下
**问题**：L68 `strength=min(current_momentum / threshold, 1.0)` — 当 `threshold` 极小（如通过 API 传入 0.0001）时，`current_momentum / threshold` 溢出为极大值。虽然 `min(..., 1.0)` 截断，但若 `threshold <= 0`（用户配置错误），触发 ZeroDivisionError。
**影响**：参数验证缺失时策略崩溃。
**建议修复**：改为 `strength=min(current_momentum / max(threshold, 1e-9), 1.0)`；并在 `validate_params` 中检查 `momentum_threshold > 0`。

### 13. `strategies/factor_based/factor_strategies.py:1292` — `HurstExponentStrategy` 的 `var_long` 计算含 lookahead（见高优先级#3）
**附加说明**：`vr = (var_long / var_1.replace(0, np.nan)).fillna(1)` 若 `var_1` 为 0（极低波动期，如横盘市场），fillna(1) 后 `vr == 1`，不触发任何策略分支，产生"沉默期"。若实际为 trending 市场，信号遗漏。

---

## 低优先级 (死代码 / 重复 / 一致性)

### 14. `strategies/factor_based/factor_strategies.py:44` — `FactorStrategyBase._create_signal` 中 `stop_loss=None, take_profit=None` 总被子类覆盖
**问题**：所有子类在调用 `_create_signal` 后立即设置 `signal.stop_loss = ...` 和 `signal.take_profit = ...`，`_create_signal` 中的 `stop_loss=None, take_profit=None` 参数从未被使用。这导致 `_finalize_generated_signals`（StrategyBase 自动包装层）检测到无 stop/take 时，会用 ATR 覆盖子类设置的值，可能与子类逻辑冲突。
**影响**：若 `_finalize_generated_signals` 先于子类赋值运行（当前因 `__init_subclass__` wrap 机制在 `generate_signals` **返回后**运行，顺序实际上是子类设好后再 finalize，所以 finalize 会覆盖子类的 stop/take），ATR stops 会覆盖子类设计的 target price。
**建议修复**：在 `_create_signal` 中接受 `stop_loss`/`take_profit` 参数并传递，或在子类统一设置 `metadata["use_atr_stops"] = False` 避免 finalize 覆盖。

### 15. `strategies/quantitative/momentum.py:123-218` — `TrendFollowingStrategy._calculate_adx` 重复实现了与 `common_strategies.ADXTrendStrategy._adx` 几乎相同的逻辑
**问题**：两个独立实现的 ADX 计算，参数名和细节不同（`TrendFollowingStrategy` 用 `rolling.mean` 平滑 DM，`ADXTrendStrategy` 也用类似逻辑），且两者都未处理 `plus_di + minus_di == 0`（DX 计算分母）的 ZeroDivisionError。
**影响**：(1) 在极低波动期（连续 doji），`dx = 100 * abs(...) / 0` 产生 inf/NaN，下游 `adx.rolling(period).mean()` 也变 NaN，导致 ADX 判断永远为 False → 信号静默；(2) 代码重复，维护负担。
**建议修复**：提取为 `core/strategies/indicator_utils.py` 公共函数 `calculate_adx(data, period)` 供复用；修复 `dx` 分母保护：`(plus_di + minus_di).replace(0, np.nan)`。

### 16. `core/strategies/signal_generator.py:102-171` — `SignalCombiner` 三个 `combine` 方法与 `strategy_manager._emit_signals` 的冲突检测重复
**问题**：`SignalCombiner` 实现了 BUY/SELL 投票合并，但 `signal_generator` 实例（`signal_generator = SignalGenerator()`）在整个代码库中没有任何策略调用（`Grep "signal_generator.process"` 无结果）。该模块为孤儿代码。
**影响**：维护负担；`SignalFilter` 的 naive datetime 问题（已在#1中报告）不会在运行时触发，但一旦有人接入此模块即引入 bug。
**建议修复**：确认是否有调用方；若无，标记为废弃或删除。

### 17. `strategies/technical/macd_strategy.py:188` — `MACDHistogramStrategy` 的 `min_histogram` 注释乱码
**问题**：L188 `"min_histogram": 0.0001,  # ????????????` — 注释明显为编码错误（原中文注释丢失），当前读者无法理解该参数用途。
**影响**：纯维护问题，不影响运行。
**建议修复**：恢复注释，如 `# 最小直方图阈值（过滤噪声信号）`。

### 18. `strategies/factor_based/factor_strategies.py` — 多个策略在 `generate_signals` 顶部设置 `_active_bar_ts` 但 `check_exit` 不设置（见#6），且 `_active_bar_ts` 本身不在 `__init__` 中初始化
**问题**：`_active_bar_ts` 是约定俗成的实例属性，但未在 `FactorStrategyBase.__init__` 中定义，纯靠赋值时动态创建。若 `check_exit` 在 `generate_signals` 之前被调用（第一次 bar），`_create_signal` 中 `getattr(self, "_active_bar_ts", None)` 返回 None，回退 wall clock。
**建议修复**：在 `FactorStrategyBase.__init__` 中添加 `self._active_bar_ts: Optional[datetime] = None`。

### 19. `core/strategies/strategy_registry` vs `core/research/strategy_research.py` — 策略注册/研究覆盖缺口
**问题**：`config/strategy_registry.py` 注册了 `LiquidationOICrowdingStrategy`、`SupplyEventStrategy`、`OnChainFlowRegimeStrategy`、`FamaFactorArbitrageStrategy`、`CEXArbitrageStrategy`、`TriangularArbitrageStrategy`、`DEXArbitrageStrategy`、`FlashLoanArbitrageStrategy`，但 `RESEARCH_SUPPORTED_STRATEGIES` 中均无这些策略，研究引擎无法为它们生成候选和回测。
**影响**：用户无法通过 AI 研究中心优化这些策略的参数；手动注册后没有验证流程（IS/OOS/WF）。
**建议**：评估是否适合将结构化策略（LiquidationOI/SupplyEvent/OnChainFlow）加入研究引擎（需特殊数据列），或在 UI 文档中标注"仅手动配置"。

### 20. `strategies/technical/common_strategies.py:281` — `VWAPReversionStrategy` 仅生成多头关仓信号，缺少空头对称
**问题**：`VWAPReversionStrategy.generate_signals` 只有 BUY 和 CLOSE_LONG 两个分支（L319-349），没有对称的 SELL 和 CLOSE_SHORT 逻辑。策略设计为纯多头均值回归，但 `get_required_data` 无说明，与 `VWAPStrategy`（factor_strategies 中有 BUY/SELL 双向）形成不一致。
**影响**：空头策略系统（如做市、对冲）接入此策略无法产生空头信号；若 `check_exit` 对空头调用 `None`，空头仓位永不退出。
**建议修复**：添加空头路径（`d_prev <= entry and d_now > entry → SELL`），或在 `get_required_data` 中明确标注 `"long_only": True`。

### 21. `strategies/quantitative/mean_reversion.py:127` — `MeanReversionStrategy.check_exit` 中 `_regime_bias` 状态跨重启丢失
**问题**：`self._regime_bias` 字典在 strategy restart 时清空（`initialize()` 调用 `positions.clear()` 等，但未清 `_regime_bias`）。实际上 `check_exit` 中也不读取 `_regime_bias`（只在 `generate_signals` 设置/弹出），属于半废弃的状态缓存。
**影响**：低；但如未来有代码读 `_regime_bias` 将获取陈旧状态。
**建议修复**：在 `initialize()` 中 `self._regime_bias.clear()`，或移除该字典（`check_exit` 路径已不依赖它）。

---

## 备注 / 待讨论

### A. `strategies/factor_based/factor_strategies.py` 中 `_create_signal` 与 `_finalize_generated_signals` 的 ATR stops 交互
`FactorStrategyBase._create_signal` 初始化 `stop_loss=None, take_profit=None`，子类在返回 list 后手动赋值。但 `__init_subclass__` 中的 wrap 机制在 `generate_signals` **返回时**（return 后）调用 `_finalize_generated_signals`，此时子类已赋值，finalize 再次基于 ATR 覆盖。即：子类设计的固定比例 SL/TP 会被 ATR 动态值覆盖（若 `use_atr_stops` 为 True）。这是系统设计取舍，需明确文档化；建议所有 factor 策略显式设置 `metadata["use_atr_stops"] = False` 或设 `mutates_input = False` 以控制 finalize 行为（但当前 `mutates_input` 不影响 finalize 逻辑，二者解耦）。

### B. `HurstExponentStrategy` 的 trending_threshold 与 variance ratio 语义对齐
当前 `trending_threshold=0.55, mean_revert_threshold=0.45`——这是真实 Hurst 指数的阈值（H>0.5=trending），但代码计算的是**方差比（VR）**，不是 Hurst 指数。方差比 >1 对应 trending，<1 对应 mean-reverting，阈值应围绕 1.0，而非 0.5。当前参数化会导致几乎所有市场被识别为 trending（VR 通常 0.8-1.5，远大于 0.55）。建议将 `trending_threshold` 改为 1.1，`mean_revert_threshold` 改为 0.9，并更新文档。

### C. `strategies/quantitative/multi_factor_hf.py` 内部虚拟持仓状态 `_virtual_side` 与 `position_manager` 隔离
`MultiFactorHFStrategy` 用 `_virtual_side: str = "flat"` 跟踪内部状态，完全独立于 `position_manager`。在策略重启或多实例场景下，`_virtual_side` 会重置为 `flat`，可能产生重复建仓信号。若与 `_collect_exit_signals` 机制共用，可能触发对空仓的虚假平仓信号。建议记录此限制，或在 `initialize()` 时查询 `position_manager` 同步初始化 `_virtual_side`。

---

*审计人：Claude Sonnet 4.6 — 2026-05-21*
