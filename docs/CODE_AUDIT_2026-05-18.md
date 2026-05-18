# 全项目代码审计报告

> 生成日期: 2026-05-18
> 范围: crypto_trading_system 全模块逐项排查
> 严重度: 🔴 严重(资金/安全) · 🟠 中等(逻辑正确性) · 🟡 轻微/改进建议
> 说明: 明文密钥问题(keys.txt)已知，按用户要求暂缓处理，不在本文档内重复。

---

## 进度索引

- [x] core/risk — 仓位/风控/止损
- [x] core/trading — 执行引擎/持仓/账户
- [x] core/execution — 订单状态机/限流重连（实现稳健，无重大问题）
- [x] core/strategies + strategies/ — 策略基类与各策略
- [x] core/backtest — 回测引擎/成本模型（止损优先止盈，无前视，稳健）
- [x] core/news — 新闻采集/LLM 抽取/存储（抽样）
- [x] core/ai — 自治代理/决策路由/研究规划（抽样）
- [x] core/data — 数据采集/coinglass/因子（抽样）
- [x] core/research — 策略研究/验证门/编排（抽样）
- [x] core/monitoring — CUSUM/衰减监控（抽样）
- [x] core/exchanges — 交易所连接器（抽样）
- [~] web/api + web/main — 42K 行，仅做模式扫描，建议二轮深审
- [x] config / scripts — 配置与脚本（抽样）
- [x] 全局系统性问题

---

## 1. core/risk — 仓位 / 风控 / 止损

### 🔴 R-1 `position_sizer.py` 无 entry_price 防护 + 无最大仓位上限
- 位置: `core/risk/position_sizer.py:77,89,107,133,159,188`
- 问题: 所有 `_*_sizing` 方法直接 `value / entry_price`，`entry_price<=0` 返回 `inf`/崩溃；`_atr_based_sizing`(小 ATR)、`calculate_with_stop_loss`(止损极近) 会返回远超账户余额的仓位，模块自身无 `position_value ≤ account_balance` 钳制。
- 现状缓解: 实盘主路径由 `execution_engine` 的 `single_cap`/`alloc_cap` 二次封顶；但 backtest、sizing-preview 等直接调用路径无保护。
- 建议: 在 `PositionSizer` 内统一加 `entry_price>0` 校验与 `min(position_value, account_balance*max_cap)` 钳制（fail-closed）。

### 🔴 R-2 `risk_manager.pre_trade_check` 在 order_value 缺失时静默放行
- 位置: `core/risk/risk_manager.py:655`
- 问题: `if equity > 0 and notional > 0 and not allow_close:` —— `order_value`未传(→0)或 equity 未知时，单笔上限/组合总敞口/策略分配额度三项检查全部跳过，订单直接通过(fail-open)。
- 建议: `notional<=0 / equity<=0` 时拒单或强制 reduce-only；审计所有调用点确保始终传 `order_value`。

### 🟠 R-3 追踪止损双路径行为不一致
- 位置: `core/risk/stop_loss.py:84` vs `:214`
- 问题: `check_stop_loss→calculate_stop_price→_trailing_stop` 不检查 `config.trailing_activation`，追踪止损从开仓即生效；而 `update_trailing_stop` 正确判断激活阈值。两路径行为不同，可能过早止损。
- 建议: 将 `trailing_activation` 判定下沉到共用路径。

### 🟠 R-4 止盈标记不可逆
- 位置: `core/risk/stop_loss.py:267`
- 问题: 命中即 `target["executed"]=True` 并返回，实际平仓由下游执行；下游失败则该档位永久标记已执行、永不重触发，静默丢单。
- 建议: 下游确认成交后再标记，或失败回滚。

### 🟠 R-5 日内熔断对快速暴跌响应迟缓
- 位置: `core/risk/risk_manager.py:508`
- 问题: 熔断需累计 2(paper)/4(live) 次连续 breach，每次依赖一次 `update_equity` 心跳；瞬间跳水可能在凑齐次数前已远超止损额度。
- 建议: 单次 breach 超过止损 1.5–2x 时走立即熔断快路径。

### 🟡 R-6 日界按 UTC 午夜切分
- 位置: `core/risk/risk_manager.py:123`
- 问题: `_day_start` 按 UTC 切日，与用户/交易所时区不一致时「日内盈亏/止损」口径偏移。
- 建议: 确认是否符合预期，必要时改为可配置时区。

---

## 2. core/trading — 执行引擎 / 持仓 / 账户

### 🔴 T-1 持仓状态每次 tick 强制全量落盘（性能 + 阻塞事件循环）
- 位置: `core/trading/position_manager.py:470,540,554,578,590`
- 问题: `open_position/close_position/update_position_price/update_all_prices` 每次都 `_persist_scope_state(force=True)`，`force=True` 完全绕过了 `_persist_throttle_seconds=2.0` 节流；每个价格 tick 都同步 JSON 序列化全部持仓并写盘。高频多持仓场景下是严重 I/O 瓶颈，且在 async 上下文中同步写盘会阻塞事件循环。
- 建议: 价格更新路径只置 `_dirty=True` 走节流；仅 open/close 等关键状态变更 force；或改异步/后台落盘。

### 🟠 T-2 市价单 governance/风控规模检查被旁路
- 位置: `core/trading/order_manager.py:341` + `core/risk/risk_manager.py:655`
- 问题: `_create_real_order` 用 `order_value = abs(amount * (request.price or 0.0))` 喂给 governance；真·市价单 `request.price` 为 None → `order_value=0`，规模/敞口检查全部跳过(fail-open，同 R-2)。
- 建议: 市价单用最新行情价估算 notional 再做风控；缺价时拒单。

### 🟠 T-3 paper / live 风控不对等
- 位置: `core/trading/order_manager.py:232 _create_paper_order`
- 问题: paper 单完全不经过 `risk_manager`/`decision_engine`（仅 `_create_real_order` 经过）。用 paper 验证策略时，风控拒单行为与 live 不一致，paper 结论无法反映 live 实际拦截。
- 建议: paper 路径也跑同一套 governance/风控（仅在成交模拟处分叉）。

### 🟠 T-4 多匹配时平仓静默失败
- 位置: `core/trading/position_manager.py:503-508`
- 问题: `close_position` 若命中多个持仓（同 symbol 多策略且未指定 strategy），仅打 warning 并 `return None`，平仓静默失败，调用方可能误以为已平，留下悬挂持仓。
- 建议: 要求消歧或对全部匹配执行平仓，返回明确错误而非 None。

### 🟠 T-5 naive datetime 跨模块混用
- 位置: `core/trading/position_manager.py:41,42,57,151,155,526` 等
- 问题: `Position.opened_at/updated_at`、`_parse_datetime` 用 naive `datetime.now()`（本地时区），而 `risk_manager`/`order_manager` 用 `datetime.now(timezone.utc)`。naive 与 aware 比较会抛 TypeError，持仓时长/PnL 时间口径不一致。历史 memory 记录的 naive→aware 清扫遗漏了 position_manager.py。
- 建议: 统一 `datetime.now(timezone.utc)`。

### 🟡 T-6 paper 单理想化全量成交
- 位置: `core/trading/order_manager.py:282-295`
- 问题: paper 单恒 `filled=amount, status=CLOSED` 单价成交，无部分成交/流动性建模，paper 结果偏乐观。
- 建议: 可选引入简单流动性/部分成交模型，使 paper 更贴近实盘。

### 🟡 T-7 main 账户凭据回退忽略账户 mode
- 位置: `core/trading/account_manager.py:189-209`
- 问题: `aid=="main"` 且无显式凭据时回退到全局 settings 的真实 API Key，未校验该 main 账户 `mode` 是否为 paper；paper 主账户可能拿到 live 凭据（是否有害取决于连接器 sandbox 处理）。
- 建议: 回退时结合账户 mode；paper 账户不注入 live 凭据。

---

## 3. core/strategies + strategies/ — 策略

### 🔴 S-1 冲突检测会抑制平仓/止损信号
- 位置: `core/strategies/strategy_manager.py:666-687`
- 问题: `_emit_signals` 冲突检测把 `close_long` 归入 sell_sides、`close_short` 归入 buy_sides。当某策略发出 `close_long`（减仓/退出）而窗口内存在更强的 `buy`（入场）信号时，`close_long` 被当作"较弱冲突信号"丢弃 —— 风险降低型的退出/止损信号被静默抑制，可能让亏损持仓无法及时平掉。
- 建议: 冲突检测仅作用于「入场方向相反」的信号；`close_*` / 止损类信号永不被冲突丢弃，应直通执行。

### 🟠 S-2 冲突检测跨策略全局共享，破坏策略隔离
- 位置: `core/strategies/strategy_manager.py:659,688` (`self._recent_signal_by_symbol[signal.symbol]`)
- 问题: 冲突状态按 symbol 全局存储，不区分策略/账户。两个独立策略（如短线 vs 波段）在同一 symbol 反向操作会被判为冲突并丢弃较弱者，违背系统其它处强调的"策略隔离"。
- 建议: 冲突键改为 (account, symbol[, 方向类别])，或将跨策略对冲排除在冲突逻辑之外。

### 🟠 S-3 策略信号时间戳 bar_time/wall-clock 混用
- 位置: bollinger/macd/common_strategies/market_sentiment/cex_arbitrage/dex_arbitrage/multi_factor_hf 仍用 `datetime.now(timezone.utc)` 作 `Signal.timestamp`；rsi/momentum/mean_reversion/pairs/fama/fund_flow/factor/ml_xgboost 已用 `_bar_time(data)`
- 问题: commit 5fee503「stamp signals with bar time」只迁移了部分策略。strategy_manager 冲突窗口按 `signal.timestamp` 比较，bar-time 与 wall-clock 混比会产生错误的冲突判定，并破坏回放对齐。
- 建议: 全部策略统一改用 `self._bar_time(data)`。

### 🟠 S-4 `data.get("symbol",["UNKNOWN"])[0]` 在 DatetimeIndex 上会 KeyError
- 位置: bollinger:68,184 / rsi:47,201 / macd:71,176 / momentum:56,176 / mean_reversion:65,184 / fund_flow:169,407 / market_sentiment:155,366 / ml_xgboost:63 / factor_strategies:32
- 问题: `data["symbol"]` 是按 df 索引(通常 DatetimeIndex)对齐的 Series；`series[0]` 为标签查找，pandas≥2.0 对非整数索引 `s[0]` 抛 KeyError。strategy_manager `_load_market_data` 注入的 `result["symbol"]=symbol` 列正是 DatetimeIndex，存在运行期崩溃隐患。
- 建议: 统一改为 `.iloc[0]`，或直接从 config/metadata 取 symbol。

### 🟠 S-5 `StrategyBase.Position.update_price` 除零
- 位置: `core/strategies/strategy_base.py:113,116`
- 问题: `unrealized_pnl_pct = (current_price - entry_price) / entry_price` 无 `entry_price>0` 防护（`core/trading/position_manager.Position` 有防护，此处没有）。
- 建议: 加 `entry_price<=0` 短路。

### 🟡 S-6 `StrategyBase.open_position` naive datetime
- 位置: `core/strategies/strategy_base.py:199` `entry_time=datetime.now()`
- 问题: 与系统其余 aware UTC 不一致（同 T-5）。
- 建议: 统一 `datetime.now(timezone.utc)`。

---

## 4. 其余模块小结（抽样审查）

### core/execution — ✅ 实现质量高
订单生命周期状态机 (`order_state_machine.py`) 设计严谨：终态不可回退、部分成交单调累加 (`min(max(...), qty)`)、状态优先级守卫合理。未发现重大问题。

### core/backtest — ✅ 实现稳健
- `backtest_engine.py:164` 用 `data.iloc[:i]`（排除当前 bar）生成信号、按当前 bar close 成交，正确规避前视偏差。
- `exit_engine.py:311 vs 342` 同一 bar 内**先判止损再判止盈**（保守/悲观假设），是回测正确做法。
- 🟡 B-1: `backtest_engine.py:152` `current_time` 为 naive datetime，与系统 aware UTC 不一致（同 T-5 系统性问题）。

### core/news — 🟡 抽样
- 🟡 N-1: `core/news/storage/db.py:404` `PRAGMA {schema_name}.table_info('{table_name}')`、`:462 ATTACH DATABASE '{escaped_legacy}'` 用 f-string 拼接表名/库名。当前来源为内部常量(迁移用)，风险低；但若 `table_name` 未来来自外部输入则存在 SQL 注入。建议对表名做白名单校验。
- ✅ `requests.post/get` 均带 `timeout=`（多行调用），网络层无挂死风险。

### core/ai — 🟠 抽样
- 🟠 AI-1: `core/ai/autonomous_agent.py` 单文件 **6101 行**，是系统中风险最高的自动化组件却最难测试/维护。建议按职责拆分（决策、风控接口、循环调度、配置）。
- 🟡 AI-2: `autonomous_agent.py:394,2543,5367` 等多处 `except Exception: pass` 静默吞错（含 LLM 决策 JSON 解析回退路径），出错时无 trace，难以诊断"代理为何不动作/误动作"。建议至少 `logger.debug` 记录。
- ✅ 存在 `AI_AUTONOMOUS_AGENT_ENABLED` 开关、live 交易门 (`allow_live_enabled`)、循环 sleep 控制等安全脚手架。

### core/data — 🟡 抽样
- 多处采集器（data_collector/historical_data/funding_rate_collector/oi_collector/orderbook_collector 等）使用 naive `datetime.now()`，纳入系统性问题 G-1。

### core/research / core/monitoring — ✅ 抽样
- 除法普遍带 `max(.., 1e-9)` 守卫，未发现明显除零。
- `orchestrator.py` 含非测试 `assert`（生产代码用 assert 在 `-O` 优化模式下会被剥离，不应用于运行时校验）。建议改显式判断+异常。

### core/exchanges — 🟡 抽样
- 各连接器 (`bybit/gate/okx_connector`、`ccxt_adapter`) 多处 `except Exception:` 静默处理，建议记录 warning 便于排查连接/下单失败。

### web/api — ⚠️ 未深审（42531 行）
体量过大，本轮仅做模式扫描（无 eval/exec、无明显 SQL 注入字符串、无裸 except）。建议安排第二轮针对：① 鉴权/越权 ② 输入校验 ③ 同步阻塞调用混入 async 端点 的专项审查。

---

## 5. 全局系统性问题

### 🔴 G-1 naive / aware datetime 全项目混用
- 范围: `risk_manager`/`order_manager`/`order_state_machine` 用 aware UTC；`position_manager`、`strategy_base`、`core/backtest`、`core/data/*` 约 50+ 处仍用 naive `datetime.now()`。
- 风险: naive 与 aware 比较 **抛 TypeError**（strategy_manager 已加 try/except 兜底但属补丁）；持仓时长/PnL/去重窗口/熔断时间口径不一致。历史 memory 多次记录"naive→aware 清扫"，但仅覆盖 strategies 部分，core/trading/position_manager 与 core/data 大面积遗漏。
- 建议: 统一封装 `now_utc()` 工具并全局替换；加 lint 规则禁止裸 `datetime.now()`。

### 🟠 G-2 关键风控/规模检查 fail-open
- 范围: `risk_manager.pre_trade_check`(R-2)、`order_manager._create_real_order`市价单(T-2) —— 缺 `order_value`/`equity` 时跳过额度校验而非拒单。
- 建议: 风控类检查统一改 **fail-closed**：信息不足即拒单或降级 reduce-only。

### 🟠 G-3 fire-and-forget asyncio.create_task（76 处）
- 风险: 未持引用的 task 可能被 GC 提前回收；task 内异常被静默吞掉无人 await。持仓回调/落盘/通知等关键副作用若失败将无感知。
- 建议: 统一经一个 `task_registry` 持引用 + `add_done_callback` 记录异常。

### 🟡 G-4 巨型单文件
- `web/api/*` 42K 行、`autonomous_agent.py` 6101 行、`execution_engine.py` 5194 行。圈复杂度高、单测覆盖困难、审查盲区大。建议按职责拆分并补关键路径单测。

### 🟡 G-5 同步磁盘 I/O 在 async 热路径
- `position_manager._persist_scope_state(force=True)` 每 tick 同步写盘（T-1）。建议关键状态落盘走后台/异步、价格路径走节流。

---

## 6. 改进优先级建议

| 优先级 | 项 | 理由 |
|---|---|---|
| P0 | T-1 持仓每 tick 强制落盘 | 直接拖慢/阻塞实盘事件循环 |
| P0 | S-1 冲突检测抑制平仓/止损信号 | 可能让亏损单无法止损，资金风险 |
| P0 | R-2 / T-2 / G-2 风控 fail-open | 规模/敞口检查可被旁路 |
| P1 | R-1 position_sizer 无钳制 | backtest/preview 路径可产生超额仓位 |
| P1 | S-4 `["symbol"][0]` KeyError | pandas≥2 运行期崩溃隐患，影响多策略 |
| P1 | S-3 / G-1 时间戳混用 | 冲突判定错误、回放错位、TypeError |
| P1 | R-3 / R-4 止损止盈逻辑 | 追踪止损过早、止盈静默丢单 |
| P2 | T-3 paper/live 风控不对等 | 影响 paper 验证可信度 |
| P2 | T-4 多匹配平仓静默失败 | 悬挂持仓 |
| P2 | AI-1/AI-2 自治代理可维护性 | 最高风险组件最难诊断 |
| P3 | G-3 fire-and-forget task | 健壮性隐患 |
| P3 | N-1 PRAGMA 表名拼接 | 当前低风险，防御性加固 |
| P3 | web/api 第二轮专项审查 | 体量过大，本轮未覆盖 |

> 备注: 本轮为首轮全面排查，深读了风控/交易/策略/执行/回测核心路径；news/ai/data/research/exchanges 为抽样；web/api(42K行) 仅模式扫描，建议单独排期二轮深审。
