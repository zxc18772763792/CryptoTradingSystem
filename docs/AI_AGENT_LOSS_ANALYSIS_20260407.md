# AI自治代理亏损复盘报告

> 生成时间: 2026-04-07 | 分析范围: 2026-03-27 ~ 2026-04-06 | 分析者: Claude Code

---

## 一、总览

| 指标 | 数值 |
|------|------|
| 总交易次数 | 27笔 (12开仓, 15平仓) |
| 总 PnL | **-$65.02** |
| 总名义金额 | $18,703.15 |
| 胜率 | **20.0%** (3胜 / 12负) |
| 平均每笔平仓亏损 | -$4.33 |
| PnL/名义金额比 | -0.35% |
| 手续费记录 | **$0.00** (bug — 见下文) |
| 最大单笔亏损 | -$30.34 (AVAX/USDT long) |

### 按标的分布

| 标的 | PnL | 交易数 | 胜/负 |
|------|-----|--------|-------|
| AVAX/USDT | **-$36.12** | 4 | 0/3 |
| ETH/USDT | -$12.78 | 11 | 2/4 |
| ICP/USDT | -$10.52 | 2 | 0/1 |
| BNB/USDT | -$2.66 | 3 | 0/1 |
| SOL/USDT | -$2.50 | 1 | 0/1 |
| LINK/USDT | -$0.44 | 3 | 0/1 |
| XRP/USDT | -$0.19 | 2 | 0/1 |
| BTC/USDT | +$0.20 | 1 | 1/0 |

---

## 二、根因分析

### 根因 #1: 三路信号全部权重为零时仍然交易 (CRITICAL — 已修复)

**影响**: 42次提交中有 **21次** (50%) 在所有信号组件权重=0的情况下执行

**机制**:
- `SignalAggregator` 由 LLM (40%)、ML/XGBoost (35%)、Factor (25%) 三路信号组成
- 当 LLM 调用失败、ML 模型未训练、Factor 数据不足时，三路的 `effective_weight` 均为 0
- `_weighted_vote()` 正确返回 `("FLAT", 0.0)`
- **但**: `_maybe_build_fast_path_hold()` 在有持仓时跳过 FLAT 检查，直接交给 LLM 决策
- **结果**: LLM 在没有任何市场信号的情况下"盲决策"

**修复**: 在 `_maybe_build_fast_path_hold()` 入口处增加 `total_effective_weight <= 0` 守卫，直接返回 hold。

---

### 根因 #2: Cooldown 阻止了止损平仓 (CRITICAL — 已修复)

**影响**: 亏损仓位无法及时平仓，损失被放大

**机制**:
```python
# 修复前 (line 5379):
decision["action"] in {"buy", "sell", "close_long", "close_short"}
```
- Cooldown (默认 180s) 不区分开仓和平仓
- Agent 刚开仓后 3 分钟内如果决策平仓，会被 cooldown 拦截为 hold
- **实际场景**: Agent 开仓后市场立即反向移动，LLM 正确判断应平仓，但 cooldown 阻止执行

**修复**: 将 cooldown 只应用于 `{"buy", "sell"}` (入场)，不再阻止 `close_long/close_short` (出场)。

---

### 根因 #3: 手续费记录全部为 $0 (HIGH — 已修复)

**影响**: 无法准确计算真实 PnL，无法做成本分析

**机制**:
1. `Order` 数据类缺少 `fee` 和 `fee_currency` 字段
2. 4个交易所连接器 (`binance/bybit/gate/okx`) 的 `_parse_order()` 未从 ccxt 返回值中提取手续费
3. `execution_engine` 的 `_consume_paper_order_cost()` 对实盘模式返回 0，且无 fallback 读取订单手续费

**修复**:
- `Order` 增加 `fee: float` 和 `fee_currency: str` 字段
- 4 个连接器 `_parse_order()` 均增加 fee 提取逻辑
- `execution_engine` 3 个执行路径增加实盘 fee fallback

---

### 根因 #4: LLM API 大面积故障 (HIGH — 需运维改善)

**影响**: 3033 条决策记录中，2991 条为 hold (98.6%)

**故障分类** (2991条hold中):

| 类型 | 次数 | 占比 |
|------|------|------|
| LLM HTTP 400 错误 | 410 | 13.7% |
| FLAT 信号 (正常hold) | 372 | 12.4% |
| LLM HTTP 502 错误 | 229 | 7.7% |
| 低于最低置信度阈值 | 238 | 8.0% |
| 风控拦截 | 216 | 7.2% |
| LLM HTTP 503 错误 | 180 | 6.0% |
| 服务不稳定自动暂停 | 156 | 5.2% |
| LLM HTTP 429 限流 | 141 | 4.7% |
| LLM 超时 | 68 | 2.3% |
| LLM HTTP 401 密钥无效 | 12 | 0.4% |
| 其他 hold | 945 | 31.6% |

**关键问题**:
- HTTP 400 中有 `"Unsupported parameter: temperature"` (117次) 和 `"Unsupported parameter: max_output_tokens"` (109次) — **API 参数不兼容**
- HTTP 429 限流 141 次 — 需要增加请求间隔或升级配额
- HTTP 401 无效密钥 12 次 — API key 可能被轮换或过期

---

### 根因 #5: 止损仅为软件层检查，非交易所级 (MEDIUM — 架构限制)

**影响**: 系统宕机或重启时持仓无保护

**机制**:
- `execution_engine` 的 `_check_protective_orders()` 每 2 秒轮询一次价格
- 达到 stop_loss/take_profit 时执行市价平仓
- **非**交易所级 OCO/stop-market 订单 — 如果系统重启期间价格穿过止损，损失不受限

**风险场景**: 系统凌晨 2 点宕机，持仓直到重启后才被检查。

---

### 根因 #6: 仓位过大 (MEDIUM — 配置问题)

**特征**:
- 4/6 后期交易的名义金额在 $1,400-$2,800 范围
- AVAX/USDT 单笔名义 $1,474 (最终亏损 $30.34 = 2.06%)
- ICP/USDT 单笔名义 $1,433 (亏损 $10.52 = 0.73%)
- ETH/USDT 连续加仓: $1,422 + $1,418 = $2,841 (亏损 $10.23)

**对比早期**: 早期交易名义金额在 $130-$644 范围 (合理)

**原因**: Agent 从 3/27 到 4/6 期间逐步增大仓位，后期交易的 confidence 和 strength 更高 (0.78-0.83)，LLM "过度自信"地分配了更大仓位。

---

### 根因 #7: Event summary 超时可导致决策循环崩溃 (MEDIUM — 已修复)

**影响**: 新闻数据库超时会导致整个 run_once 失败

**修复**: 增加 `asyncio.wait_for(..., timeout=5.0)` 和 try/except fallback。

---

### 根因 #8: CUSUM 衰减监控遗漏关键状态 (MEDIUM — 已修复)

**影响**: `live_candidate` 和 `live_running` 状态的策略不会被 CUSUM 监控

**修复**: 状态过滤增加 `live_candidate` 和 `live_running`。

---

## 三、亏损归因分解

```
总亏损 -$65.02 =
  [1] 盲决策亏损 (零权重交易)     ≈ -$30 ~ -$40  (估算: 占50%提交)
  [2] Cooldown 延迟平仓           ≈ -$10 ~ -$15  (估算: 3-5笔平仓被延迟)
  [3] 过大仓位放大损失             ≈ -$15 ~ -$20  (后期 AVAX/ETH 交易)
  [4] 未记录的实际手续费            ≈ -$5 ~ -$10  (按 taker 0.04% × $18,703)
```

---

## 四、已修复清单

| # | Bug | 文件 | 严重度 | 状态 |
|---|-----|------|--------|------|
| 1 | 零权重信号仍允许交易 | `autonomous_agent.py` | CRITICAL | ✅ 已修复 |
| 2 | Cooldown 阻止紧急平仓 | `autonomous_agent.py` | CRITICAL | ✅ 已修复 |
| 3 | Order 类缺少 fee 字段 | `base_exchange.py` | HIGH | ✅ 已修复 |
| 4 | Binance 连接器未解析 fee | `binance_connector.py` | HIGH | ✅ 已修复 |
| 5 | Bybit 连接器未解析 fee | `bybit_connector.py` | HIGH | ✅ 已修复 |
| 6 | Gate 连接器未解析 fee | `gate_connector.py` | HIGH | ✅ 已修复 |
| 7 | OKX 连接器未解析 fee | `okx_connector.py` | HIGH | ✅ 已修复 |
| 8 | 执行引擎实盘 fee fallback | `execution_engine.py` (×3) | HIGH | ✅ 已修复 |
| 9 | Event summary 超时未捕获 | `autonomous_agent.py` | MEDIUM | ✅ 已修复 |
| 10 | CUSUM 遗漏 live_candidate/live_running | `cusum_watcher.py` | MEDIUM | ✅ 已修复 |

---

## 五、建议改进

### 紧急 (立即)

1. **修复 LLM API 参数兼容性**: 排查 `temperature` / `max_output_tokens` 参数为何被 GPT-5.4 API 拒绝 (226次 400 错误)。可能需要针对不同 provider 使用不同的参数名。

2. **增加 API key 监控**: 12 次 401 错误表示 key 可能失效。建议在 model_feedback_guard 中对 401 做特殊处理 — 立即通知而非默默 hold。

3. **限制单笔最大名义金额**: 增加 `MAX_SINGLE_TRADE_NOTIONAL_USD` 配置项 (建议 $500)，在 `_build_signal()` 中做硬上限裁剪。

### 短期 (1-2周)

4. **交易所级止损单**: 对实盘交易，在开仓后立即提交 OCO 止损/止盈订单到交易所，而非仅依赖软件轮询。

5. **开启 `force_close_on_data_outage_losing_position`**: learning_memory 中该标志默认为 false。在 LLM 服务中断 30 分钟后，亏损仓位应自动平仓。

6. **减小默认 cooldown**: 当前 180s 太长。建议 entry cooldown 60s，exit 无 cooldown (已修复)。

### 中期 (1个月)

7. **信号组件健康检查**: 在 `run_once()` 开始前检查至少 1 个信号组件可用。如果 LLM/ML/Factor 全部不可用，直接跳过本轮。

8. **Position sizing 基于波动率**: 当前仓位由 LLM "自信度"决定。改为 ATR 基础的动态仓位管理，confidence 只作为缩放因子。

9. **强化学习回路**: learning_memory 应追踪"LLM 过度自信"模式 — 当 confidence > 0.8 但实际 PnL 为负时，后续降低同标的的自信度权重。

---

## 六、数据来源

- `data/cache/ai/autonomous_agent_journal.jsonl` — 3033 条决策记录
- `data/cache/live_review/strategy_trade_journal.jsonl` — 27 条 AI agent 交易
- `data/cache/ai/autonomous_agent_learning_memory.json` — 自适应风控状态
- `data/cache/ai/agent_runtime_config.json` — 运行时配置

---

*本报告由 Claude Code 自动生成，基于代码审查和交易数据分析。*
