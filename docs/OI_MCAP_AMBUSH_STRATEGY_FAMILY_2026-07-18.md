# OI/市值 埋伏策略族（A/B/C）设计文档

日期：2026-07-18
模块：`strategies/quantitative/oi_mcap_ambush.py`
注册：`config/strategy_registry.py`（三条目均 `backtest.supported=True`）
测试：`tests/test_oi_mcap_ambush_strategies.py`（8 例）
数据：`scripts/build_ambush_dataset.py` → `data/research/ambush_modes/`
回测：`scripts/backtest_ambush_modes.py` → `reports/ambush_modes_<date>/`

## 背景与研究结论

目标是"提前埋伏 Binance 十倍币"，初始思路为「小市值 + 大 OI」。研究结论
（详见 2026-07-18 会话与外部案例：ALPACA 2025-04、RAVE/GENIUS/SIREN 2026-04）：

1. OI/市值 是**波动率/战场筛选器**，不是方向信号。同一结构既产出 60 倍逼空
   （ALPACA：市值 $5M、OI $110M），也产出 24 小时 −83%（SIREN）。
2. 方向要靠三个增量变量：资金费率符号、OI 变化与价格的相位差、现货是否接力。
3. Odaily 对 221 个合约币的实证：用 OI/费率/链上信号「预测拉盘」的模型样本外
   失效（F1 0.72 → 0.1）。纯预测式埋伏不可行，可复现的只有结构化模式。

因此拆成三个可验证的模式，全部只做多、fail-closed（缺 enrichment 列即静默不交易）：

## 模式定义

### A — AccumulationAmbushStrategy（吸筹指纹埋伏）
- 进场：OI 7 日涨幅 ≥ +25% 且 价格 7 日振幅 ≤ ±10% 且 7 日平均费率 ≤ 0.005%/8h
  且 市值 ∈ [$30M, $800M] 且 OI/市值 ≥ 0.15。边沿触发（指纹首次成立才进），
  冷却 14 天。
- 出场：结构失效（OI 3 日回落 ≥20% 或费率 ≥0.10%/8h 拥挤）、追踪止损 25%、
  硬止损 −18%、时间止损 21 天。不设固定止盈——A 的赔率来自尾部。
- 定位：现货式持仓（回测不计资金费）。命中率最低、赔率最高。

### B — SqueezeFuelStrategy（逼空燃料）
- 进场：费率 ≤ −0.05%/8h（空头付费）且 OI 3 日涨幅 ≥ +15% 且 OI/市值 ≥ 0.30
  且 收盘上破前 24h 高点（前一根未破，天然边沿）。
- 出场：费率翻正 ≥ +0.05%（燃料耗尽）、止盈 +40%、追踪 15%、止损 −12%、
  时间止损 7 天。持仓期收负费率是顺风（多头收钱）。
- 定位：永续多头，回测计资金费。

### C — IgnitionFastFollowStrategy（点火快跟）
- 进场：1h 涨幅 ≥ +6% 且 小时量 z-score ≥ 4（30 日窗）且 收盘破 7 日高
  且 OI 4h 跳增 ≥ +8% 且 48h 累计涨幅 < +80%（不追已翻倍的）。
- 出场：止盈 +25%、追踪 10%、止损 −8%、时间止损 48h。
- 定位：不预测点火、只跟点火，实证上最可复现的模式。

## 数据契约

1h OHLCV + 三列 enrichment（前向填充，含防前视位移）：

| 列 | 来源 | 防前视处理 |
|---|---|---|
| `oi_usd` | Coinglass OI history 4h（近 166 天）+ 1d（全史） | 可用性 +1 bar（4h/24h）后 ffill |
| `funding_rate` | Binance fapi fundingRate（403 时 Coinglass 兜底） | 结算时刻起 ffill |
| `mcap_usd` | CoinGecko market_chart 日频；无映射时用 当前市值×价格比 近似 | +24h 后 ffill |

策略基类强制 `use_atr_stops=False`（防止 `strategies.` 包默认 ATR 止损覆盖显式
百分比止损），并 `generic_check_exit_enabled=False`（A 不能被 +1% 利润锁提前踢出）。

## 实盘接线现状与 TODO

- 回测路径 ✅（enriched frame 由 backtest 脚本拼装）。
- 实盘路径 ⚠️：`strategy_manager` 目前只喂 OHLCV，三策略会 fail-closed 不交易。
  需要一个 enrichment 步骤把 coins-markets 快照（`market_cap_usd`、OI）与费率
  合入策略输入帧——与 `LiquidationOICrowdingStrategy` 的 structural_derivatives
  需求同源，建议做统一的 derivatives enrichment feed 后一并解决。
- 性能：热路径已 numpy 化（~1ms/bar/symbol）；实盘 41 标的 × 1h 无压力。

## 风险与纪律（写给未来的自己）

- 该策略族的目标población是高操纵度资产：单笔预期为负偏分布 + 长右尾，
  仓位纪律（单笔 ≤2%、并发 ≤10）比参数更重要。
- OI/市值 > 1.0 的极端区（ALPACA/妖币区）只允许 C 短线，禁止 A 隔夜埋伏。
- 回测普遍高估此类资产的可成交性（滑点/流动性/借贷可得性），实际预期应在
  回测数字上打折。详见回测报告的局限性小节。
