# CoinGlass 信号增强落地计划

日期：2026-04-18

适用仓库：`E:\9_Crypto\crypto_trading_system`

关联文档：

1. [COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md](/E:/9_Crypto/crypto_trading_system/docs/COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md)

## 1. 目的

这份计划不是再扩一轮“支持更多接口”的清单，而是把已经接入的 CoinGlass 能力真正变成：

1. 可稳定拉取
2. 可正确解析
3. 可结构化落盘
4. 可进入 AI / 研究 / 雷达 / 套利决策
5. 可回归验证

的 derivatives 特征层。

当前仓库已经完成了 Keystone 代理接线、预算保护、worker/health/status 集成，也已经让 `BTC` 在本地成功跑通了部分 CoinGlass 数据链路。下一步的重点不再是“有没有接上”，而是“哪些字段还没被正确吃透，哪些特征最值得优先强化”。

## 2. 当前状态快照

截至 2026-04-18，当前本地状态可以概括为：

1. CoinGlass key 已经可以本地生效，`COINGLASS_ENABLED=true`
2. `/api/data/coinglass/overview?symbol=BTC/USDT` 已可返回真实快照
3. 当前 `active_datasets` 已经覆盖：
   - `open_interest_exchange_list`
   - `funding_rate_exchange_list`
   - `taker_buy_sell_volume_exchange_list`
   - `liquidation_history`
   - `global_long_short_account_ratio_history`
   - `funding_arbitrage`
   - `derivatives`
4. `AI 自治`、`AI 研究`、`研究工坊`、`山寨雷达` 已经能消费 CoinGlass 衍生品影子上下文
5. `套利` 主流程暂时不是以 CoinGlass 为主，但 funding/basis 相关参考能力已开始接入

## 3. 当前最需要修正的质量问题

这部分优先级高于继续扩接口。因为如果参数、symbol 映射或归一化不正确，新增再多数据也只会扩大噪声。

### 3.1 `taker_buy_sell_volume_exchange_list`

当前问题：

1. 原始响应存在 `400`
2. 错误提示为缺少 `range` 参数

风险：

1. `order flow` 相关特征当前可能是空值或错误值
2. 山寨雷达、AI 自治中的主动买卖失衡判断会失真

处理目标：

1. 按 CoinGlass 实际要求补齐 `range`
2. 明确 `range` 与现有 `interval` 的转换规则
3. 只在响应成功时写入标准化行

### 3.2 `liquidation_history`

当前问题：

1. 原始响应存在 `400`
2. 错误提示为缺少 `exchange`

风险：

1. 清算强度、long/short liquidation 方向判断失真
2. 山寨雷达的 squeeze / flush 标签可能只依赖残缺数据

处理目标：

1. 为 `liquidation_history` 显式传入 `exchange`
2. 定义聚合策略：优先单交易所还是多交易所汇总
3. 把 `long_liquidation_usd`、`short_liquidation_usd`、`burst_score` 做成稳定字段

### 3.3 `global_long_short_account_ratio_history`

当前问题：

1. 当前请求能命中接口，但响应里存在 pair 不存在的错误
2. 说明 `symbol/exchange/instrument` 映射仍未完全对齐 CoinGlass 口径

风险：

1. 多空比相关特征可能被误判为缺失
2. `crowding` 和 `contrarian` 逻辑会不稳定

处理目标：

1. 引入可维护的 `exchange + symbol -> CoinGlass instrument` 映射
2. 对失败 pair 做降级和回落，而不是直接污染状态
3. 为主流交易所优先建立稳定映射：`Binance`、`OKX`、`Bybit`

### 3.4 `funding_rate_exchange_list`

当前问题：

1. 当前接口成功返回，但标准化尾样本可能不是目标 symbol
2. 说明归一化层对响应体结构的过滤仍不够严格

风险：

1. `BTC` 的 funding 特征可能混入其他币种
2. 后续衍生品策略会在错误基础上做判断

处理目标：

1. 只保留与目标 symbol 严格匹配的行
2. 统一 `stablecoin_margin_list` / `token_margin_list` 的解析策略
3. 构建聚合 funding 指标：
   - 交易所均值
   - OI 加权均值
   - 极端交易所偏离度

### 3.5 `funding_arbitrage`

当前问题：

1. 当前接口成功返回，但更像全市场套利榜单，不一定是目标 symbol 的单币种快照
2. 当前写法容易把榜单第一名当作请求 symbol 的结果

风险：

1. `basis_pct` / `funding arbitrage` 可能被错误挂到 `BTC`
2. 研究工坊和 AI brief 中的套利语句会失真

处理目标：

1. 明确把它定义为“市场榜单”还是“单币种特征”
2. 如果作为榜单，单独落库，不直接覆盖 symbol snapshot
3. 如果作为单币种特征，必须先做 symbol 精确筛选

## 4. 第一阶段：把现有 CoinGlass 数据修成可用特征

这是最优先阶段，目标是让当前已经接上的 6 个数据集真正可用。

### 4.1 需要完成的工程动作

1. 修正每个数据集的参数模板
2. 为 `range`、`interval`、`exchange` 建立显式映射，而不是临时拼接
3. 为每个数据集写“成功响应结构断言”
4. 对 `code != 0` 或 `400/429` 响应禁止写入业务有效数据
5. 把失败状态保留在 ingest status 中，但不要污染 snapshot

### 4.2 需要新增/强化的测试

1. `taker_buy_sell_volume_exchange_list` 参数构造测试
2. `liquidation_history` 参数构造测试
3. `global_long_short_account_ratio_history` instrument 映射测试
4. `funding_rate_exchange_list` 目标 symbol 过滤测试
5. `funding_arbitrage` 榜单/单币语义测试
6. 实际 overview 生成时只消费成功且匹配的行

### 4.3 阶段验收标准

满足以下条件才算通过：

1. `BTC` 刷新时上述 6 个数据集至少 5 个能稳定返回结构化有效行
2. `overview.active_datasets` 与真实成功数据集一致
3. `snapshot.payload.payload_counts` 不再把错误响应当作有效数据
4. `order flow`、`清算`、`多空比` 不再长期为 `null`

## 5. 第二阶段：把快照升级成时间序列特征

当前系统已经有快照，但真正能提升 AI 和策略质量的，是历史斜率和结构变化。

### 5.1 建议优先增加的历史接口

1. `open-interest/history`
2. `funding-rate/history`
3. 如果成本可控，再补：
   - `liquidation/history` 更细粒度窗口
   - `taker-buy-sell-volume` 历史

### 5.2 建议生成的派生特征

1. `oi_change_1h / 4h / 24h`
2. `funding_mean / funding_zscore / funding_reversion_speed`
3. `taker_imbalance_15m / 1h`
4. `liquidation_burst_score`
5. `long_short_ratio_change`
6. `oi + funding` 背离标签

### 5.3 建议的解释型标签

1. `crowded_long`
2. `crowded_short`
3. `squeeze_building`
4. `flush_risk`
5. `basis_dislocation`
6. `flow_divergence`

## 6. 第三阶段：按模块落地 CoinGlass 强化

### 6.1 AI 自治

重点加强：

1. `order flow` 作为快速方向确认
2. `liquidation_burst_score` 作为追单/逆势过滤
3. `OI + funding` 组合作为拥挤惩罚项
4. `basis_dislocation` 作为减少杠杆或延迟执行的依据

验收标准：

1. `signal_aggregator` 的 derivatives 组件不再长期只给 0 值
2. `execution_engine` 的 CoinGlass filter 能引用真实 taker / liq / ratio 字段

### 6.2 AI 研究

重点加强：

1. 研究 brief 中自动插入 funding、OI、清算、LS ratio 结论
2. 当数据新鲜时，把 derivatives context 从“状态提示”升级成“研究论据”
3. 如果后续补 options，则把 skew / PCR 纳入宏观市场摘要

验收标准：

1. AI brief 能生成稳定的 derivatives thesis
2. `avoid_conditions` 和 `next_steps` 会引用真实拥挤/清算信号

### 6.3 研究工坊

重点加强：

1. 做成固定四件套：
   - `OI`
   - `Funding`
   - `Taker Imbalance`
   - `Liquidation`
2. 增加 freshness 和 degradation 的显式提示
3. 将错误数据集单独标红，而不是静默缺失

验收标准：

1. workbench 面板可以区分：
   - 数据不存在
   - 数据过期
   - 数据请求失败
   - 数据有效

### 6.4 山寨雷达

重点加强：

1. `derivatives_heat_score`
2. `squeeze_score`
3. `distribution_score`
4. `taker_buy_sell_imbalance`
5. `long_short_ratio`

建议新增标签：

1. `Derivatives Heat`
2. `Short Squeeze Risk`
3. `Crowded Long`
4. `Order Flow Confirmed`
5. `Derivatives Stale`

验收标准：

1. 雷达前排候选的排序明显受真实衍生品因子影响
2. 缺失 derivatives 的标的会被清楚降权，而不是悄悄混排

### 6.5 套利

当前判断：

1. 套利主流程不是以 CoinGlass 为主
2. 但 funding / basis / funding arbitrage 数据非常适合做增强

建议加强：

1. 为 `PairsTradingStrategy` / `FamaFactorArbitrageStrategy` 增加 funding/basis 风险卡片
2. 对 `CEXArbitrageStrategy` 增加 funding/basis 异常提示
3. 如果 `funding_arbitrage` 后续语义被修正，可单独做“资金费率套利机会榜”

验收标准：

1. 套利页清楚区分：
   - 回测 readiness
   - 市场微结构风险
   - CoinGlass 衍生品异常提示

## 7. 如果继续加数据，优先顺序建议

在完成前述修复之后，再扩以下数据。

### 优先级 P1

1. `open-interest/history`
2. `funding-rate/history`
3. `taker-buy-sell-volume` 历史
4. `liquidation` 更细时间粒度

### 优先级 P2

1. `options`：
   - skew
   - put/call ratio
   - options OI
   - max pain
2. `ETF flow`

### 优先级 P3

1. `on-chain`：
   - exchange inflow/outflow
   - whale flow
2. 更高频全市场 heatmap / coin list

## 8. 落地顺序建议

建议按下面顺序推进，不要并行开太多口子：

1. 修 `taker_buy_sell_volume_exchange_list`
2. 修 `liquidation_history`
3. 修 `global_long_short_account_ratio_history`
4. 修 `funding_rate_exchange_list` symbol 过滤
5. 重新定义 `funding_arbitrage` 语义
6. 为以上 5 项补完整单测
7. 再接 `open-interest/history` 和 `funding-rate/history`
8. 最后才扩 `options` 和 `ETF`

## 9. 手工验证清单

落实时建议固定执行以下验证：

### 9.1 数据层

1. `python scripts/discover_coinglass_capabilities.py`
2. `python scripts/refresh_coinglass_incremental.py --symbols BTC/USDT --manual --max-symbols 1`
3. 检查 `data/premium/coinglass/raw/` 原始响应是否为 `code=0`
4. 检查 `data/premium/coinglass/normalized/` 尾样本是否为目标 symbol

### 9.2 接口层

1. `GET /api/data/coinglass/overview?symbol=BTC/USDT`
2. `GET /api/trading/analytics/history/status?exchange=binance&symbol=BTC/USDT`
3. 检查 `active_datasets`、`status`、`snapshot.payload.payload_counts`

### 9.3 业务层

1. 打开 AI 研究页，确认 derivatives context 不再长期为空
2. 打开研究工坊，确认 Funding / Basis / Derivatives 状态正常
3. 打开山寨雷达，确认 `Derivatives Heat` 等标签会真实变化
4. 检查执行链路日志，确认 CoinGlass filter 有实际输入而不是空快照

## 10. 最后结论

对当前系统来说，CoinGlass 已经足够成为核心 derivatives 数据源，短期内最值钱的工作不是再接更多平台，而是把下面 4 类特征修成高质量输入：

1. `order flow`
2. `liquidation`
3. `long/short ratio`
4. `funding/basis`

把这四类吃透之后，再扩 `history`、`options`、`ETF`，整个系统的增益会明显更高，也更容易验证收益。
