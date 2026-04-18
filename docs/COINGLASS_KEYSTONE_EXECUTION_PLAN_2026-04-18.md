# CoinGlass + Keystone 升级实施计划（补强版）

日期：2026-04-18

适用仓库：`E:\9_Crypto\crypto_trading_system`

## 1. 目标与结论

本次接入 CoinGlass 的目标，不是再增加一个独立的行情源，而是把它建设成一层：

1. 可限流
2. 可缓存
3. 可观测
4. 可灰度
5. 可被 AI / 策略 / 雷达复用

的 premium derivatives 特征层。

结合当前仓库结构与 Keystone 中转源约束，本次升级的最优路线已经明确：

1. 先把 CoinGlass 做成“后台缓存优先”的标准数据层
2. 再接入现有 `premium_external`、readiness、health、overview 体系
3. 最后按灰度方式向 AI、策略和山寨雷达输出结构化特征

这次计划不再继续堆能力清单，重点是把下面 6 个工程决策写死：

1. 密钥规范
2. 数据契约
3. worker / cache 接法
4. 配额账本
5. 健康状态暴露
6. 灰度开关

## 2. 中转源与协议约束

### 2.1 Keystone 中转源

- Base URL：`https://www.keystore.com.cn/api/v1/proxy/coinglass`
- Header：`X-Api-Key: <key>`
- 请求和响应格式与 CoinGlass 官方 API 保持一致
- 官方文档：`https://docs.coinglass.com`
- 完整 API 规范入口：
  `GET https://www.keystore.com.cn/api/v1/modules/coinglass/api-spec`

### 2.2 版本规则

- V3 路径必须包含 `/v3/`
- V4 路径必须包含 `/v4/`
- V3 使用 `camelCase`
- V4 使用 `kebab-case`
- 默认优先使用 V4，只有在 V4 不覆盖或历史接口缺失时，才回落到 V3

### 2.3 当前 Plan 约束

- 频率限制：`10 req/min`
- 日上限：`50000`
- 月上限：`500000`
- 当前 Plan：`Pro`
- 最晚到期：`2026-05-18`

## 3. 仓库对齐点

本次接入必须对齐现有仓库结构，不单独开一套旁路系统。

关键锚点如下：

1. 配置入口：
   [config/settings.py](/E:/9_Crypto/crypto_trading_system/config/settings.py:1)
2. 后台 premium worker 管理：
   [web/main.py](/E:/9_Crypto/crypto_trading_system/web/main.py:889)
3. 现有数据汇总与 overview API：
   [web/api/data.py](/E:/9_Crypto/crypto_trading_system/web/api/data.py:237)

扩展方向需要兼容以下现有能力：

1. `premium_external`
2. `/premium-data/status`
3. source health / readiness
4. altcoin radar
5. AI signal aggregator
6. analytics snapshots

## 4. 六个必须冻结的工程决策

### 4.1 配置与密钥

默认采用：

1. `Env + config 文件`

不再采用：

1. `keys.txt` 主路线

新增设置项：

1. `COINGLASS_ENABLED`
2. `COINGLASS_BASE_URL`
3. `COINGLASS_API_KEY`
4. `COINGLASS_RATE_LIMIT_PER_MIN`
5. `COINGLASS_DAILY_BUDGET`
6. `COINGLASS_MONTHLY_BUDGET`
7. `COINGLASS_INCLUDE_AI`
8. `COINGLASS_INCLUDE_RADAR`
9. `COINGLASS_INCLUDE_STRATEGIES`
10. `COINGLASS_LIVE_GATING_ENABLED`

密钥读取顺序：

1. 环境变量优先
2. 回退到 `config/coinglass_api_key.txt`

硬性规则：

1. 真实 key 不进入计划文档
2. 真实 key 不进入脚本示例
3. 真实 key 不进入日志
4. 真实 key 不进入前端 payload
5. 如果 key 曾出现在日志或文档中，先轮换再继续接入

### 4.2 数据契约冻结

在 Phase 0 必须产出一份 `dataset manifest`，用于冻结接入规则。

每个 dataset 至少要定义：

1. dataset 名称
2. 主路径
3. 首选 API 版本
4. V3 / V4 回退关系
5. 必填参数
6. 默认参数
7. TTL
8. freshness SLA
9. 字段映射
10. 失败降级规则

canonical key 固定为：

`dataset + api_version + market_type + normalized_symbol + exchange + interval + source_ts`

时间规范必须统一：

1. 所有时间落 UTC
2. 严格区分 `source_ts`
3. 严格区分 `ingested_at`

`capability scan` 的职责只允许是：

1. 生成可用矩阵
2. 生成首选路由

运行期不允许继续用动态试探端点的方式做路由决策。

### 4.3 运行时接入方式

接入方式锁定为：

1. 后台缓存优先

也就是：

1. 前端只读缓存
2. AI 只读标准化快照
3. 雷达只读标准化快照
4. 研究系统只读缓存结果
5. 不允许在前台请求时直接打 CoinGlass 上游

worker 接入方式：

1. 在 `web/main.py` 的 premium worker 体系内新增 `coinglass` worker
2. 由 supervisor 统一托管
3. 不依赖单独脚本作为常驻主链路

worker 负责：

1. token bucket
2. staggered schedule
3. 429/5xx 重试
4. 手动 refresh 保留配额
5. 预算扣减
6. ingest 状态更新

说明：

这里的“全量并进”指开发工作流并行推进，不是运行时一次性把所有能力都全开。

### 4.4 存储与状态

存储层保留三层结构：

1. raw jsonl
2. parquet
3. normalized snapshot

在 SQLite 中补齐以下状态表：

1. `coinglass_budget_ledger`
   记录分钟 / 日 / 月消耗、429 次数、最后成功时间、最后 HTTP 状态、最后错误
2. `coinglass_ingest_status`
   记录 dataset freshness、last_success_at、last_error、rows_written、latency、details

业务快照层：

1. 保留 `analytics_derivatives_snapshots`
2. 保留 `analytics_market_structure_snapshots`

其中：

1. `analytics_derivatives_snapshots` 必须补唯一键、索引和去重策略
2. `analytics_market_structure_snapshots` 第一版只存导出的结构化分数与少量核心字段
3. 不把大体量 orderbook 原样塞进关系表

### 4.5 健康状态暴露

CoinGlass 不能做成孤立入口，必须进入现有 premium 健康体系。

以下接口或状态视图必须能反映 CoinGlass：

1. `premium_status`
2. `coinglass_overview`
3. `derivatives_overview`
4. `premium_external`
5. `/premium-data/status`
6. source health / readiness

这些 payload 至少返回以下字段：

1. `available`
2. `freshness_sec`
3. `degraded_reason`
4. `quota_headroom`
5. `active_datasets`

关于 analytics history 的处理规则：

1. 如果后续要把 derivatives 放入统一 ingest 状态视图，则与 `microstructure/community/whales` 同级
2. 如果不放入统一 analytics history，则必须提供等价的 CoinGlass 状态端点

### 4.6 AI / 策略 / 雷达灰度

第一批虽然按“全量并进”推进开发，但运行期开关必须拆开：

1. 数据层和 overview API 可以直接上线
2. AI 的 `derivatives` 支路先 shadow
3. 策略层先 paper-only
4. `COINGLASS_LIVE_GATING_ENABLED` 默认保持 `false`

灰度规则如下：

1. AI 先参与解释和降权，不直接决定 live 方向
2. 策略先做 filter，不先做 aggressive 新策略
3. 雷达可以优先接入新的 ranking / tags / score
4. 只有 paper burn-in KPI 通过后，才进入 live gating 评审

## 5. 第一批 dataset manifest

第一批 v1 只覆盖“衍生品核心 + overview + AI shadow + 雷达增强”。

### 5.1 首批接入 dataset

1. `open_interest_exchange_list`
2. `funding_rate_exchange_list`
3. `taker_buy_sell_volume_exchange_list`
4. `liquidation_history`
5. `global_long_short_account_ratio_history`
6. `funding_arbitrage`

### 5.2 Manifest 约束

#### `open_interest_exchange_list`

- 主路径：`/v4/api/futures/open-interest/exchange-list`
- 兜底：`/v3/api/futures/openInterest/exchange-list`
- 必填参数：`symbol`
- TTL：`300s`
- freshness SLA：`<= 900s`
- 用途：OI 总量、交易所分布、OI 变化率

#### `funding_rate_exchange_list`

- 主路径：`/v4/api/futures/funding-rate/exchange-list`
- 兜底：`/v3/api/futures/fundingRate/exchange-list`
- 必填参数：`symbol`
- TTL：`300s`
- freshness SLA：`<= 900s`
- 用途：funding、weighted funding、过热判断

#### `taker_buy_sell_volume_exchange_list`

- 主路径：`/v4/api/futures/taker-buy-sell-volume/exchange-list`
- 兜底：`/v3/api/futures/takerBuySellVolume/exchange-list`
- 必填参数：`symbol`
- TTL：`300s`
- freshness SLA：`<= 900s`
- 用途：主动买卖方向、orderflow proxy

#### `liquidation_history`

- 主路径：`/v4/api/futures/liquidation/history`
- 兜底：`/v3/api/futures/liquidation/history`
- 必填参数：`symbol`, `interval`
- TTL：`300s`
- freshness SLA：`<= 1800s`
- 用途：清算冲击、squeeze / flush 识别

#### `global_long_short_account_ratio_history`

- 主路径：`/v4/api/futures/global-long-short-account-ratio/history`
- 无稳定 V3 等价时允许标记 capability-only
- 必填参数：`symbol`, `exchange`, `interval`
- TTL：`600s`
- freshness SLA：`<= 1800s`
- 用途：拥挤度、方向极化

#### `funding_arbitrage`

- 主路径：`/v4/api/futures/funding-rate/arbitrage`
- 无 V3 主兜底
- TTL：`900s`
- freshness SLA：`<= 3600s`
- 用途：basis / weighted funding / 套利结构

## 6. 标准对象与对外接口

### 6.1 标准对象

需要标准化以下对象：

1. `CoinglassDatasetManifest`
2. `CoinglassBudgetState`
3. `CoinglassIngestStatus`
4. `DerivativesSnapshot`
5. `CoinglassOverviewPayload`

### 6.2 API 变更

需要新增或扩展：

1. `premium_status` 中加入 `coinglass`
2. `coinglass_overview`
3. `derivatives_overview`
4. `analytics/status` 中加入 `derivatives` freshness 或等价状态视图

### 6.3 AI runtime context

AI runtime context 只允许注入结构化字段，不直接喂原始 CoinGlass JSON。

允许注入：

1. `market_regime`
2. `funding_regime`
3. `oi_regime`
4. `liquidation_state`
5. `orderbook_state`
6. `crowding_warning`

## 7. 存储设计

### 7.1 原始层

建议落地：

1. `data/premium/coinglass/raw/<dataset>/<YYYY-MM-DD>.jsonl`

用途：

1. 回放
2. 排障
3. 字段演进
4. 离线重算

### 7.2 标准化层

建议落地：

1. `data/premium/coinglass/normalized/<dataset>.parquet`

用途：

1. 统一 schema
2. 回测读取
3. 因子重算
4. 缓存快读

### 7.3 结构化快照层

用于服务：

1. AI
2. 雷达
3. overview API
4. readiness / health

主要表：

1. `analytics_derivatives_snapshots`
2. `analytics_market_structure_snapshots`

## 8. 预算与调度策略

### 8.1 总原则

1. 所有 CoinGlass 请求必须经过统一预算入口
2. 不允许前端请求绕过缓存直接打上游
3. 手动 refresh 必须保留额外配额
4. 优先使用聚合或 exchange-list 类接口，而不是碎片化多端点轮询

### 8.2 推荐限流模型

1. token bucket：`10 tokens / 60 sec`
2. 常态运行上限：`8 req/min`
3. 预留：`2 req/min` 给人工 refresh、调试和异常重试

### 8.3 推荐 TTL

#### 高频衍生品核心层

对象：

1. BTC
2. ETH
3. 当前重点观察 symbol

TTL：

1. `OI / Funding / Liquidation / Long-Short / Taker Buy-Sell`：`5 min`

#### 中频结构层

TTL：

1. orderbook / basis / market-structure：`10-15 min`

#### 低频情绪与 ETF 层

TTL：

1. ETF / Fear & Greed / Altcoin Season / AHR999 / SOPR：`1 h`

#### 超低频链上层

TTL：

1. reserve / exchange flow / whale / token transfer：`2-24 h`

### 8.4 Universe 分层轮询

建议按 3 层轮询：

1. Layer A：`BTC / ETH / SOL / BNB / XRP`
   每 `5 min`
2. Layer B：当前雷达 Top N + 已建预警币
   每 `10-15 min`
3. Layer C：其余 universe
   每 `30-60 min`

## 9. 分阶段实施

### Phase 0：协议冻结与 capability scan

目标：

1. 固化 Keystone 协议
2. 拉取 `api-spec`
3. 生成 capability matrix
4. 冻结 manifest

动作：

1. 新增 `coinglass_client`
2. 新增 `coinglass_registry`
3. 落地 `api_spec_v4.json`
4. 跑核心 endpoint capability scan
5. 写入 `coinglass_capability_matrix`

验收：

1. 明确哪些 endpoint 是 `200`
2. 明确哪些 endpoint 是 `403`
3. 明确哪些只能走 V3
4. 明确哪些优先走 V4

### Phase 1：最小可用数据接入

目标：

1. 打通首批衍生品核心数据
2. 建立 raw -> parquet -> snapshot 主链路

动作：

1. 接入 OI / Funding / Liquidation / Long-Short / Taker / Arbitrage
2. 落 raw jsonl
3. 转 normalized parquet
4. 计算 `analytics_derivatives_snapshots`

验收：

1. BTC / ETH / SOL 可稳定刷新
2. 可计算 crowding / squeeze / distribution 等基础分数
3. 不突破分钟预算

### Phase 2：状态与 overview API

目标：

1. 接入现有 premium / health / readiness 体系
2. 提供可消费的 overview payload

动作：

1. 加入 `premium_external`
2. 加入 `/premium-data/status`
3. 新增 `coinglass_overview`
4. 新增 `derivatives_overview`
5. 暴露 freshness / quota / degraded reason

验收：

1. key 已配置但缓存为空时，前端可显示 degraded
2. premium 状态里可见 CoinGlass
3. 可返回 active datasets 与 quota headroom

### Phase 3：AI shadow 接入

目标：

1. 把 CoinGlass 接入 AI runtime context
2. 默认只做解释和降权

动作：

1. 新增 `core/ai/coinglass_signal.py`
2. 扩展 `signal_aggregator`
3. 扩展 `autonomous_agent` compact payload
4. 注入 structured context

验收：

1. derivatives 缺数时能平稳降级
2. 不直接改变 live 方向
3. 高拥挤或 distribution 风险时能降低置信度

### Phase 4：山寨雷达增强

目标：

1. 用 derivatives heat / squeeze / crowding 补强雷达排序

动作：

1. 扩展 `altcoin_radar`
2. 扩展 `web/api/altcoin.py`
3. 扩展雷达 detail payload
4. 增加 tags / score / explainability

验收：

1. 排序可解释
2. detail payload 可见 derivatives 维度
3. 缺 derivatives 时不会报错，只会降级

### Phase 5：策略 paper-only 接入

目标：

1. 把 derivatives 特征先作为策略 filter

动作：

1. 趋势策略增加 crowding filter
2. 反转策略增加 liquidation / squeeze 冷却
3. 因子研究加入 CoinGlass 因子

验收：

1. paper 模式下减少高拥挤追单
2. 不直接开启 aggressive live 策略

### Phase 6：live gating 评审

进入条件：

1. capability scan 稳定
2. 至少 7 天缓存
3. offline 特征重算完成
4. radar 评估完成
5. AI shadow 评估完成
6. paper burn-in KPI 通过

完成以上条件后，才允许评估：

1. 是否开启 `COINGLASS_LIVE_GATING_ENABLED`

## 10. 测试计划

### 10.1 客户端与协议

1. V3/V4 路径映射
2. header 注入
3. manifest 路由选择
4. 429/403/5xx 行为
5. symbol 归一化

### 10.2 配额与调度

1. token bucket
2. 分钟预算
3. 日预算
4. 月预算
5. 手动 refresh 保留配额
6. worker 重启后预算恢复

### 10.3 存储与快照

1. raw jsonl 落盘
2. parquet 转换
3. snapshot 去重
4. UTC 时间对齐
5. ingest status 更新

### 10.4 API / UI

1. `premium_status` 显示 CoinGlass
2. overview payload 返回 freshness / quota / degraded reason
3. 缓存为空但 key 已配置时，前端显示 degraded

### 10.5 AI / 雷达 / 策略

1. derivatives 缺数时可降级
2. 雷达新增分数后排序可解释
3. 策略 filter 在 paper 模式下减少高拥挤追单

## 11. 验收闸门

推荐验收路径：

1. `capability scan`
2. `7天缓存`
3. `offline 特征重算`
4. `radar 评估`
5. `AI shadow`
6. `paper burn-in`
7. `live gating 评审`

## 12. 第一批实施范围

第一批只做：

1. `api-spec` 缓存
2. capability scan
3. symbol registry
4. OI / Funding / Liquidation / Long-Short / Taker / Arbitrage
5. `analytics_derivatives_snapshots`
6. `coinglass_overview`
7. `derivatives_overview`
8. AI shadow context
9. altcoin radar derivatives score

第一批先不做：

1. 全量 options
2. 全量 heatmap 明细
3. 全量链上明细
4. 复杂 live 自动执行

## 13. 风险与反模式

### 13.1 不建议的做法

1. 前端刷新时直接请求 CoinGlass
2. 每个 symbol 每轮同时打多个碎片 endpoint
3. 不做 capability scan 就默认全量可用
4. 只存几个数值，不保留 raw snapshot
5. 让 AI 直接消费原始 CoinGlass JSON

### 13.2 主要风险

1. 部分 endpoint 仍可能 plan-gated
2. 某些历史接口仍需要 V3 兜底
3. 不同 endpoint 粒度与时间戳未必一致
4. symbol alias 映射错误会导致脏数据
5. 如果 refresh 设计不好，10 req/min 很容易被打爆

## 14. 推荐执行顺序

建议严格按以下顺序推进：

1. Phase 0：协议冻结与 capability scan
2. Phase 1：最小可用数据接入
3. Phase 2：状态与 overview API
4. Phase 3：AI shadow
5. Phase 4：山寨雷达增强
6. Phase 5：策略 paper-only
7. Phase 6：live gating 评审

原因：

1. 先打通数据，再扩展消费层
2. 先做状态可观测，再做交易影响
3. 先增强雷达和解释层，再进入 live 决策层

## 15. 当前默认假设

本计划默认以下假设已经确认：

1. 配置模式：`Env + config 文件`
2. 接入方式：`后台缓存优先`
3. 开发推进：`全量并进`
4. 运行时启用：`分层灰度`
5. CoinGlass v1 聚焦：衍生品核心、overview、AI shadow、雷达增强
6. 期权、全量 heatmap、全量链上明细留到后续批次

## 16. 一句话路线图

先把 CoinGlass 接成一个“限流可控、缓存优先、可观测、可灰度”的 premium derivatives 特征层，再用这层统一增强数据库、overview API、AI shadow、策略 filter 和山寨雷达，而不是把它当成另一个孤立的数据源。
