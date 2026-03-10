# 分析历史数据库重构计划

## Summary
当前这部分数据库已经有表和落库逻辑，但还不是稳定的数据服务：采集主要由页面触发，社区/巨鲸源偏弱，且最新样本里已有超时降级数据。目标是把它改成“后台定时采集、免费公开源优先、失败样本允许入库但必须带降级标记”的历史数据库子系统。

## Key Changes
### 1. 数据模型与质量标记
- 保留现有三张表：`analytics_microstructure_snapshots`、`analytics_community_snapshots`、`analytics_whale_snapshots`。
- 给三张表统一补充质量字段：
  - `capture_status`: `ok | degraded | failed`
  - `source_error`: 简短错误摘要
  - `source_name`: 实际采集源标识
  - `latency_ms`: 本次采集耗时
  - `ingest_version`: 当前采集器版本号
- `payload` 继续保留细节，但页面和研究逻辑只读取标准列，不直接依赖 payload 结构。
- 降级策略固定为：失败也允许入库，但必须写 `capture_status=degraded/failed`，且所有不可用字段明确置空或默认值，禁止伪装成正常样本。

### 2. 采集模式改为后台定时
- 新增独立后台采集 worker，不再依赖打开数据页。
- 默认调度：
  - 微观结构：每 5 分钟一次
  - 社区/公告：每 15 分钟一次
  - 巨鲸：每 10 分钟一次
- 页面“刷新体检”只做两件事：
  - 立即读取历史库并展示
  - 异步触发一轮补抓，不阻塞页面
- Web 启动时注册该 worker，和现有新闻 worker 一样受开关控制：
  - `ANALYTICS_HISTORY_ENABLED=1`
  - `ANALYTICS_HISTORY_MICRO_INTERVAL_SEC=300`
  - `ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC=900`
  - `ANALYTICS_HISTORY_WHALE_INTERVAL_SEC=600`

### 3. 数据源与采集定义
- 微观结构：
  - 来源固定为当前已连接交易所公开接口
  - 内容：订单簿、逐笔成交不平衡、资金费率、现货/永续基差
  - 支持 `binance/gate/okx`，优先当前主交易所
  - 若资金费率或 basis 不可取，只降级对应字段，不让整条记录报废
- 社区：
  - v1 明确定义为“社区代理层”，不是社交媒体舆情库
  - 来源：
    - 逐笔成交流向代理 `flow_proxy`
    - 官方公告源：Binance / OKX / Bybit
    - 安全事件先保留 placeholder，但必须显式标 `source_name=placeholder`
  - 不把 `twitter_watchlist` 当成已采集数据，只作为配置展示
- 巨鲸：
  - v1 来源：
    - `blockchain.info` 未确认大额 BTC 转账
    - 交易所 BTC 价格估值
  - 明确这是“公开链上大额转账代理”，不是地址标签级鲸鱼画像
  - `available=false` 时仍可入库，但必须写错误原因
- v1 不做付费源接入，但采集器接口按可替换 provider 设计，后续可接 Nansen / Santiment / Glassnode。

### 4. API 与前端行为
- 保留现有接口，但固定语义：
  - `POST /api/trading/analytics/history/collect`
    - 只负责触发一次采集
    - 返回本轮采集状态和降级信息
  - `GET /api/trading/analytics/history/health`
    - 只负责读历史库与最近状态
    - 不再默认做同步抓取
- 新增状态接口：
  - `GET /api/trading/analytics/history/status`
  - 返回最近一次各采集器的 `started_at/finished_at/status/error/rows_written`
- 数据页显示改为：
  - 历史快照数、最近更新时间、最近一次采集状态
  - 正常样本数 / 降级样本数
  - 每类数据的来源说明
  - 若是 placeholder 或代理数据，明确显示“代理源”“占位源”
- 历史体检区域禁止再因单次采集超时而整块空白；读取历史库必须优先成功。

### 5. 实施顺序
1. 将计划写入 `docs/analytics_history_db_plan.md`
2. 扩展三张表字段并补迁移/初始化逻辑
3. 抽出独立采集 service，拆分微观结构 / 社区 / 巨鲸三个 worker
4. 改造 `collect` 与 `health` 接口语义
5. 接入 Web 启动调度
6. 改数据页展示和异步刷新链路
7. 回归验证数据库、接口、页面和降级标记

## Public APIs / Interfaces
- 新增环境变量：
  - `ANALYTICS_HISTORY_ENABLED`
  - `ANALYTICS_HISTORY_MICRO_INTERVAL_SEC`
  - `ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC`
  - `ANALYTICS_HISTORY_WHALE_INTERVAL_SEC`
- 新增接口：
  - `GET /api/trading/analytics/history/status`
- 变更接口语义：
  - `GET /api/trading/analytics/history/health` 变为纯读接口
  - `POST /api/trading/analytics/history/collect` 变为明确的一次性采集入口
- 前端输出字段新增：
  - `capture_status`
  - `source_error`
  - `source_name`
  - `latency_ms`
  - `rows_written` 或等价汇总字段

## Test Plan
- 数据模型：
  - 三张表初始化后存在新增质量字段
  - 正常样本和降级样本都能入库
- 采集逻辑：
  - 微观结构超时时，仍写入一条 `degraded` 样本
  - 社区公告为空时，不报错，正常写入并标明源为空
  - 巨鲸源超时时，样本入库且 `available=false`
- 调度逻辑：
  - worker 启动后按配置周期运行
  - 禁用开关时不自动采集
- API：
  - `health` 不触发实时抓取也能返回历史结果
  - `collect` 返回本轮状态
  - `status` 返回最近采集状态
- 前端：
  - 数据页首次进入时先显示历史库
  - 后台补抓失败时仅在说明区告警，不清空表格
  - 降级样本在页面上有明确标记

## Assumptions
- 默认采用后台定时采集，而不是页面触发为主。
- 默认只用免费公开源，不接入付费数据服务。
- 默认允许保存降级样本，但必须显式标记，后续研究/展示不得把它当正常样本。
- v1 的“社区”定义为代理层，不承诺真实社交媒体情绪覆盖。
- v1 的“巨鲸”定义为公开链上大额转账代理，不承诺地址标签级准确性。
