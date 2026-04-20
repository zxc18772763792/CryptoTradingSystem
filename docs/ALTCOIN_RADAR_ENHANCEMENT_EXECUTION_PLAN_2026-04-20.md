# 山寨雷达增强执行方案 v2

日期：2026-04-20

适用仓库：`E:\9_Crypto\crypto_trading_system`

关联文档：

1. [ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md](/E:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md)
2. [COINGLASS_SIGNAL_ENHANCEMENT_PLAN_2026-04-18.md](/E:/9_Crypto/crypto_trading_system/docs/COINGLASS_SIGNAL_ENHANCEMENT_PLAN_2026-04-18.md)
3. [COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md](/E:/9_Crypto/crypto_trading_system/docs/COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md)

---

## 1. 目标

这份方案的目标不是继续把“山寨雷达”做成一个更复杂的榜单，而是把它明确升级成两个不同用途的发现系统：

1. `Perp Radar`
   - 面向 ORDI 这类“合约驱动 / OI 爆发 / 挤空加速”的币
   - 核心优势来自 CoinGlass 的衍生品和市场结构数据
   - 目标是做到“早于 4h 确认，在 15m-1h 阶段给出点火提示”

2. `Narrative Meme Radar`
   - 面向“币安人生”这类“叙事驱动 / 板块热度扩散 / 链上交易活跃”的币
   - CoinGlass 只做辅助确认，不再假设它能独立抓到最早期
   - 目标是做到“叙事发酵期先入榜，合约放大期继续加权”

最终交付不是单一分数的增强，而是：

1. 更快的点火检测
2. 更大的扫描覆盖
3. 更明确的行情分类
4. 更可执行的预警系统
5. 更适合后续 Codex 按阶段落地的工程拆解

---

## 2. 当前问题判断

基于当前代码和页面状态，山寨雷达已经具备以下基础：

1. 已有 `layout_score / alert_score / accumulation_score / control_score / chain_confirmation_score / derivatives_heat_score`
2. 已有 CoinGlass 市场快照和衍生品上下文接入
3. 已有榜单、详情、预警预设、研究工坊联动
4. 已有基础缓存和降级逻辑

但如果目标变成“尽量早发现 ORDI / 币安人生这类行情”，当前系统仍有 5 个结构性缺口：

1. 时间框架偏慢
   - 目前默认设计仍偏 `1h / 4h / 1d`
   - 对“突然点火”的币，4h 视角会明显偏晚

2. 排名逻辑仍偏确认型
   - 现在更擅长在“已经有一定结构”的币中找优先级
   - 不擅长发现“刚从底部点火”的币

3. universe 仍偏小
   - 当前主流程以研究币池前 30 个为核心
   - 这适合工作台，但不适合“发现系统”

4. CoinGlass 优势还没有被组织成“事件因子”
   - 现在更多是把衍生品字段纳入综合分
   - 还没有明确做 `OI 异动 / 清算偏向 / 多所同步放量 / rank jump`

5. 缺少叙事型雷达
   - 对 meme / 板块 / 中文社区热点，当前只有弱链上和弱外生确认
   - 还没有单独的板块热度、链上活跃、社媒热词机制

---

## 3. 目标行情分型

后续所有实现都必须先区分这两类行情，不允许再用一套分数试图兼顾全部。

### 3.1 ORDI 型

特征：

1. OI 在短时窗口快速抬升
2. 爆仓偏向明显，常见为 short liquidation 增强
3. 成交量和价格同步放大
4. 多个交易所同时出现持仓和量能扩张
5. funding 尚未极端，说明可能仍处于中前段

适合用 CoinGlass 发现。

### 3.2 币安人生型

特征：

1. 叙事先行
2. 社群和板块热度先扩散
3. DEX 成交和地址活跃先启动
4. 合约数据要么滞后，要么覆盖不完整
5. 资金偏向“短时热点轮动”，不是纯 perp 驱动

不适合只用 CoinGlass 发现。

### 3.3 系统要求

后续输出层必须明确标注行情来源：

1. `perp_ignition`
2. `perp_continuation`
3. `narrative_ignition`
4. `narrative_confirmation`
5. `crowded_late_stage`

不能只给一个“布局吸筹”或“异动启动”而没有机制来源。

---

## 4. 产品结构升级

### 4.1 页面保持一个 tab，但内部拆成两套榜单

页面仍保留 `山寨雷达` 这个顶层入口，不再新开二级 tab 入口，但内部工作区改成双榜模式：

1. `Perp Radar`
   - 默认榜单
   - 面向 OI/清算/衍生品驱动币

2. `Narrative Radar`
   - 次级榜单
   - 面向板块 / meme / 链上叙事驱动币

### 4.2 页面新增视图切换

左侧控制区增加：

1. `雷达模式`
   - `Perp`
   - `Narrative`
   - `Combined`

2. `周期视图`
   - `15m 点火`
   - `1h 延续`
   - `4h 确认`

3. `排序模式`
   - `点火优先`
   - `延续优先`
   - `确认优先`
   - `拥挤度优先`
   - `综合热度`

### 4.3 页面新增榜单列

当前榜单列保留，但需要新增以下列或在详情中优先展示：

1. `点火分 ignition_score`
2. `跃升分 rank_jump_score`
3. `OI 异动`
4. `清算偏向`
5. `板块热度`
6. `信号来源`

其中：

1. `OI 异动`、`清算偏向` 主要服务 Perp Radar
2. `板块热度`、`信号来源` 主要服务 Narrative Radar

---

## 5. 总体架构改造

### 5.1 新增服务层模块

建议新增而不是继续把所有逻辑塞进一个文件：

1. `core/research/altcoin_radar_perp.py`
   - perp 点火、延续、确认相关评分

2. `core/research/altcoin_radar_narrative.py`
   - narrative 点火和板块热度评分

3. `core/research/altcoin_radar_universe.py`
   - 统一管理扫描 universe、watchlist、板块池

4. `core/research/altcoin_radar_events.py`
   - 统一输出事件型预警和 rank jump

5. 保留 `core/research/altcoin_radar.py`
   - 作为聚合层
   - 负责统一 row schema、combined 排名、详情拼装

### 5.2 后端接口保持 `/api/altcoin`，但要扩展参数

现有接口上新增参数，不另起一个平行 API：

1. `mode=perp|narrative|combined`
2. `view=15m|1h|4h`
3. `sort_by=ignition|continuation|confirmation|crowding|heat|layout|alert|control`
4. `universe_scope=research|expanded|watchlist`

### 5.3 数据层增加“扫描缓存”和“事件缓存”

新增两个缓存层：

1. `scan snapshot cache`
   - 维持现有榜单结果

2. `event snapshot cache`
   - 记录最近 15m / 1h 的分数变化、排名变化、事件触发

后续 `rank jump`、`cross up`、`ignition trigger` 都从事件缓存读取，不临时反推。

---

## 6. 数据源矩阵

### 6.1 Perp Radar 数据源

必须优先接入和利用：

1. CoinGlass `coins markets`
2. CoinGlass `open interest history`
3. CoinGlass `liquidation history`
4. CoinGlass `funding rate history`
5. CoinGlass `long-short ratio / taker imbalance`
6. 本地 OHLCV
7. 本地 micro/community/whale snapshots

### 6.2 Narrative Radar 数据源

建议新增一层“轻量叙事数据”，按优先级推进：

1. 已有 community snapshot
2. 公告/新闻聚合
3. DEX 成交额和交易笔数
4. 活跃地址 / 新增地址
5. watchlist 板块映射
6. 中文热词 / 项目别名字典

### 6.3 重要原则

1. CoinGlass 不再被假设为 narrative 的第一发现源
2. narrative 层允许先用简化版本起步
3. 没有链上付费依赖时，可以先做“板块热度 + 新闻/公告 + 交易活跃”代理层

---

## 7. Universe 升级方案

### 7.1 扫描 universe 拆三层

1. `research universe`
   - 延续现有研究币池
   - 适合默认视图和研究联动

2. `expanded universe`
   - 扩展到 `80-150` 个 symbol
   - 适合发现型任务

3. `watchlist universe`
   - 人工维护的热门 watchlist
   - 包含中文 meme、BSC、Rune/铭文、交易所概念等板块

### 7.2 expanded universe 生成规则

建议由 4 组来源合并：

1. CoinGlass altcoin universe
2. research symbols
3. 最近 24h 成交/波动活跃币
4. narrative watchlist

### 7.3 universe 输出

后端对每次扫描都必须返回：

1. `symbols_requested`
2. `symbols_used`
3. `universe_scope`
4. `board_membership`
5. `excluded_reasons`

这样前端可以明确告诉用户“这次到底扫了什么”。

---

## 8. 新评分体系

### 8.1 保留旧分数，但新增三层新分数

保留：

1. `layout_score`
2. `alert_score`
3. `accumulation_score`
4. `control_score`
5. `chain_confirmation_score`

新增：

1. `ignition_score`
2. `continuation_score`
3. `rank_jump_score`

### 8.2 Perp Radar 核心分数

#### ignition_score

面向“刚开始动”的币，适合 15m / 1h。

建议组成：

1. `oi_zscore_1h`
2. `volume_zscore_1h`
3. `short_liq_share`
4. `breakout_from_compression`
5. `exchange_breadth`

建议公式：

```text
ignition_score =
0.30 * oi_zscore_1h +
0.25 * volume_zscore_1h +
0.20 * short_liq_share +
0.15 * breakout_from_compression +
0.10 * exchange_breadth
```

#### continuation_score

面向“已经启动，但还没有明显过热”的币。

建议组成：

1. `oi_change_4h`
2. `price_change_4h`
3. `taker_buy_sell_imbalance`
4. `flow_confirmation`
5. `funding_not_overcrowded`

#### crowding_late_score

面向“容易追在末端”的币。

建议组成：

1. `funding_extreme`
2. `crowding_risk_score`
3. `long_short_ratio_extreme`
4. `distribution_score`
5. `liq_after_spike`

这个分数不是为了推高排名，而是为了降权和告警。

### 8.3 Narrative Radar 核心分数

#### narrative_heat_score

建议组成：

1. `board_heat`
2. `news_announcement_burst`
3. `community_flow`
4. `tx_count_acceleration`
5. `active_address_acceleration`

#### meme_rotation_score

建议组成：

1. `sector_breadth`
2. `same-theme token count`
3. `watchlist hit count`
4. `short-term turnover spike`
5. `market_cap elasticity`

### 8.4 combined 组合策略

`combined` 模式不做简单平均，而是按来源选择路径：

1. 如果 `perp ignition` 很高，优先按 Perp 排名
2. 如果 `narrative heat` 很高且 perp 数据弱，按 Narrative 排名
3. 如果两者同时强，标记为 `multi-engine`

---

## 9. 事件系统升级

### 9.1 从“分数预警”升级到“事件预警”

新增预警类型：

1. `altcoin_ignition_cross_up`
2. `altcoin_rank_jump_top_n`
3. `altcoin_perp_burst`
4. `altcoin_narrative_heat_spike`
5. `altcoin_crowding_risk_spike`

### 9.2 事件定义

#### rank_jump_top_n

定义：

1. 当前排名进入 top `N`
2. 且相对 15m 前至少跃升 `M` 位

#### ignition_cross_up

定义：

1. `ignition_score` 从阈值下方穿越到上方
2. 且 `oi_zscore_1h` 与 `volume_zscore_1h` 至少有一个同步放大

#### narrative_heat_spike

定义：

1. `narrative_heat_score` 在 1h 内显著抬升
2. 且 `board_heat` 或 `announcement_burst` 命中

---

## 10. 页面与交互改造

### 10.1 左侧控制区

新增以下控件：

1. `雷达模式`
2. `视图周期`
3. `universe scope`
4. `仅看点火`
5. `仅看排名跃升`
6. `仅看 narrative`
7. `仅看 perp`

### 10.2 榜单行为

新增这三种交互：

1. `查看事件`
   - 打开最近 15m / 1h 的 score 和排名变化

2. `加入 watchlist`
   - 加入 narrative/perp watchlist

3. `创建事件预警`
   - 不再只建“分数超过阈值”

### 10.3 详情检视器

新增三个面板：

1. `点火路径`
   - 解释为什么现在被识别为 ignition

2. `事件时间线`
   - 展示近几次 `cross up / rank jump / crowding spike`

3. `板块联动`
   - 展示同主题候选、板块热度、watchlist 命中

---

## 11. 需要改动的文件

### 11.1 后端

重点文件：

1. `core/research/altcoin_radar.py`
2. `core/research/altcoin_radar_perp.py`
3. `core/research/altcoin_radar_narrative.py`
4. `core/research/altcoin_radar_events.py`
5. `core/research/altcoin_radar_universe.py`
6. `web/api/altcoin.py`
7. `web/api/data.py`
8. `core/data/coinglass_altcoin.py`

### 11.2 前端

重点文件：

1. `web/templates/index.html`
2. `web/static/js/altcoin_radar.js`
3. `web/static/css/style.css`

### 11.3 测试

新增或扩展：

1. `tests/test_altcoin_radar_ui_assets.py`
2. `tests/web/test_altcoin_route.py`
3. `tests/test_altcoin_radar_derivatives.py`
4. `tests/test_altcoin_notification_manager.py`
5. `tests/test_altcoin_radar_events.py`
6. `tests/test_altcoin_radar_narrative.py`
7. `tests/test_altcoin_radar_universe.py`

---

## 12. 分阶段执行顺序

### Phase 1：Perp 点火层

目标：

1. 先把 ORDI 型机会抓出来
2. 不碰 narrative 层

必须完成：

1. 新增 `ignition_score`
2. 新增 `rank_jump_score`
3. 新增 `15m / 1h / 4h` 视图参数
4. 新增 `点火优先` 排序
5. 新增 `altcoin_ignition_cross_up`
6. 新增 `altcoin_rank_jump_top_n`

验收标准：

1. 扫描接口在 warm cache 下 2 秒内返回
2. expanded universe 下仍可在 8 秒内给出结果或降级结果
3. 页面能显示“点火分、跃升分、信号来源”

### Phase 2：Narrative 雷达

目标：

1. 解决“币安人生型”发现能力不足的问题

必须完成：

1. 新增 `Narrative Radar`
2. 新增 `narrative_heat_score`
3. 新增 `watchlist universe`
4. 新增 `board_heat` 和 `theme linkage`
5. 新增 `altcoin_narrative_heat_spike`

验收标准：

1. 能输出 narrative-only 候选
2. 即使没有强 perp 数据，也能入榜
3. 详情页能解释“为什么这是叙事驱动而非合约驱动”

### Phase 3：事件系统和执行联动

目标：

1. 把榜单变成真正可追踪的发现系统

必须完成：

1. 新增事件缓存
2. 新增时间线面板
3. 新增事件预警预设
4. 新增 watchlist 管理
5. 优化研究工坊联动参数

---

## 13. 测试与回归要求

### 13.1 单元测试

至少覆盖：

1. OI 暴增但 funding 未极端时，`ignition_score` 应高
2. funding 极端且 distribution 上升时，`crowding_late_score` 应高
3. narrative 热度上升但 perp 数据弱时，仍能进入 narrative 排名
4. `rank_jump_score` 能正确识别短时跃升

### 13.2 接口测试

至少覆盖：

1. `/api/altcoin/radar/scan?mode=perp&view=15m`
2. `/api/altcoin/radar/scan?mode=narrative&view=1h`
3. `/api/altcoin/radar/detail`
4. `/api/altcoin/alerts/preset`
5. 新事件型预警接口

### 13.3 前端烟测

至少覆盖：

1. 首次切页
2. 切换 `Perp / Narrative / Combined`
3. 切换 `15m / 1h / 4h`
4. 创建事件预警
5. 展开事件时间线
6. 带入研究工坊

---

## 14. 验收口径

如果要判断“这次改造有没有真的完成目标”，统一按下面 5 条判断：

1. 山寨雷达不再只是“确认型榜单”，而是可以输出 `点火候选`
2. Perp 和 Narrative 两类币在页面和接口层都有明确区分
3. CoinGlass 不再只是综合分中的弱加分项，而是形成明确的 perp 点火事件逻辑
4. Narrative 型币不再依赖 CoinGlass 单点发现
5. 预警系统从 `score_above` 升级到 `event-driven`

---

## 15. 给后续 Codex 的执行说明

后续如果让 Codex 直接按本方案施工，建议严格按下面顺序执行：

1. 先读本文件，再读现有 [ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md](/E:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md)
2. 先做 `Phase 1`，不要一口气把 narrative 和 perp 混着改
3. 每个 phase 结束后先补测试，再继续下一阶段
4. 每次提交都优先保证：
   - 接口可降级
   - 页面不悬挂
   - 旧有 `layout / alert / control` 能继续工作
5. 任何新增数据源如果会影响首屏性能，必须先加缓存、超时和 fallback，再接入页面主链路

---

## 16. 本方案的建议落地策略

如果只允许做一轮改造，推荐优先级如下：

1. `ignition_score + rank_jump_score + 15m/1h 视图`
2. `expanded universe`
3. `Perp event alerts`
4. `Narrative Radar`
5. `watchlist / board heat / theme linkage`

原因很简单：

1. ORDI 型可以更快见效
2. narrative 型虽然更难，但可以在第二阶段独立推进
3. 这样既能尽快提升抓涨能力，也不会一次把系统复杂度拉爆

