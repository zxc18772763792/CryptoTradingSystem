# 山寨雷达页 v1 方案

## 摘要
- 新增一个和 `套利` 并级的顶层 tab，名称定为 `山寨雷达`，页面定位是“预警 + 选币”工作台，不直接下单；主目标是发现 `异动启动`、`庄家吸筹布局`、`高控盘警戒/跟踪` 三类山寨币状态。
- 进入实施阶段后的第一步固定为：先把本方案保存为本地文档，建议路径为 `E:\9_Crypto\crypto_trading_system\docs\ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md`，再开始代码实现。
- 默认服务口径固定为 `Binance / 4h / 研究币池前 30 个 / exclude_retired=true`，并保留 `1h / 4h / 1d` 切换；默认排序是 `布局优先`，不是单纯涨幅榜。
- 页面采用双引擎，但角色不同：`代理行为引擎` 决定主排名，负责从量价、波动、横截面、微观结构里识别操盘痕迹；`链上/外生确认引擎` 只做确认、加分和降权，不允许链上弱证据把弱代理候选推到榜首。
- 第一版最重要的后续动作是 `创建预警` 和 `带入研究工坊`；回测和自治代理只保留为二级动作，不让页面变成执行台。

## 实施顺序
- `Step 0`：将本计划原样保存到 `docs/ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md`，作为实现期间的本地单一依据文档。
- `Step 1`：新增后端雷达服务与 `/api/altcoin` 路由，先把扫描、评分、详情接口打通。
- `Step 2`：新增 `山寨雷达` tab 页面骨架与前端状态管理，先把榜单和详情检视器跑起来。
- `Step 3`：接入预警预设、研究工坊回填和跨页联动。
- `Step 4`：补测试、压性能、处理降级显示和错误保留逻辑。

## 页面设计
- 视觉与布局沿用现有 `研究工坊 / 套利` 的暗色工作台范式，做成“雷达台”而不是营销页；新 tab 放在 `研究工坊` 和 `套利` 同级，内部结构继续使用 `左侧配置 + 右侧工作区 + 明细检视器`。
- 左侧配置区放 `交易所`、`周期`、`币池`、`排序模式`、`仅看预警中`、`排除停更币`、`刷新雷达`；默认排序模式为 `布局优先`，可切到 `异动优先`、`高控盘优先`、`综合热度`。
- 顶部摘要区显示 `扫描币数`、`异动数`、`吸筹数`、`高控盘数`、`数据降级数`、`当前榜首`，让用户一眼看到今天有没有值得追踪的山寨币。
- 中部主表格作为核心工作区，列固定为 `排名 / 币种 / 布局分 / 异动分 / 吸筹分 / 控盘分 / 链上确认 / 新鲜度 / 状态标签 / 操作`；默认按 `布局分` 排序。
- 右侧明细检视器点击行后更新，展示 `两套引擎拆解`、`关键指标`、`理由列表`、`风险失效条件`、`研究跳转按钮`、`预警创建按钮`；设计成“看一行榜单，右侧马上知道为什么上榜”。
- 状态标签固定为 5 类，避免实现时再发明命名：`布局吸筹`、`异动启动`、`高控盘跟踪`、`高控盘警戒`、`派发风险`。
- 交互上要求三件事同时成立：点击任一榜单行可在 1 次交互内看到完整解释；切换排序模式不重新排版整体结构；创建预警后当前行立刻显示“已建预警”标记。

## 核心评分与判定
- 新增服务层 `core/research/altcoin_radar.py`，负责统一输出 `AltcoinRadarRow`；前端不自己拼评分，所有标签、分数、理由都由后端生成。
- `AltcoinRadarRow` 固定字段为：`symbol`、`layout_score`、`alert_score`、`anomaly_score`、`accumulation_score`、`control_score`、`chain_confirmation_score`、`risk_penalty`、`signal_state`、`tags`、`reasons_proxy`、`reasons_chain`、`data_quality`、`freshness`、`metrics`。
- `anomaly_score` 用于抓“异动启动”，公式固定为 `0.45 return_shock_pctile + 0.35 volume_burst_pctile + 0.20 range_expansion_pctile`；底层特征来自最近 1/3/6 bar 收益、量比、真实波幅扩张。
- `accumulation_score` 用于抓“吸筹布局”，公式固定为 `0.35 compression_inverse + 0.25 drift_stability + 0.20 absorption_proxy + 0.10 breakout_proximity + 0.10 positive_flow`；其中 `compression_inverse` 表示波动压缩，`absorption_proxy` 表示下影吸收/回落承接，`positive_flow` 优先取 symbol 级 community/flow snapshot，没有则置空不补猜。
- `control_score` 用于抓“高控盘代理”，公式固定为 `0.30 close_control + 0.25 liquidity_thinness + 0.20 impulse_after_compression + 0.15 spread_impact + 0.10 one_sided_flow`；这里定义的是“高控盘代理分”，不是地址集中度真值。
- `chain_confirmation_score` 作为确认引擎，公式固定为 `0.40 community_flow + 0.25 announcements + 0.20 funding_basis + 0.15 whale_context`，再乘以 `data_quality_factor`；它只加分或降权，不单独决定上榜。
- `risk_penalty` 固定包含 `安全事件`、`数据过旧`、`价差/流动性过差`、`快照缺失` 四类惩罚；任何一项极端时允许直接把 `signal_state` 改为 `高控盘警戒` 或 `派发风险`。
- 默认主排序 `layout_score` 固定为 `0.45 accumulation + 0.30 control + 0.15 anomaly + 0.10 chain_confirmation - risk_penalty`；`alert_score` 固定为 `0.55 anomaly + 0.20 accumulation + 0.15 control + 0.10 chain_confirmation - risk_penalty`。
- 标签阈值固定如下：`布局吸筹` = accumulation≥0.68 且 control≥0.55 且 risk_penalty<0.20；`异动启动` = anomaly≥0.72 且 (accumulation≥0.45 或 control≥0.45)；`高控盘跟踪` = control≥0.70 且 risk_penalty<0.25；`高控盘警戒` = control≥0.70 且 risk_penalty≥0.25；`派发风险` = anomaly≥0.70 且 accumulation<0.35 且 control≥0.65。
- 所有分数都按当前扫描 universe 做百分位归一，避免不同市场波动水平下阈值失真；实现时不要混用绝对值和百分位。

## 后端与接口
- 新增路由 `web/api/altcoin.py`，在 `web/main.py` 注册为 `/api/altcoin`；不要把这块继续塞进现有 `data.py`，避免研究/数据接口继续膨胀。
- 新增 `GET /api/altcoin/radar/scan`，参数固定为 `exchange`、`timeframe`、`symbols`、`limit`、`sort_by`、`exclude_retired`、`refresh`；返回 `summary`、`rows`、`scan_meta`、`warnings`。
- 新增 `GET /api/altcoin/radar/detail`，参数固定为 `exchange`、`timeframe`、`symbol`、`symbols`、`refresh`；返回 `selected_row`、`proxy_breakdown`、`chain_breakdown`、`sparkline`、`invalidate_conditions`、`related_candidates`。
- Universe 默认来源直接复用 `/api/data/research/symbols`；OHLCV、factor library、多币种相关性都复用现有数据加载逻辑，但在雷达服务内统一封装，不让前端并发拼 API。
- 全量扫描阶段只读取 `本地 K 线 + factor library + multi-assets + 最新 micro/community/whale snapshots`；`/data/onchain/overview` 只在右侧明细按需拉取，不参与全 universe 排名，避免 30 个 symbol 扫描时被重型链上请求拖垮。
- 新增批量快照加载器，一次性取出 universe 中每个 symbol 的最新 `AnalyticsMicrostructureSnapshot / AnalyticsCommunitySnapshot / AnalyticsWhaleSnapshot`；不要在扫描里做 30*3 次独立查询。
- 雷达结果做缓存，key 固定为 `exchange + timeframe + universe_hash + exclude_retired`；TTL 固定为 `1h=120s, 4h=300s, 1d=900s`，`refresh=true` 强制重算并覆盖缓存。
- 数据质量输出必须显式化；每一行都返回 `market_data_freshness`、`snapshot_freshness`、`chain_quality`、`degraded_reason`，前端不自己猜“为什么这行分数低”。

## 预警与跨页联动
- 预警不另造一套系统，继续复用现有 `notifications`；但需要给 `core/notifications/notification_manager.py` 增加两类规则：`altcoin_score_above` 和 `altcoin_rank_top_n`。
- `altcoin_score_above` 的参数固定为 `exchange`、`timeframe`、`universe_symbols`、`symbol`、`score_key(layout|alert|anomaly|accumulation|control)`、`threshold`、`channels`、`source_page=altcoin_radar`。
- `altcoin_rank_top_n` 的参数固定为 `exchange`、`timeframe`、`universe_symbols`、`symbol`、`sort_by(layout|alert|control)`、`rank_n`、`channels`、`source_page=altcoin_radar`。
- 为了让前端少拼规则细节，新增 `POST /api/altcoin/alerts/preset`；预设固定三种：`异动预警`、`吸筹预警`、`高控盘预警`，分别生成对应的 `altcoin_score_above` 规则并写入现有通知中心。
- 通知评估时，`web/api/notifications.py` 需要先按活跃 altcoin 规则聚合出唯一的雷达配置，再批量获取对应 scan snapshot 填入 context，之后再让 notification manager 做 rule evaluation；不要在 `_eval_rule` 里临时逐条重扫全市场。
- `带入研究工坊` 的行为固定为：把 `exchange / timeframe / symbol / universe_symbols` 回填到现有 `research` 页控件，切换到 `research` tab，并默认不自动跑全量 overview，只提示用户一键刷新；这样动作轻、可控。
- `回测` 和 `自治代理` 在 v1 只做次级按钮；按钮存在，但不默认出现在主行操作位，只放在右侧明细的次级操作区。

## 前端落地
- `web/templates/index.html` 新增 `tab-btn` 和 `tab-content`，id 固定为 `altcoin-radar`；新页面采用和 `套利` 同级的完整工作台，而不是塞进 `研究工坊` 的一个子模块。
- 新增 `web/static/js/altcoin_radar.js` 承担页面状态、渲染和交互；`app.js` 只补充 `loadAltcoinRadarTabData()`、tab loader 注册和跨页跳转钩子。
- 页面渲染分四块：`scan controls`、`summary strip`、`candidate ranking table`、`symbol inspector`；不要做卡片瀑布流，不要把主要信息拆成一堆小卡。
- 排名表必须支持客户端二次过滤，但后端排序结果仍是主数据源；过滤项固定为 `全部 / 仅布局吸筹 / 仅异动启动 / 仅高控盘 / 仅已建预警 / 仅数据新鲜`。
- 右侧 inspector 的理由展示固定分成 `代理行为证据` 和 `链上/外生确认` 两段，明确告诉用户“这次上榜主要是量价痕迹，还是外部事件确认”，避免黑盒感。
- 交互反馈要完整：刷新时保留旧榜单并标记 `refreshing`，请求失败时保留上次成功结果并显示错误原因，不允许整个区域清空成“暂无数据”。

## 测试计划
- 服务层单元测试至少覆盖 3 个合成 archetype：`缩量横盘后抬升` 应命中 `布局吸筹`，`放量突破` 应命中 `异动启动`，`低流动性急拉+上影回落` 应命中 `高控盘警戒` 或 `派发风险`。
- API 测试要覆盖 `scan`、`detail`、`refresh`、`degraded snapshot`、`universe 为空回退 research symbols`、`exclude_retired=true` 的结果稳定性。
- 通知测试要覆盖 `altcoin_score_above`、`altcoin_rank_top_n`、`cooldown`、`多规则共用同一 scan cache`，确保不会每条规则都触发一次全量扫描。
- 前端烟测至少覆盖：tab 切换首次加载、切换排序模式、点击行更新 inspector、创建预警后显示已绑定状态、带入研究工坊后 research 控件被正确回填。
- 验收标准固定为：默认 30 币扫描在命中缓存时 2 秒内可视完成、强制刷新时 8 秒内返回占位或新结果；数据缺失时页面仍有榜单且明确标出 `degraded`。

## 假设与默认
- 页面名称默认使用 `山寨雷达`，tab key 固定为 `altcoin-radar`。
- 实施时必须先落地本地计划文档，再开始任何代码改动；实现过程中若方案有调整，优先更新该文档后再改代码。
- 第一版的“双引擎”里，链上侧是 `symbol 级 community snapshot + onchain/exogenous context`，不是地址集中度/持仓分布的真值引擎；真正的 holder concentration、smart-money 标签、付费链上数据源不纳入 v1。
- 第一版不做自动下单、不把候选直接推入执行链路、不新增外部付费依赖；所有“高控盘”判断都明确标注为 `代理判断`。
- 默认只扫描研究币池前 30 个高流动性币种；如果本地 K 线覆盖不足，允许自动补足本地可用 symbols，但 UI 必须展示实际 `symbols_used`。
