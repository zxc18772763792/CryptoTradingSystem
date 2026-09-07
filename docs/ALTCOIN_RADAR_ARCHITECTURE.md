# 山寨雷达 — 代码架构与地图

更新：2026-09-07
用途：把多轮迭代积累下来的山寨雷达系统"整理"成一张可维护的地图——每个文件干什么、
数据怎么流、依赖什么、哪些验证过哪些没有。看这一份就知道全貌。

---

## 0. 一句话定位

山寨雷达是一个**分层发现系统**:核心是**唯一验证过的周度横截面模型**(挑最可能起飞的
小市值币),外面裹一圈**诚实标注的上下文/风险维度**(实时异动、链上控盘、多空拥挤、
大盘 KOL 共识),再加预警、检视器和数据基建。设计原则:**验证过的当核心,没验证的
只当上下文,并把这个区分明明白白写在 UI 上。**

## 1. 分层与验证状态

| 层 | 功能 | 验证状态 |
|---|---|---|
| **核心** | 周度「本周起飞候选」横截面模型(Top-15) | ✅ 走查+对抗验证,事件级 ~1.9× |
| 上下文 | 实时扫描「现在在动的」(状态机打分) | ⚠️ 描述当下,日内择时已证伪 |
| 上下文 | 链上持仓集中度(前10持仓)+ 解锁临近 | ⚠️ 展示列 |
| 上下文 | LSR 多空拥挤度 | ⚠️ 仓位非预测 |
| 上下文 | 大盘 KOL 共识 + 风险体制(5 大币) | ⚠️ 宏观背景,未验证 |
| 预警 | 点火/跃升/拥挤/叙事 → 飞书 | — |
| 基建 | 周度/持仓/Alpha 快照(前向积累) | 🔧 为将来建模攒数据 |

## 2. 运行时代码地图(雷达页真正跑的东西)

### 后端 API — `web/api/altcoin/` 包（原 2.7k 行单文件,已按内聚拆分）
原 `web/api/altcoin.py` 已重构为一个包。`__init__.py`(~140 行)是**薄装配层**:
把各子路由并进一个 `router`,并**原样再导出**公开 + monkeypatch 接口(`altcoin_api.<name>`),
所以 `from web.api.altcoin import ...` / `import web.api.altcoin as alt` 的旧接口一字不变。

子模块(测试按各自归属模块 patch,如 `altcoin_api.scan._resolve_universe`、`altcoin_api.detail.get_onchain_overview`):
- `constants.py` / `state.py` — 常量+请求模型 / 进程内可变缓存(单例,按引用共享)。
- `helpers.py` — 纯函数:归一化、快照序列化、行情新鲜度、Binance 公共行情构造、告警预设查表。
- `cache.py` — 扫描缓存键 + 单飞载荷装配 + 淘汰。
- `scan.py`(~1.35k 行,不可再分的热路径引擎)— 宇宙解析 / 行情+快照加载 / 告警规则匹配 /
  `_compute_scan_payload` / 缓存快照 `get_altcoin_scan_snapshot` / 通知上下文 + `GET /radar/scan`、`GET /radar/events`、`warm_default_scan_cache`。
- `detail.py` — `GET /radar/detail` + `POST /radar/{symbol}/research-proposal`(链上/外生确认,15s/10s 预算)。
- `alerts.py` — `POST`/`DELETE /alerts/preset`。
- `universe.py` — `GET /radar/watchlist`(+`POST`/`DELETE`)、`GET /radar/universe`、`GET /radar/collector`。
- `pump.py` — `GET /radar/pump-watchlist` + `POST .../refresh`(**周度模型名单**,读 latest.json,refresh 触发子进程)。
- `signals.py` — `GET /radar/lsr-crowding`、`GET /radar/kol-consensus`。

扫描主入口 `GET /radar/scan`:宇宙解析(scope) → 缓存(单飞) → `_compute_scan_payload`
(本地 K 线 + coins-markets 快照 + 因子/相关性/微结构/衍生品) → `build_altcoin_rows` →
mode 过滤 → 排序 → 限量。缓存 key = exchange+tf+宇宙哈希+mode+view+scope;TTL 15m→30s/1h→60s/4h→300s;上限 48 条(会淘汰)。

### 核心评分 — `core/research/altcoin_radar.py`（~1k 行,已按内聚拆出 3 个 sibling）
- `build_altcoin_rows(...)`(留在本文件,~930 行的核心行构建器)— 每币算所有分(布局/异动/吸筹/控盘/点火/跃升/叙事),定状态机
  (派发风险>高控盘警戒>布局吸筹>异动启动>高控盘跟踪),打标签,算事件。本文件只保留它 + 常量,其余按整函数搬到 sibling 并 re-export 公开 API。
- `altcoin_radar_metrics.py` — 常量(VALID_TIMEFRAMES/TIMEFRAME_SECONDS/STATE_*)、纯数值/序列算子(ret/vol/atr/吸收/突破…)、逐信号取值器、market-snapshot 指标构造。
- `altcoin_radar_ranking.py` — 百分位/加权、排序键、优先级打分、**`sort_rows`/`summarize_rows`(公开)**、状态机标签 `_signal_state_for_row`/`_state_tags`。
- `altcoin_radar_detail.py` — **`build_detail_payload`(公开)** + 检视器详情辅助(sparkline/top drivers/链上百分位回填/行动计划)。
- 公开 API(被 `web/api/altcoin` 的 scan/detail/helpers 与测试引用):`build_altcoin_rows`、`sort_rows`、`summarize_rows`、`build_detail_payload`、`TIMEFRAME_SECONDS`、`VALID_TIMEFRAMES` — 全部从 `altcoin_radar.py` re-export,导入路径不变。**本模块无 monkeypatch 耦合**(测试只 import+调用,不 patch),故拆分零改测试。
- **排名/点火历史按 timeframe 分键**(见 events 模块),避免 15m/4h 交替产生幻影事件。

### 信号子模块
- `altcoin_radar_perp.py` — 合约点火/延续/拥挤分(funding/多空比/taker/OI)。
- `altcoin_radar_narrative.py` — 叙事/Meme 轮动/热度分。
- `altcoin_radar_universe.py` — 宇宙 scope:`research`(30)/`expanded`(~100)/`watchlist`/`alpha`(~388);
  `normalize_altcoin_pair`、watchlist 增删、`resolve_universe_scope`。
- `altcoin_radar_events.py` — **内存**排名快照 + 事件(点火穿越/跃升/拥挤/叙事);`context` 参数按周期隔离;进程重启即清零。

### 周度模型 — `core/research/pump_precursor.py`（138 行）
- `FEATURES`(16 个)、`build_daily_features`(训练/推理**共用**,防偏斜)、`score_universe`(横截面 rank→逻辑回归,BLAS-free)、`top_feature_drivers`。
- 权重:`data/research/pump_watchlist/model_weights.json`(由 `scripts/pump_precursor_panel.py` 时间切分训练导出)。

### LSR + KOL — `core/data/coinglass_lsr.py`（185 行）
- `fetch_lsr_ranking`/`fetch_lsr_signals`(TTL 缓存)、`crowding_label`。
- `fetch_kol_consensus`(5 大币 + `_aggregate_risk_tone`)、`get_cached_kol_symbol`/`get_cached_risk_tone`(同步缓存读,供策略)。
- KOL 只覆盖 BTC/ETH/SOL/DOGE/BNB,路径 `/api/lsr/consensus/v1/*`。

### 前端 — `web/static/js/altcoin_radar.js`（~2.6k 行）+ `web/templates/index.html`
- 主列顺序:**本周起飞候选(头牌)** → 大盘 KOL 共识条 → LSR 拥挤度卡 → 摘要/元信息 → 现在在动的(实时扫描) → 检视器。
- 竞态守卫(scanSeq/detailSeq)、失败保留上一版、后台刷新轮询。
- `loadAltcoinRadarTabData` 进 tab 时拉:pump-watchlist / kol-consensus / lsr-crowding / 实时 scan。
- 改 JS/CSS 必须 bump `web/asset_versions.py`(缓存清除);当前 altcoin_radar.js v29。

## 3. 数据来源与依赖

| 数据 | 来源 | 经由 |
|---|---|---|
| 市值 / OI / 费率(现值) | Coinglass coins-markets | vip2 relay → `coinglass_altcoin.load_coinglass_market_snapshots` |
| OI/费率历史 | Coinglass(非版本化路径) | vip2 relay → `coinglass_client`(strip /v3 /v4) |
| 日线/K线/资金费 | Binance fapi(免费) | 直接 httpx |
| 持仓集中度(top10) | GeckoTerminal token info(免费) | `snapshot_onchain_features.fetch_holder_row` |
| 解锁日历 | DefiLlama datasets 桶(免费) | `fetch_unlock_features` + `config/onchain_unlock_slugs.json` |
| LSR / KOL 共识 | vip2 relay(lsr 桶) | `coinglass_lsr` |
| Alpha 新币目录/K线 | Binance Alpha(后台采集器) | `core/data/binance_alpha.py` |

**中转迁移(2026-09-06)**:keystore relay 挂了(403)→ vip2.coinglass.site。差异:vip2 用**非版本化路径**,
客户端加 `COINGLASS_STRIP_API_VERSION` 剥 `/v3 /v4`。详见 [[coinglass-keystore-relay]] 记忆。

## 4. 配置与密钥（都 gitignored，永不进 git）

- `.env.local` — `COINGLASS_BASE_URL`=vip2、`RATE_LIMIT_PER_MIN`=11、`STRIP_API_VERSION`=true(优先级高于 settings.py 默认)。
- `config/coinglass_api_key.txt` — Coinglass/LSR key。
- 非密钥:`config/onchain_unlock_slugs.json`、`config/onchain_full_float_bases.json`(种子表,已提交)。

## 5. 周度批处理与调度

`scripts/run_pump_watchlist_weekly.bat`(计划任务 `CryptoTradingSystem_PumpWatchlistWeekly`,周一 08:30):
1. `generate_pump_watchlist.py` — 全宇宙打分 → Top-15 名单 + **自拉 top-N 的持仓/解锁**(2026-09-06 起,不再依赖独立快照)。
2. `snapshot_onchain_features.py` — 110 币持仓/解锁快照(**仍需保留:`web/api/ai_research.py` 读它**)。
3. `snapshot_alpha_fundamentals.py` — Alpha 新币基本面 point-in-time 快照(前向积累,8-12 周后可建模)。

## 6. 相关策略（策略库）

- `strategies/quantitative/oi_mcap_ambush.py` — A 吸筹/B 逼空/C 点火(**回测全负,注册为告警/研究,勿自动实盘**)。
- `strategies/macro/kol_consensus.py` — KOL 共识多空(5 大币,置信度门控,**backtest 不支持=无历史,纸面/未验证**)。

## 7. 已知边界与技术债（诚实记录）

- **新币盲区**:周度模型只覆盖有永续+35天历史的 130 币,看不见最新 Alpha/Gate 新币(它们无永续衍生品数据)。正用 Alpha 快照前向补,2-3 个月后见分晓。
- **KOL 只 5 大币**:是大盘风险体制信号,不是山寨选币。
- **事件历史是内存**:web 重启即清零,跃升分需 ~15 分钟重建;多 worker 会各持一份(当前单进程无碍)。
- **持仓/解锁覆盖**:原生链币(无 DEX 合约)、不在 GeckoTerminal 的币 → 空;meme 币无 vesting → 解锁空(如实,非 bug)。
- **`web/api/altcoin.py` 已拆包**(2.7k 行 → `web/api/altcoin/` 11 个内聚模块,行为不变,测试按新家 patch)。
- **`core/research/altcoin_radar.py` 已拆**(2.15k 行 → 主文件 ~1k + `altcoin_radar_metrics`/`_ranking`/`_detail` 三个 sibling,整函数搬迁、re-export 公开 API,行为不变,零改测试)。
- **`build_altcoin_rows` 已拆**(原 ~930 行单函数 → ~170 行编排器 + `_build_symbol_interim`(pass1:原始指标+interim)+ `_score_symbol_row`(pass2:百分位→打分行)。逐字节搬迁(仅 dedent + `continue`→`return` / `rows.append(row)`→`return row`);用 8 币金标准(golden-master)验证前后输出逐字节一致(屏蔽事件的墙钟时间戳后)。
  - **⚠ 已知潜伏 bug(拆分中如实保留,未改)**:pass2 的顶层 `row["squeeze_score"]` 与 `alert_score` 的 squeeze 项用的是**从 pass1 最后一个币泄漏下来的 `squeeze_signal`**(所有行同值),不是每币值。单币调用看不出;多币扫描下每行顶层 squeeze_score 都是最后一个币的值。已串成显式参数保持行为一致——**值得单独修**(改成 `item["metrics_raw"]["squeeze_score"]` 或 `pct` 派生;会改多币生产输出,需另跑金标准)。
  - `web/static/js/altcoin_radar.js`（~2.6k 行)— 单个 IIFE 闭包共享状态,无打包器;高风险低收益,**暂不拆**。

## 8. 测试

`tests/test_altcoin_radar_*`、`tests/web/test_altcoin_route.py`、`test_pump_watchlist_route.py`、
`test_kol_lsr.py`、`test_oi_mcap_ambush_strategies.py`、`test_pump_precursor.py`、
`test_strategy_library_and_factors.py`(改雷达代码后跑这批)。
拆包后测试按名字**新归属模块** patch:`altcoin_api.scan.*`(扫描热路径/`_resolve_universe`/`_compute_scan_payload`)、
`altcoin_api.detail.*`(detail/on-chain)、`altcoin_api.universe.*`(watchlist/universe 路由)、`altcoin_api.pump._PUMP_WATCHLIST_DIR`;
`altcoin_api.notification_manager` 仍在包根(patch 的是共享单例的方法,跨模块生效)。

*研究/工程记录,不构成投资建议。*
