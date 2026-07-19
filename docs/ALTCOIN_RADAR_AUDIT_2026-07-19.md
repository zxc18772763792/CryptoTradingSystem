# 山寨雷达页排查报告（功能逻辑梳理 + Bug 清单）

日期：2026-07-19
范围：`web/api/altcoin.py`（~2.4k 行）、`web/static/js/altcoin_radar.js`（~2.4k 行）、
`core/research/altcoin_radar*.py`（~3.3k 行）、事件/通知链路
回归：修复后 `tests/test_altcoin_radar_*`、`tests/web/test_altcoin_route.py`、
`tests/test_altcoin_radar_events_context.py`（新增）全部通过。

---

## 一、功能逻辑梳理（数据流）

```
前端 controls(exchange/timeframe|view/mode/scope/sort)
  → GET /api/altcoin/radar/scan
    → _resolve_universe(scope: research 30 / expanded 100 / watchlist 100)
       └ retired 过滤 + 空结果回退 research 池（带警告）
    → 缓存层（key = exchange+tf+universe哈希+mode+view+scope）
       ├ TTL: 15m→30s / 1h→60s / 4h→300s
       ├ 命中→直接返回；过期→后台单飞刷新 + 先回旧快照(stale=true)
       └ 计算路径 _compute_scan_payload：
          ├ 本地 K 线帧 + CoinGlass coins-markets 快照（并发）
          ├ 快照缺失→Binance 公共 ticker 兜底（45s TTL，双市场，1000x 前缀感知）
          ├ 新鲜度过滤（陈旧 K 线弃用，用快照兜底）
          ├ 因子库 + 多资产相关性 + 微结构/社区/鲸鱼/衍生品四类 DB 快照
          ├ coins-markets 覆盖过期衍生品快照（30min 阈值）
          └ build_altcoin_rows（核心评分）
             ├ 状态机：派发风险 > 高控盘警戒 > 布局吸筹 > 异动启动 > 高控盘跟踪
             ├ 信号源：perp_ignition/continuation/crowded_late_stage/narrative_*
             ├ 事件：点火穿越/排名跃升/拥挤尖峰/叙事热度（内存缓存，500 条上限）
             └ 排名历史（本次修复：按 timeframe 分键）
    → mode 过滤（perp/narrative/combined）→ sort_rows → 去重 → 限量
  → 前端 renderScan（seq 守卫防竞态）→ 检视器 detail（15s/10s 链上预算）
预警：POST /alerts/preset 建规则 → notification_manager 按规则 context 复扫
     → 消费 row.event_flags 触发（飞书）
```

前端要点：`scanSeq`/`detailSeq` 竞态守卫完备；扫描失败保留上一版榜单；
背景刷新轮询在 cache.refreshing 时启动、切换配置时清理——这层没有发现问题。

## 二、修复的 Bug（本次提交，共 7 项）

| # | severity | 位置 | 问题 | 修复 |
|---|---|---|---|---|
| 1 | 高 | `core/research/altcoin_radar_events.py` + `altcoin_radar.py` | 排名/点火历史全局共享：15m 与 4h 扫描交替写同一份历史，`ignition_cross_up`/`rank_jump` 用跨周期分数差计算 → **幻影事件直接触发飞书预警** | 历史按 `timeframe\|symbol` 分键（`context` 参数），事件只与同周期历史比较；新增 3 个隔离测试 |
| 2 | 高 | `web/api/altcoin.py` `_clear_altcoin_scan_cache` | watchlist 增删会 cancel 进行中的刷新任务，而请求端 `asyncio.shield(task)` 对**内部任务被取消**不设防 → 并发请求收到 CancelledError/500 | 不再 cancel，仅解除登记 + 清缓存；孤儿任务自然完成后由容量淘汰回收 |
| 3 | 中 | 同上 `_ALTCOIN_SCAN_CACHE` | 缓存 key 含宇宙哈希，key 流动但**从不淘汰** → 长期进程内存缓慢增长 | 新增 `_evict_altcoin_scan_cache`（上限 48 条，按 stored_at 淘汰） |
| 4 | 中 | `_load_latest_snapshot_map` / `_load_latest_derivatives_snapshot_map` | 无时间下限、无 LIMIT：每次扫描把宇宙内**全部历史快照行**拉进内存（表只增不减，扫描越用越慢） | 增加 7 天 timestamp 下限（远超一切新鲜度阈值） |
| 5 | 中 | `_resolve_universe` | watchlist scope 上限 30：用户 watchlist 80+ 条时**静默只扫前 30 个** | 上限提到 100（与 expanded 一致）+ 超限显式警告 |
| 6 | 中 | `scripts/generate_pump_watchlist.py` / `build_ambush_dataset.py` | `request_dataset` 会把 `CoinglassBudgetExceeded` 包装成 `CoinglassError` 字符串重抛 → 预算耗尽被当成永久失败**跳过该币**而不是等待重试 | 错误文本含 `budget_exhausted` 时改为 sleep-retry |
| 7 | 低 | `get_altcoin_scan_snapshot` | 死分支：`refresh` 为真时已提前 return，后面仍按 `if refresh` 选警告文案 | 清理为单一文案 |

## 三、已知行为与限制（未改，属设计权衡，先记录）

1. **事件与排名历史是进程内存**：Web 服务重启即清零（事件时间线短暂为空、
   跃升分需 ~15 分钟重建）；多 worker 部署会各持一份（当前单进程无此问题）。
   如需持久化，落 SQLite 即可，但要考虑写放大。
2. **自定义币池的残余排名噪音**：同一 timeframe 内，30 币池与 100 币池的
   排名量纲不同，用户混用自定义 symbols 扫描仍会带来小幅 rank 抖动
   （本次修复已消除主要来源=跨周期分数错配；如需彻底，可把宇宙哈希也
   并入 context，代价是历史更碎、跃升信号更迟钝）。
3. **watchlist > 100 仍会截断**（现在有警告提示）。
4. `summarize_rows` 会对已排序行再排一次（浪费，量小无感，未动）。
5. 结果不缓存的场景（市场数据全体不新鲜）会每请求重算——单飞锁限制了
   并发爆炸，属"宁可慢不可错"的现有取舍。
6. `/radar/watchlist` GET 里 delisted 探测依赖公共 ticker 8s 超时，超时即
   判空数组（不误报，但可能漏报）。

## 四、周度伏击名单接入（本次新增功能）

- 后端：`GET /api/altcoin/radar/pump-watchlist`（读 `data/research/pump_watchlist/latest.json`，
  带 8 天过期标记）；`POST .../refresh`（manage_data_sources 权限，子进程重生成，
  防并发锁）。测试：`tests/web/test_pump_watchlist_route.py`。
- 评分：`core/research/pump_precursor.py` 与研究面板共用同一特征实现
  （防训练/推理偏斜），权重工件 `model_weights.json` 由
  `scripts/pump_precursor_panel.py` 时间切分训练导出。
- 前端：雷达页新增「周度伏击名单（模型）」卡片（复用现有表格样式，
  `js/altcoin_radar.js` bump → v25），显示 Top15 + 驱动因子 + 新鲜度 +
  手动重生成按钮（60s 轮询直到后台完成）。
- 生成：`scripts/generate_pump_watchlist.py`，Coinglass 预算内节流（~15 分钟），
  建议每周一跑一次（可挂 Windows 计划任务或手动点按钮）。
