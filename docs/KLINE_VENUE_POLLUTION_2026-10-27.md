# 现货 K 线库里混进 U 本位永续价格（2026-10-27）

## 现象

10/26 22:35:31 网页服务日志：`Saved 1000 incremental klines to 2 parquet parts under ...\binance\BTC_USDT\1m_parts`。这 1,000 根（10-25 21:56 到 10-26 14:35 UTC）是一位小数的价格（80261.3），同一分钟的现货是 80198.00 这类两位小数。`binance/` 是现货库（永续在 `binance_futures/`），这是场所污染。10/24 认证时发现的 AAVE/OP/RUNE/SUI 现货 15m 10/17–10/20 整天与永续归档逐根吻合，是同一类问题。

## 根因

`.env` 里 `BINANCE_DEFAULT_TYPE=future`（模拟/实盘交易的是 U 本位永续）。ccxt 4.5.78 的 `binance.market()` 对旧式 `defaultType=future` 有一段兼容逻辑：传入现货名 `BTC/USDT` 时，只要存在 `BTC/USDT:USDT`，就返回永续市场。于是共用的 Binance 连接器 `get_klines()`、`_client.fetch_trades()` 拿到的都是永续数据，而下列进程内写入方把它们以交易所名 `binance` 存进了现货库：

| 写入方 | 触发 | 写了什么 |
|---|---|---|
| `web/api/data.py` `get_klines` 的实时刷新（`_refresh_cache_from_live` / 阻塞刷新 / 无本地数据时） | 数据页默认图表 `BTC/USDT 1m limit=1200`，本地最后一根超过 3 根周期就在后台刷新 | 连接器 1,000 根（ccxt 现货/永续上限 1000）整段写回，覆盖现货行 |
| `core/ai/autonomous_agent.py` `_load_market_data_for_timeframe` | 代理扫描时本地数据不新 | `lookback+2`=242 根整段写回（现存日志从 10/13 起，10/13–10/18 约 3,300 次，26 个币的 5m/15m；10/19 起代理暂停） |
| `core/strategies/strategy_manager.py` `_load_market_data` | 策略每个周期 | 比本地新的几根；连接器失败时还会退到 `fapi.binance.com`（明确是永续）并同样写回 |
| `core/data/historical_data.py` `download_historical_klines`（网页“下载历史数据”、周/月线聚合） | 手动下载 | 用交易连接器拉，连接器不可用时的“公共”ccxt 客户端也读 `BINANCE_DEFAULT_TYPE`，同样是永续；现货没上市的币还会被映射到 `XXX/USDT:USDT` |
| `core/ai/research_loop_evaluation.py` `_refresh` | 研究循环补数（15m/1h/4h） | 同上的“公共”客户端 |
| `web/api/data.py` `_fetch_public_trades`、`core/data/second_level_backfill.py` | 1s/10s 图表、秒级回填 | 连接器的 `fetch_trades`（永续成交）聚合成秒级 K 线 |

批量维护脚本（`collect_binance_history.py`、`maintain_all_binance_market_data.py`、`maintain_top100_data.py`）明确走现货接口，不受影响。

**为什么 22:35 那次访问日志里没有 `/api/data/klines`：** 22:35:21–26 事件循环被 `get_available_data` 堵了约 9 秒，页面重连了 3 次；uvicorn 在客户端已断开时不写访问日志（`RequestResponseCycle.send` 一开头就 `if self.disconnected: return`），但处理函数照常跑完并排好了后台刷新。10/18、10/24 的几次 1,000 根写入前面都有 `GET /api/data/klines?exchange=binance&symbol=BTC%2FUSDT&timeframe=1m&limit=1200`。

## 修复（不改 `BINANCE_DEFAULT_TYPE`，交易照旧用永续）

- 新模块 `core/data/kline_venue.py`：`connector_market_type`（连接器 `config.default_type` / `default_type`）、`venue_matches_store`（`*_futures` 库存永续，其余库存现货）、`require_store_venue`。
- `DataStorage.save_klines_to_parquet(..., market_type="spot")`：不匹配就抛 `KlineVenueError`，一行不写；网页 `_save_df_to_parquet` 同样先查。
- 网页图表刷新：连接器不是现货时，Binance 改用现货 REST（`api.binance.com/api/v3/klines`，原来阻塞刷新就在用），其他交易所只用本地数据；`_fetch_public_trades` 对非现货连接器返回空。
- 代理、策略管理器：永续 K 线只在内存里给决策用，不再写库（`fapi` 兜底同样不写）。
- 历史下载、研究循环补数：交易连接器不是现货时改用公共 ccxt 客户端；公共客户端固定为现货，现货没上市的币不再映射到合约（ccxt 会报“没有这个市场”，下载按不可重试错误结束）。
- 秒级回填：连接器不是现货时任务直接失败并写明原因，不再拿永续成交聚合。

测试：`tests/test_kline_venue.py`（14 个），`tests/test_strategy_manager_runtime_data.py` 里原来断言“`fapi` 兜底要写库”的用例改成断言不写。

## 污染范围（只读扫描）

`scripts/scan_kline_venue_pollution.py`（只读，结果在 `data/research/kline_venue_pollution_20261027/`）：

1. 粗筛：每根 K 线在整点边界必须和 1h 线一致（HH:00 开始的那根的开盘价 = 该小时开盘价，HH+1:00 结束的那根的收盘价 = 该小时收盘价；1h/4h/1d 两头都查）。现货 1h 参照 = 本地 `binance/<币>/1h.parquet` + 官方现货归档（主文件够不着的月份，直连 data.binance.vision，缓存在 `spot_1h_archive_cache/`）；永续 1h 参照 = `binance_futures/<币>/1h.parquet`（1000 倍合约按倍数折回）。等于永续、不等于现货算一次命中。
2. 复核：每个命中的“序列 × 日”下载该周期的现货和 U 本位**日归档**，逐行比对开高低收；只统计“等于永续、不等于现货”的行（流动性差的币有不少平盘根两边完全一样，不算）。

范围：518 个序列（136 个币的 1s/10s/1m/5m/15m/1h/4h/1d 分区，1.06 亿行）。

| | 序列 | 序列×日 | 永续行 |
|---|---:|---:|---:|
| 5m | 78 | 957 | 194,176 |
| 15m | 35 | 1,329 | 118,239 |
| 1m | 7 | 91 | 26,495 |
| 1h | 11 | 71 | 1,464 |
| 4h / 1d / 1s / 10s | 0 | 0 | 0 |
| **合计** | **131（81 个币）** | **2,448** | **340,374** |

- 时间上集中在 2026-07（9.1 万行）、09（13.4 万）、10（11.4 万）；1–6 月合计约 1,300 行。其中 1,741 个“序列×日”整天 ≥90% 是永续。
- 存放位置：`.legacy` 包 29.1 万行（10/21 之前写的无标记日子）、近期按天文件 4.2 万、月/年文件 0.7 万。
- 读取函数确实返回这些行（抽查：BTC 15m 09-10 读出 96 根全等于永续归档；BTC 1m 10-23 1,440 根全是永续；AAVE 5m 10-01 288 根中 216 根）。
- 典型：BTC/ETH/SOL/BNB/XRP/LINK 等 25 个主流币的 15m 从 09-03（部分 7 月）到 10-23 几乎整段是永续（每个约 4,600 行）；5m 有 71 个序列各超过 1,000 行，22 个超过 3,000 行（7/13–10/18）；1h 有 7 个币在 8/30–9/28 各约 190 行；BTC 1m 7/07–10/25 有 26 天、约 1.6 万行（每次 1,000 根，对应数据页图表刷新）；MANTA/MEME 1m 7/13–7/16 各约 5,200 行。
- 10-26 当天的 USD-M 日归档还没发布：BTC 1m 10-26 只有 613/1,440 根等于现货（22:35 那批），没法逐行确认，另 5 个 10-26 命中整天等于现货（巧合）。
- 每日明细 `confirmed_perp_days.csv`（含文件名、首末永续时间），按序列汇总 `confirmed_perp_series.csv`，粗筛原始记录 `polluted_days_*.jsonl`。

**影响：** 7 月以来用本地 5m/15m/1m 现货 K 线做的研究、回测、因子（尤其 15m 主流币 9/3–10/23）实际混用了永续价。永续和现货价差通常只有万分之几，但成交量完全不同（永续量大得多），涉及成交量、基差、逐根收益的结论都可能受影响。

## 存量修复（10/27 已执行）

工具：`core/data/kline_venue_repair.py` + `scripts/repair_kline_venue_pollution.py`，测试 `tests/test_kline_venue_repair.py`。写入沿用 10/21 时区修复的引擎 `core/data/parquet_timezone_repair.apply_plan`（拿目录锁和文件锁、锁内复核 sha256、先备份、写临时文件读回比对、再替换、记 manifest；`rollback_run` 按备份还原）。

```bash
python scripts/repair_kline_venue_pollution.py plan                       # 试运行，只写计划目录
python scripts/repair_kline_venue_pollution.py apply --plan <计划目录>      # 预检：现在执行会写什么（不写）
python scripts/repair_kline_venue_pollution.py apply --plan <计划目录> --apply
python scripts/repair_kline_venue_pollution.py verify --plan <计划目录>     # 读者视图对计划的“修复后”指纹
python scripts/repair_kline_venue_pollution.py rollback --run <计划目录>/run_<时间>
```

**方法。** 候选是扫描 `--confirm` 确认的“序列×日”（加上当时 USD-M 归档未发布的 10-26 几天）。对每个候选日，序列里**每个**含这一天的文件（主文件 `<周期>.parquet`、月/年文件、`.legacy` 包、按天文件）逐行和当天的现货、U 本位日归档比对（1000 倍合约按连接器的做法：价格除以倍数、成交量不变）：

| 类 | 判定 | 处理 |
|---|---|---|
| spot | OHLCV 等于现货归档 | 不动 |
| perp | 开高低收等于永续归档、不等于现货 | **换成现货归档那一根** |
| perp_volume | 价格两边相同，成交量等于永续、不等于现货 | **换** |
| perp_forming | 开盘等于永续、区间在永续那根之内（像没收完的永续 K 线） | 只列出：不是逐值证据 |
| spot_forming / neither / no_spot_bar | | 只列出 |

- 只换值：不增删行、不挪文件，列、类型、索引、属性原样（`.legacy` 包仍无标记，带标记的月/年文件仍带标记）。
- **时钟闸门**：某个文件的某一天如果超过 25% 的行两边都对不上（`MAX_NEITHER_SHARE`），这一天整个不动（`blocked`），再拿前后 8 小时的归档诊断它是什么。原因：1–5 月本地时钟时代的整天 K 线错位 8 小时，偶尔有一两根在存储时刻碰巧等于永续，换掉会把 UTC 的现货 K 线插进错位的一天。本次每个日子不是 ≤20% 就是 ≥50%，25% 落在空档里。
- 执行时按行把关：每一行此刻必须还是计划里的“修复前”值（或已经是“修复后”值，跳过）；有一行变了，这个序列整个不动（stale）。计划外的行（例如每小时刷新新追加的）按执行时的样子保留。

**计划** `data/research/kline_venue_repair_20261027/plan_20261027T121401Z/`（`summary.json` 里有 `changes.parquet` 的 sha256，执行前会核对）：

| | 序列 | 要换的行 |
|---|---:|---:|
| 5m | 78 | 194,243 |
| 15m | 35 | 118,981 |
| 1m | 3 | 29,018 |
| 1h | 11 | 1,464 |
| **合计** | **127（81 个币）** | **343,706**（perp 343,603 + perp_volume 103） |

- 2,392 个“序列×日”有改动；读了 659 个文件、要改写 573 个：`.legacy` 包 205（29.1 万行）、按天文件 343（4.2 万）、月文件 21（0.8 万）、**主文件 4**（BTC 1m 1,847 行，JTO/ORDI/RENDER 15m 共 709 行）。扫描只看了分区，主文件里也有永续行，见下面“未覆盖”。
- 按月：7 月 9.2 万、9 月 13.5 万、10 月 11.5 万，5–6 月约 1,900。BTC 1m 10-26 归档现在已发布，确认 827 根是永续（全部要换）。
- 价差：收盘价相差中位数 0.06%、99 分位 0.34%、最大 3.9%；永续成交量中位数是现货的 6 倍。

**指纹**（试运行就能核对，执行后用 `verify` 再核）：

- `files.csv`：每个文件现在的 sha256、**执行后会有的 sha256**（计划时用执行时同一种写法把结果序列化出来算的；文件在执行前没被别人改过的话，执行后逐字节相同）、修复前后的 frame 哈希、各类行数。
- `days.csv`：每个“序列×日”读取函数（主文件+分区）看到的行数和 SHA-1，修复前一份、修复后一份（把计划好的文件内容替进读取函数算的），以及修复后是否逐根等于现货归档。例：BTC 15m 09-10 读出 96 根，`7416deef…` → `34f42a6d…`，修复后等于现货归档，永续 96 → 0；BTC 1m 10-26 1,440 根，永续 827 → 0；AAVE 5m 10-01 永续 216 → 0。
- 合计：读取函数看到的可替换行 339,982 → 228（剩下的全在被闸门挡住的日子里）；2,453 个日子里 2,350 个修复后逐根等于现货归档。
- 独立复核（另写脚本对 `changes.parquet`）：每个“修复后”都是现货归档那一根；每个 perp 行修复前逐值等于永续归档且不等于现货；没有重复行、没有空改。预检 `apply`（不写）：127 个序列全部可写，没有 stale。

**不动、只列出的**（`unresolved.parquet`、`blocked_days.csv`）：

- 被闸门挡住的 89 个“文件×日”（`blocked_days.csv`）：DOT 1m 1/16–4/15 的 26 天整天是**永续且错 8 小时**（51 个文件×日：主文件和包各一份）；DOT/FIL/ICP/ATOM 1m 4/15–5/18 的 32 天是现货错 8 小时（不是场所问题）；JTO/ORDI/RENDER 15m 主文件 5/26–6/8 的 5 天里 13–46% 的行在存储时刻逐值等于永续，其余前后 8 小时都对不上（来源不明），里面 140 根确定的永续行这次没换。这些属于时区修复工具的范围。
- perp_forming 353 根：TIA/MANTA/MEME 5m 07-16 各约 95 根（开盘等于永续，但成交量只有永续的约一成、收盘不同，来源不明），其余是零星的批末未收完 K 线。
- neither 210 根：BTC 1m 07-07 的 188 根一位小数价格，币安现货和永续在 ±8 小时都对不上（像别的交易所的 BTC，本地 Gate 库没有这天）；MEME/MANTA 等零星几根。
- spot_forming 30 根：10-20 各主流币 15m 最后一根等，是现货但没收完。

**执行（10/27 12:38 UTC，用户审过计划后）：** `apply --plan plan_20261027T121401Z --apply`，记录在 `plan_20261027T121401Z/run_20261027T123809Z/`（`results.jsonl`；每个序列一个子目录，含 `manifest.json` 和 `backups/`）。

- 127 个序列全部 `complete`，没有 stale、没有重试；573 个文件、343,706 行，`rows_already_repaired` 为 0。
- 573 个文件在计划和执行之间都没被别人改过，执行后的 sha256 **全部**等于计划里算好的值（逐字节一致）。
- 备份 573 个（29 MB）；目录里没有残留的临时文件，月/年文件和包旁边没有多出锁文件。
- `verify`（`verify_20261027T124008Z.csv`）：2,453 个日子的读取函数指纹**全部**等于计划的“修复后”指纹；修复过的日子里可替换行 0；被闸门挡住的日子里仍是预计的 228 行。
- 回滚：`python scripts/repair_kline_venue_pollution.py rollback --run data/research/kline_venue_repair_20261027/plan_20261027T121401Z/run_20261027T123809Z`（只在文件仍是修复写入的样子时还原，之后被别的写入改过的文件会拒绝）。

**未覆盖：** 候选日来自扫描，而扫描只看了 `_parts`。修复会顺带改到候选日里主文件的永续行（上面 4 个主文件），但只有主文件被污染的日子不在候选里，例如合并文档记下的 BTC 15m 主文件 4/18–4/21。要找全，需要把扫描扩到主文件再重新计划。

## 需要人决定的

- **修复之后新写进去的永续行。** 修复执行时网页服务还没重启，仍会往 BTC 1m 最近几天写永续价；10-27 及以后的行要等官方日归档发布后重新扫描（`scan_kline_venue_pollution.py --confirm`）、重新计划。
- **被闸门挡住的 89 个文件×日**（错 8 小时的整天，DOT 1m 1–4 月还是永续）和 **JTO/ORDI/RENDER 15m 主文件的 140 根永续行**：交给时区修复，或单独决定。
- **主文件扫描**：只有主文件被污染的日子还没找（例如 BTC 15m 主文件 4/18–4/21）。
- **跨场所兜底。** `run_download_historical_data` 在 Binance 原生下载失败时，会把 Gate 的 K 线、Coinglass 的价格历史以 `binance` 写进库（提示文案里写明了“已使用 Gate/Coinglass 补齐”）。这是设计上的选择，没动；如果库要严格等于币安现货，应该去掉或改存别的命名空间。
- **策略在内存里拼接现货历史和永续最新几根。** 修复只管不写库；策略看到的序列尾部仍是永续价，属于信号口径问题。
- **还在跑旧代码的进程。** 网页服务 10/27 07:52 启动，没有这次修复，重启前数据页图表刷新仍会把永续价写进现货库。按要求没有重启。
