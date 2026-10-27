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

## 需要人决定的

- **已存的永续行怎么修。** 没改任何存储行。UTC 守护要求进来的数据覆盖并逐值等于已存行，永续行和现货归档对不上，所以常规写入方修不了，需要一个专门的显式修复（按天用现货归档整天替换、记录前后指纹），和 10/21 的时区修复同一类做法。
- **跨场所兜底。** `run_download_historical_data` 在 Binance 原生下载失败时，会把 Gate 的 K 线、Coinglass 的价格历史以 `binance` 写进库（提示文案里写明了“已使用 Gate/Coinglass 补齐”）。这是设计上的选择，没动；如果库要严格等于币安现货，应该去掉或改存别的命名空间。
- **策略在内存里拼接现货历史和永续最新几根。** 修复只管不写库；策略看到的序列尾部仍是永续价，属于信号口径问题。
- **还在跑旧代码的进程。** 网页服务 10/27 07:52 启动，没有这次修复，重启前数据页图表刷新仍会把永续价写进现货库。按要求没有重启。
