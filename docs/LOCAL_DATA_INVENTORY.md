# 本地数据总览（2026-10-13）

研究用的历史数据都在 `crypto_trading_system/data/` 下，这个目录不入库。本文说明：

- 有哪些数据；
- 放在哪、覆盖到哪；
- 怎么读；
- 怎么保持更新。

10/13 用户要求"尽可能全地把历史上全币种各方面的数据收集到本地"，下表的大部分内容是当天补齐的。

## 一、总表

| 数据 | 位置 | 覆盖 | 更新 |
|---|---|---|---|
| 币安永续 K 线（1h / 4h / 1d） | `data/historical/binance_futures/<币>_USDT/<周期>.parquet` | 654 个 USDT 永续（523 个在交易，131 个已下架），自 2020-01 或上市起；已下架的截到下架日 | 每日定时任务补尾部 |
| 币安现货 K 线（1h / 4h / 1d） | `data/historical/binance/<币>_USDT/<周期>.parquet` | 691 个 USDT 交易对（496 个在交易，其余已下架），自 2020-01 或上市起；不含稳定币、法币、杠杆代币 | 每日定时任务补尾部 |
| 币安现货分钟和秒级 K 线 | 同上（`1m`、`5m`、`15m`、`10s` 等） | 30 个默认研究币，约最近 90 天 | 每小时定时任务（ResearchUniverseRefresh） |
| 币安资金费率 | `data/historical/binance_futures/<币>_USDT/funding_rate.parquet` | 每个永续自 2020-01 或上市起的每一次结算（费率和标记价格） | 每日定时任务 |
| 币安溢价指数（日线） | `data/historical/binance_futures/<币>_USDT/premium_index_1d.parquet` | 永续相对指数价格的溢价（开高低收），自 2020-01 或上市起 | 每日定时任务 |
| Coinglass 每币衍生品日线（12 类） | `data/research/coinglass_history/raw/<交易对>.json.gz`；总表 `panels/<数据>.parquet` | 654 个币安永续对应的币，自 2020 年起（深度自 2024-10，Hyperliquid 自 2025-07）；见第二节 | 每周定时任务 |
| Coinglass 市场整体指标（约 70 项） | `data/research/coinglass_macro/<名称>.json` | 恐慌贪婪、稳定币市值、山寨季、比特币市占率、期权与期货持仓比、Coinbase 溢价、比特币周期指标 24 项、ETF 资金流、期权、交易所余额、借贷利率、大户指数等 | 每周定时任务 |
| Coinglass 全币快照 | `data/research/coinglass_snapshots/<日期>/<时>_coins_markets.json.gz` | 约 1,500 个币，每天 4 份（00/06/12/18 UTC），自 10/11 起；只有当时的值，补不了历史 | 研究调度器 |
| Coinglass 爆仓地图（7 天） | `data/research/coinglass_snapshots/<日期>/liq_map_7d.json.gz` | 131 个研究币，每天 1 份，自 10/11 起 | 研究调度器 |
| Coinglass 日线数据库（跟踪器用） | `data/research/coinglass_daily/` | 234 个币 × 6 类，滚动保留 60 天 | 研究调度器，每天 00:15 UTC 起 |
| 币安净多空持仓存档 | `data/research/coinglass_net_position/` | 234 个币，自 2024-01 起，只追加 | 研究调度器，每天 08:00 UTC 起 |
| 研究下载（旧，保留给复现） | `data/research/coinglass_positioning/raw`、`data/research/coinglass_wide/raw`、`raw_new` | 10/11、10/12 研究用的 131 + 103 个币，自 2024-01 起 | 不再更新 |
| 新闻 | `data/news.db` | 新闻采集器全部历史 | 新闻进程 |
| 交易所公告 | `data/research/binance_announcements/`、`data/research/upbit/`、Bithumb 标记在跟踪器状态里 | 币安、Upbit、Bithumb 公告 | 研究调度器 |
| 代币解锁 | `data/research/unlock_study/` | DefiLlama 解锁排期与供给曲线 | 解锁跟踪器 |
| 币安 Alpha | `data/research/binance_alpha/alpha_market.db` | Alpha 采集器，K 线保留 14 天 | Alpha 采集器 |

## 二、Coinglass 每币日线：12 类数据

| 名称 | 内容 | 口径 | 最早 |
|---|---|---|---|
| `oi` | 合计持仓量开高低收（美元） | 全部交易所 | 2020 |
| `funding` | 按持仓加权的资金费率开高低收 | 全部交易所 | 2020 |
| `funding_vol` | 按成交加权的资金费率开高低收 | 全部交易所 | 2023-04 |
| `liquidation` | 多头、空头爆仓额（美元） | 币安 + OKX + Bybit | 2020 |
| `taker` | 合约主动买入、卖出额（美元） | 币安 + OKX + Bybit | 2020 |
| `spot_taker` | 现货主动买入、卖出额（美元） | 币安 + OKX + Bybit | 2020 |
| `global_ratio` | 全部账户多空比（多、空占比） | 币安交易对 | 2020-10 |
| `top_account` | 大户账户多空比 | 币安交易对 | 2020-05 |
| `top_position` | 大户持仓多空比 | 币安交易对 | 2020 |
| `net_position` | 净多头变化、净空头变化（币数量） | 币安交易对 | 2020 |
| `depth_4h` | 价格上下 1% 以内的买卖挂单（美元和数量），4 小时线 | 币安 + OKX + Bybit | 2024-10-21 |
| `hl_ratio` | Hyperliquid 账户多空比 | Hyperliquid | 2025-07-02 |

读之前要知道的几点：

- **时间戳：** 一根日线的时间戳是当天 00:00（UTC）。成交、爆仓这类流量描述当天全天，第 D 天的值在 D+1 的 00:00 才能用。
  币安多空比的日线是时间戳那一刻（当天 00:00）的快照，所以描述第 D 天结束时状态的是时间戳为 D+1 的那根（`docs/COINGLASS_DATA_CATALOG.md` 第五节）。
- **上市前是 0：** 爆仓、主动买卖从 2020-01-01 起每天都有一行，币上市之前的值是 0，不是缺失。
- **1000 倍合约：** 币安的 1000PEPE 在 Coinglass 里查的是 PEPE。合计类数据只包含名字叫 PEPE 的合约（例如 OKX 的），不含币安、Bybit 的 1000PEPE。币安交易对类的数据不受影响。
- **已下架的币也在里面：** 131 个已下架的永续，数据截至下架日前后，研究时可以避免只看活下来的币。

读法：

```python
import pandas as pd
oi = pd.read_parquet("data/research/coinglass_history/panels/oi.parquet")    # perp, coin, time, open, high, low, close
wide = oi.pivot(index="time", columns="perp", values="close")                # 日 × 币
```

## 三、币安 K 线、资金费率、溢价指数

- **K 线**
  - 每个币一个目录，每个周期一个 parquet，索引是开盘时间（UTC，无时区），列是 open / high / low / close / volume。
  - 少数币另有按天分区的 `<周期>_parts/` 目录，由每小时任务写入，读的时候要合并。
- **读取方式**
  - 项目的读取函数是 `core.data.data_storage.load_klines_from_parquet(exchange="binance_futures", symbol="BTC/USDT", timeframe="1h")`，现货把 exchange 换成 `binance`。
  - 它遇到损坏的文件会把文件挪进隔离区，只读分析时可以直接用 pandas / pyarrow 读。
- **已下架的永续**
  - 币安在下架后还会继续返回成交量为 0、价格不变的假 K 线。这里截到交易所信息里的下架日（deliveryDate），没有收这部分。
- **剩下的缺口都是币安本身的**
  - 现货 1 小时线全部交易对加起来约 2 万根，集中在 2020-11-30、2020-12-21、2021-02-11、2021-03-06、2021-04-20、2021-08-13、2021-09-29、2023-03-24 等维护或宕机日，币安自己就没有这些 K 线。
  - 永续 KNC 1 小时线丢掉了 2023-11-10 的 1 根（最高价低于开盘价，数据本身不一致）。
  - USUAL 现货 2024-12-17/18 停牌。
- **资金费率**
  - `funding_rate.parquet`，索引是结算时间（UTC），列是 funding_rate、mark_price。
  - 币安的结算间隔因币而异，有 8 小时、4 小时、1 小时几种。
- **溢价指数**
  - `premium_index_1d.parquet`：永续相对现货指数的溢价，日线开高低收，可以拿来看期现基差。

## 四、市场整体指标（`data/research/coinglass_macro/`）

- 每项一个 JSON，字段 `path`、`params`、`fetched_at`、`data`。`data` 是中转站原样返回的内容：
  - 多数是按天的列表；
  - 恐慌贪婪、稳定币市值、期权持仓这几项是"时间列表 + 数值列表"的字典。
- 汇总见 `_summary.json`。
- 10/13 拿到 68/71 项。缺的 3 项中转站返回 500，上游不提供：灰度 ETH 溢价、交易所 USDT 余额（图表和列表）。

## 五、怎么保持更新

| 定时任务 | 时间 | 做什么 | 日志 |
|---|---|---|---|
| `CryptoTradingSystem_WideKlineTopUp` | 每天 09:20（北京时间） | `scripts/collect_binance_history.py --tail-only`：在交易的永续和现货补尾部 K 线、资金费率、溢价指数（约 35 分钟） | `logs/wide_kline_topup.log` |
| `CryptoTradingSystem_WeeklyCoinglassTopUp` | 每周六 14:00（北京时间） | `scripts/collect_coinglass_history.py --update`（约 5,800 次中转站请求、一天左右）→ 市场指标 → 重建总表 | `logs/weekly_coinglass_topup.log` |
| `CryptoTradingSystem_ResearchUniverseRefresh` | 每小时 | 30 个默认研究币的现货分钟、小时数据 | `logs/research_universe_refresh.log` |

手动复查和补齐：

```bash
python scripts/collect_binance_history.py --audit        # 币安：逐根核对缺口（只读，约 30 分钟）
python scripts/collect_binance_history.py                # 币安：补齐所有缺口
python scripts/collect_binance_history.py --funding-only # 只补资金费率（例如被币安限流之后）
python scripts/collect_coinglass_history.py              # Coinglass：补齐缺失或失败的数据（可续传）
python scripts/collect_coinglass_history.py --panels     # 重建 Coinglass 总表
python scripts/collect_coinglass_macro.py                # 市场指标
```

注意：

- **代理流量：** 币安走代理，按流量计费。首次补齐经代理传输约 1 GB；之后每天补尾部只有十几 MB。
- **Coinglass：** 走中转站直连，不耗代理流量。限额是滚动 60 秒 11 次，和后台采集、存档任务共用；批量任务每 12 秒一次，并避开 00:10–05:55（UTC）日线数据库更新的时段。
- **币安限流：** 资金费率接口和服务里的结算共用每 5 分钟 500 次的额度。10/13 第一次以每秒 1.2 次批量拉取时，546 个永续被币安防火墙临时封（403）。现在改为每秒 0.5 次，被封时先等 2 分钟以上。

## 六、相关文档

- `docs/COINGLASS_DATA_CATALOG.md`：Coinglass 全部接口的实测结果和已知的坑。
- `docs/LOCAL_KLINE_STORE_AUDIT_2026-10-12.md`：10/12 对 234 个币的体检与补齐。
- `docs/RESEARCH_TRACKERS_2026-10-13.md`：跟踪器一览（`scripts/tracker_overview.py` 可重新生成）。
