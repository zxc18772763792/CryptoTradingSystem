# Binance Alpha 新币起飞模型：可行性调研（2026-09-06）

动机：山寨雷达的验证过的周度模型只覆盖 130 个 Binance USDT 永续（有 OI/资金费/
≥35 天历史的中小盘），看不见最大的一类 10x 机会——**刚上的 Alpha/Gate 新币**。
本调研回答：现在能不能给 Alpha 新币建一个**能通过对抗验证**的起飞模型？

数据：`data/research/binance_alpha/`（后台采集器，388 活跃 / 668 目录，DB 346MB）。
复现：`scripts/alpha_feasibility_probe.py`。

## 结论：现在建不了可回测的模型，只能从今天起前向采集

### 1. 没有历史基本面 → 基本面横截面模型无法回测

会起作用的特征（持仓集中度、流动性、FDV、上市新鲜度、链、目录分）在 Alpha 数据里
**只有今天一天**：
- `market_snapshots` 与 `collector_runs`：全部是 2026-09-06（DB 无多日历史）。
- `token_snapshots.jsonl`：30 条全是今天；采集器把**整份目录**每几分钟 append 一次
  （每条 ~830KB），几天内就触发轮转（`BINANCE_ALPHA_HISTORY_MAX_MB=256`），
  所以留不下长历史,更没有适合建面板的**紧凑逐日基本面**。

要回测一个"用上市时的基本面预测起飞"的模型,必须有 point-in-time 的历史基本面——
它不存在。用今天的市值/持仓去"预测"过去的拉盘就是前视泄漏。

### 2. 唯一能回测的角度（纯价格 post-listing）是死的

价格历史是真的、干净的：1d klines ~300 bar/token（310 个 token ≥90 天），加上
`listingTime` 锚点。以此做**上市锚定、非重叠**的检验（predictor=前 3 天涨幅，
outcome=第 3→33 天前向最高涨幅）：

| 指标 | 值 |
|---|---|
| 可用 token（≥40 天上市后 1d 历史） | 316 |
| 上市后 30 天最高涨幅：中位 | +6%（多数新币基本不动） |
| 2x 比例 / 4x 比例 | 8% / 1% |
| 早期动量 → 起飞 spearman（去重叠后） | **0.122**（此前重叠计算的 0.64 是伪相关） |
| 高动量 vs 低动量组 2x 率 | 9% vs 10%（**无分离**） |

和此前所有价格 only 研究一样：中位近零、彩票尾部驱动、早期价格动量不预测后续起飞。

### 3. 幸存者信号可见

目录里 114/668 = **17% 已完全下架**（`fullyDelisted`）。新币死亡率真实且高,
将来的研究必须把下架币计入,否则严重高估。

## 落地：启动前向采集（唯一可行的路）

- **新增 `scripts/snapshot_alpha_fundamentals.py`**：每周从磁盘目录（不走网络,
  避开采集器的超时）抽一份**紧凑、逐 token、point-in-time 的基本面快照**
  （24 列：listing_age、mcap、fdv、circ/fdv、liquidity、holders、supply、
  volume、score、hot_tag、tge/airdrop、fully_delisted…）→
  `data/research/alpha_fundamentals/<date>.parquet`（只增不删,永不轮转）。
- 已挂进每周批处理 `run_pump_watchlist_weekly.bat`（与持仓集中度快照同一天跑）。
- 首份 2026-09-06 已落盘：668 token（554 活跃 / 114 下架）,捕到最新上市
  （TAC 2 天、CP 2 天、FLORK 3.8 天）。

**时间线**：累积 ~8-12 份周度快照后（约 2-3 个月）,才有 point-in-time 面板去跑
和永续模型同一套流程（发现特征 → 时间切分走查 → 伪重复/OOS/置换 对抗验证）。
在此之前,Alpha 起飞只能当观察,不能建模,更不能信。

**诚实定位**：这不是一个结果,是**开始积累让结果成为可能的数据**。而且大概率
和前九个策略一样,要杀掉几个假设才可能剩一个;新币样本噪声和幸存者偏差更大,
门槛只会更高,不会更低。

*研究性质,不构成投资建议。*
