# @alastartrader1（冒充号）· 按原文执行纸面账本（alastartrader1_as_written_v1）

2026-10-30 按用户要求，把 @alastartrader1 从 Alastar 收件箱里拆出来，单独做成一本账。只记录，不下单。

## 为什么单独记

@alastartrader1 不是 Alastar 本人。10/22 复核时就已认定：官方简介的私信是 @alastartrader（没有 1），这个号通过 @Albanwatts 卖 VIP/PREMIUM 订阅和“存 500 赚 6,800”的回本计划。10/30 它的 BEAMX 50 倍做多（#2064）和“TP3 达成 +201%”（#2066）在同一秒发出，发帖时价格 0.00463，已经高于入场价 0.004040 和全部四个止盈。这本账回答一个问题：如果有人从首次收件起照它的喊单原文去做，会得到什么。

## 规则

| 项目 | 规则 |
|---|---|
| 收件 | 新闻采集器存档的公开页 t.me/s/alastartrader1，首次收件距发帖不超过 10 分钟；采集器标记的迟到、截断、纯图片都排除，只放过“可见编辑” |
| 识别 | 前三行恰好有 LONG 或 SHORT 之一，有币名和 TP 行；用冻结的括号单规则 `kol_bracket_engine` 读入场、止盈、止损、杠杆。“TP3 HIT”之类的战报和不带方向的分析不算喊单 |
| 时钟 | 收件前后 10 分钟内有一次合格的币安对时（偏差 ≤2 秒），每 5 分钟刷新 |
| 执行 | 直接调用 HUB 账本的 `simulate`：冻结后下一根 1 分钟开盘挂单；已经穿过市价的挂单按开盘成交；止盈 1 或止损先到就撤单；72 小时未成交作废，最长 14 天；各止盈档平均分仓，原文止损，不加杠杆。原文杠杆的逐仓爆仓结果另记 |
| 成本 | 往返 20bps；资金费按实际结算另列，覆盖不全记为缺失 |
| 同币 | 一个币同时只算一笔 |
| 对照 | 入场 45 天后，同币同几何在入场后 30 天每 12 小时起点（58 个）的 1h 均值 |
| 样本 | 60 笔成交时复核，最长 12 个月；不设无效性停账，没有通过条件，只作观察 |

## 文件

- 协议：[config/research_protocols/alastartrader1_as_written_v1.json](../config/research_protocols/alastartrader1_as_written_v1.json)
- 账本：[core/research/alastartrader1_ledger.py](../core/research/alastartrader1_ledger.py)，命令行 `scripts/alastartrader1_ledger.py activate|cycle|status|verify`
- 日志钉住四个文件的哈希：本模块、协议、`kol_bracket_engine.py`、`hub_runup_ledger.py`。改任何一个都会停账。前两个在 `.gitattributes` 里标成 `-text`，后两个本来就是 HUB 的锁定文件
- 数据：`data/research/alastartrader1_as_written_v1/`；网页任务 `alastartrader1_ledger` 每 30 秒一轮，心跳在 `runtime/alastartrader1_ledger.json`
- 研究页“Telegram信号”分组有一行。Alastar 收件箱仍保留它的帖子，带红色“冒充号”标签，不计入 Alastar

## 激活

2026-10-30 13:26:48 UTC，基线消息 2069（之后的新帖才计入）。钉住的哈希：协议 `b92b0638…`，账本 `b03029ad…`，规则 `38e1ccd7…`，执行 `4cdf2298…`（后两个与 HUB 账本相同）。

激活前用当天两条真实喊单回放：BEAMX 发帖时已越过止盈 1，记为作废、不成交；ON 开盘买入后打到止损，−8.1%；按原文 30 倍杠杆会先爆仓。
