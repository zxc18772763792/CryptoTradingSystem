# 四个 Telegram 频道逐条整合回测（2026-10-31）

频道：Wise Advice Signal（wiseadvicebysumit）、The Rich Trader Reborn、Zetoshi X、Market Dealer。
数据窗口：2026-04-28 至 2026-10-30（四个频道的全部可见消息）。

本文只放汇总数字；消息原文、逐条标签、图片和逐笔结果都在本机私有目录
`~/.crypto_trading_system/research/kol4_integrated_20261030/`，不进仓库。

## 1. 怎么做的

**逐条读，不做关键词提取。** 四个频道共 6,039 条消息全部按时间顺序渲染成阅读稿，一条一条读，每条连同它回复的上一条一起判断，写成一行标签：
开仓 / 条件单 / 持仓声明 / 加仓 / 平仓 / 部分止盈 / 移保本 / 改目标 / 晒单 / 观点 / 新闻 / 推广。

- Wise 的新闻快讯（固定页脚格式）折叠成标题行后照样逐条过目。
- 只有图片里才有的信息去看了图：
  - Market Dealer 图上画的目标线（红色虚线）、入场区、仓位工具里的止损：42 张；
  - Zetoshi 只在截图里出现的币种和方向：8 张；
  - Wise 的持仓截图和区间图：4 张。
- Rich Trader 的配图抽查后确认只是币安 App 的价格截图（币种 + 现价），没有画位，所以不逐张读。

| 频道 | 消息数 | 逐条标签 | 其中可交易喊单 | 主要内容 |
|---|---:|---:|---:|---|
| Wise | 2,870 | 296 | 62 | 新闻快讯、prop firm 与交易所返佣推广；4–7 月有公开的 BTC/ETH/黄金短线单；7/29 起只发 VIP 群的结果截图 |
| Rich Trader | 743 | 206 | 122 | 币安价格截图 + “Long #X here”，部分附目标/止损，常在几小时后编辑补位 |
| Zetoshi | 1,800 | 230 | 86 | 推广和晒单为主；模板化的 “LONG – $X” 很多是事后补发；7 月起私密群才发币名 |
| Market Dealer | 626 | 310 | 146 | 带目标线的技术图、BTC 计划（突破/跌破触发）、“Buying some $X” 现货式买入 |

**规则先冻结，后看价格。** 交易清单和规则（`RULES`）写进 `freeze.json`（含哈希）之后，才第一次读价格。规则要点：

- 生效时间：发布 +60 秒；若编辑晚于发布 10 分钟以上，按最后编辑时间 +60 秒（偏保守）。
- 事后补发的“证明帖”（同一秒贴出 “TP hit”、几个月后再转发旧单）单列为 `repost` 组，不进主结果。
- 入场：
  - `m` = 下一根 K 线开盘市价；
  - 价位列表 = 等权限价单，72 小时有效，先碰 TP1 则撤单；
  - 数字突破/跌破条件 = 1 小时收盘越过该价后下一小时开盘入场（7 天内；发布时已越过则立即入场）；
  - 文字条件（“回调后再看”）不执行。
- 价位换算：
  - Rich 的 “Targets 50%-100%” 按所述杠杆的保证金收益换算，未写杠杆按 10x；
  - Wise 的 “$700 止损、1:1/1:2 止盈” 按止损距离倍数换算；
  - Market Dealer 用图上目标线；
  - 不替作者补止损。
- 持有上限 14 天（标为 scalp 的 24 小时）。成本：单边 10 bps；计资金费；1m/5m 官方 K 线，同一根 K 线内目标和止损都碰到时按不利方向处理（能取 1m 时先细化）。

**三种口径：**

| 口径 | 含义 |
|---|---|
| A 照原文 | 开仓帖（及 10 分钟内补的位）给出的入场、目标、止损 |
| B 跟作者 | A + 作者后续指令在发布 +60 秒执行（平仓、减半、移保本、改目标/止损） |
| C 只看方向 | 所有有币种和方向的非条件喊单，发布时市价进，24 小时和 7 天后平；不设止损 |

**对照组：** 同一个币、同方向、同一套相对价位，在喊单前 30 天和之后 30 天里每 12 小时起一笔（1 小时 K 线），超额 = 喊单 − 对照均值。显著性用按周分块自助法（4000 次），全表做 BH 校正。

## 2. 结果

（待填）

## 3. 复现

```bash
PY="F:/9_Crypto/.conda/miniforge3/envs/crypto_trading/python.exe"
"$PY" scripts/research_kol4_integrated.py collect          # 增量拉取（需要 reader.lock）
"$PY" scripts/research_kol4_integrated.py render           # 生成逐条阅读稿
"$PY" scripts/research_kol4_backtest.py build              # 标签 -> 交易清单
"$PY" scripts/research_kol4_backtest.py freeze             # 冻结规则和清单
"$PY" scripts/research_kol4_backtest.py simulate           # A/B/C 三种口径
"$PY" scripts/research_kol4_backtest.py nulls              # 同币对照
"$PY" scripts/research_kol4_backtest.py report
"$PY" scripts/research_kol4_backtest.py extras             # 杠杆、跨频道共识、作者晒单对照
```
