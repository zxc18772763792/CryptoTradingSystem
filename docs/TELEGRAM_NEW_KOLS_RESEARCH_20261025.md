# 10/25 新关注的四个 Telegram KOL 频道：喊单解析与事件研究

完成：2026-10-25 北京时间约 20:20。脚本 [scripts/research_telegram_new_kols.py](../scripts/research_telegram_new_kols.py)，测试 [tests/test_research_telegram_new_kols.py](../tests/test_research_telegram_new_kols.py)。

**结论：四个频道都没有统计上站得住的跟单优势，不建议跟单或接入新闻/交易流程。** 两个样本够大的频道（next10xgem1、The Rich Trader Reborn）在预先声明的 24 小时主周期上，相对“同币随机时点”零分布的超额收益**显著为负**；喊单普遍出现在币价已经大涨之后。另两个频道样本不足（Market Dealer 23 笔、Wise Advice 6 笔），给不出结论。

| 频道 | 180 天存活帖 | 可交易的明确新喊单（独立笔数，24h） | 喊单形式 | 24h 均值净收益 / 超额 vs 零分布（周聚类 95% 区间） | 判定 |
|---|---:|---:|---|---|---|
| Market Dealer（私有频道，无用户名） | 607 | 23 | “❗️Buying some $X here” 现货买入 + BTC/ETH 观点 | −1.31% / −1.51 个百分点 [−4.12, +0.66] | 样本不足 |
| Binance Spot & FutuRes Signals（@next10xgem1） | 1,815 | 325 | “XXXUSDT buy and hold big Move soon”、少量合约 LONG/TP/SL，全部做多 | −1.52% / **−1.78** [−3.33, −0.26] | 无优势（显著为负） |
| Wise Advice Signal（@wiseadvicebysumit） | 2,802 | 6 | 以新闻、推广为主；少量 BTC/ETH/黄金短线 | −0.64% / −0.41 | 样本不足 |
| The Rich Trader Reborn（@Therichtrader_Reborn） | 738 | 65 | “Long #X”、结构化 LONG SIGNAL（入场/目标/止损） | −1.99% / **−1.89** [−2.99, −0.88] | 无优势（显著为负） |

净收益已扣 20bps 往返成本和实际资金费；空头按 (entry−exit)/entry 计；收益是每笔名义本金收益，不是账户收益。

## 1. 新频道核实

用已有的 DPAPI 加密 Telethon 会话只读列出账号对话（持 `reader.lock`，不加群、不发消息、不下载媒体）。已加入频道的 `Channel.date` 就是本账号的加入时间。对照已有研究：

- 10/25 早些时候另一会话已研究过三个来源（HUB、Zetoshi X、Karsa），其加入来源快照时间为 05:27 UTC；Zayk Charts 10/24 14:13 UTC 加入，也已研究（`data/research/zayk_backtest_20261025/`、`data/research/multi_channel_backtest_*` 指向的受限目录）。
- 05:27 UTC 之后加入的只有这四个，加入时间集中在 **11:45–11:48 UTC**（北京时间 19:45–19:48）：Market Dealer 11:45:21、@next10xgem1 11:45:58、@wiseadvicebysumit 11:46:25、@Therichtrader_Reborn 11:48:24。

因此“新关注的 KOL”可靠确定为这四个，没有猜测。无关对话没有写入任何文件。

## 2. 数据与覆盖

只读回溯快照：2026-04-28 05:00 至 2026-10-25 12:00 UTC（180 天，与 10/25 三来源延长研究同一左边界）。四个频道都读到了更早的边界，均未触及 8,000 条上限。这是当前账号可见的**存活**历史，不是前向记录；删帖和编辑前原文都拿不到。

| 频道 | 订阅数 | 帖子 | 编辑过 | 消息 ID 缺口（被删上限） | 喊单中转发占比 | 频道自述/特征 |
|---|---:|---:|---:|---:|---:|---|
| Market Dealer | 68,755 | 607 | 98%（中位 38 秒，配图改文字） | **296 / 903（33%）** | 0% | “最赚钱的免费社区”；提到付费 Premium 频道 |
| next10xgem1 | 324,786 | 1,815 | 5% | **1,131 / 2,946（38%）** | 97%（转发延迟中位 5 秒） | “100% HIGH QUALITY SIGNALS. PROFIT IS GUARANTEED.” |
| wiseadvicebysumit | 165,529 | 2,802 | 91% | 106 / 2,908（4%） | 38% | 新闻 1,653 条、推广 214 条（prop firm 推荐码、Yubit 跟单邀请、免费 VIP 机器人、币安投票、“AI 机器人 2100% 收益”） |
| Therichtrader_Reborn | 24,980 | 738 | 38%（211 条超过 10 分钟后才改） | 6 / 744（1%） | 33% | “只分享自己的交易”；也给其他频道打广告 |

频道消息 ID 连续，缺口就是被删的消息（服务消息也会被读到）。删掉的可能是到期广告，也可能是失败喊单，无法区分；但 Market Dealer 和 next10xgem1 的删帖比例很高，下面的结果对它们是**偏乐观**的。

## 3. 方法（先冻结，再读价格）

协议 `PROTOCOL.md` 和逐帖标签 `events_before_prices.json` 在读取任何行情前写入受限目录并记录 SHA-256（标签 `f1d969de…`，协议 `1388957d…`）。

- **标签**：确定性正则，只看帖子自己的文字，不用模型、不看价格。分为结构化计划（trade_plan）、明确喊单（direct_call）、观点（bias_view）、持仓跟进（followup_view）、战绩/推广、新闻等。草稿阶段的盲审纠正了：`TAKE PROFIT:` 被当成战绩、新闻页脚“JOIN FREE VIP”把所有帖子都判成推广、清算机器人提醒和“Strategy 在卖 BTC”新闻被当成做空、鲸鱼开仓新闻被当成喊单、“holding Bitcoin LONG”被当成新喊单、“KERNEL / 2Z / NOM Usdt buy and hold”这类无 `$`/`#` 的币名漏识别、价格位与币名矛盾（BTC 价位旁边顺带一个 #SOL）、$TOTAL 指数评论被映射成 BTC。这些都在封存前改完；封存后标签可逐条复现。
- **主样本**：每个频道的无条件明确喊单（trade_plan + direct_call），未被“晚改”（发布 10 分钟后才编辑），且本地有币安数据。有条件的喊单（if/in case/wait for…）、观点、跟进、晚改的喊单各自单列为探索性样本。
- **时间**：可用时间 = 编辑时间（如有编辑）否则发布时间；入场 = 可用时间 + 60 秒之后的下一个整点开盘价。发布时间 ≠ 可交易时间，不回填。
- **价格**：优先本地 `data/historical/binance_futures/` 1h K 线 + 资金费；只在现货有、合约没有的币上用现货 1h（只做多）。本地数据截至 2026-10-25 00:00 UTC，最后几天的长周期样本自然未成熟。
- **收益**：持有 1/4/24/72/168 小时，**24 小时为预先声明的主周期**。扣 20bps 往返 + 实际资金费，另报 80bps 压力成本；空头 (entry−exit)/entry，1 倍保证金下限。同一频道同一币有持仓时，新喊单不叠加。
- **零分布**：同币、同方向、同周期，在 ±30 天内随机整点开盘入场（排除与该喊单持有期重叠的起点），置换检验 5,000 次；另做“只用入场前 30 天”的敏感性。BH-FDR 覆盖 频道×样本组×周期（n≥10 的 30 格）。
- **判定门槛**（预先写入协议）：≥30 笔独立交易；BH q<0.10 且 p<0.05；≥8 个入场周且超额的周聚类 bootstrap 95% 区间下限 >0；去掉最好一笔后均值 >0；80bps 成本下仍 >0。全部满足才算“站得住”，即便满足也不等于策略可用。
- **封存后的改动**：评估路径修了两个机械错误（面板按币名查找、JSON 键类型），不影响标签与收益口径。按格式拆分、TP1/止损模拟、喊单战绩核对是看过主结果后加的，**全部是探索性的**，不进 FDR 族、不用来改判。

## 4. 主结果（无条件明确喊单，全方向）

单位：百分比 / 百分点。“超额” = 实际净收益 − 同币零分布均值；p 为“优于零分布”的单侧置换 p；区间为超额的入场周聚类 bootstrap 95% 区间。所有 30 格的 BH q 都是 1.00（没有一格在“正优势”方向接近显著）。

| 频道 | 周期 | 独立笔数 | 胜率 | 均值净收益 | 中位数 | 零分布均值 | 超额 | p | 超额 95% 区间 | 喊单前 24h 涨幅 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| Market Dealer | 1h | 24 | 46% | −0.05 | −0.06 | −0.18 | +0.13 | 0.30 | [−0.68, +0.91] | +6.4 |
| | **24h** | 23 | 39% | −1.31 | −1.72 | +0.20 | −1.51 | 0.89 | [−4.12, +0.66] | +6.6 |
| | 72h | 23 | 30% | −3.50 | −5.14 | +0.85 | −4.36 | 0.99 | [−8.52, −0.44] | |
| | 168h | 22 | 41% | −1.92 | −2.62 | +3.00 | −4.92 | 0.93 | [−10.86, +1.43] | |
| next10xgem1 | 1h | 329 | 40% | −0.19 | −0.36 | −0.17 | −0.01 | 0.55 | [−0.53, +0.59] | +15.7 |
| | 4h | 326 | 34% | −0.69 | −1.04 | −0.11 | −0.58 | 1.00 | [−1.30, +0.29] | |
| | **24h** | 325 | 33% | −1.52 | −2.63 | +0.26 | **−1.78** | 1.00 | **[−3.33, −0.26]** | +15.8 |
| | 72h | 316 | 35% | +1.20 | −3.23 | +0.91 | +0.29 | 0.32 | [−3.05, +4.07] | |
| | 168h | 289 | 39% | +2.29 | −4.26 | +2.27 | +0.03 | 0.44 | [−5.28, +5.61] | |
| Wise Advice | 4h | 6 | 67% | +0.35 | +0.12 | −0.21 | +0.56 | 0.07 | 不足 8 周 | |
| | **24h** | 6 | 17% | −0.64 | −0.81 | −0.24 | −0.41 | 0.67 | 不足 8 周 | −1.0 |
| Rich Trader | 1h | 71 | 41% | −0.10 | −0.30 | −0.19 | +0.09 | 0.30 | [−0.52, +0.89] | +5.6 |
| | 4h | 69 | 30% | −0.61 | −0.75 | −0.18 | −0.43 | 0.88 | [−1.77, +0.79] | |
| | **24h** | 65 | 32% | −1.99 | −1.79 | −0.10 | **−1.89** | 0.98 | **[−2.99, −0.88]** | +5.5 |
| | 72h | 62 | 34% | +1.57 | −4.12 | −0.06 | +1.63 | 0.18 | [−2.67, +6.04] | |
| | 168h | 58 | 41% | +0.73 | −1.38 | +0.74 | −0.01 | 0.47 | [−4.46, +3.99] | |

只用“入场前 30 天”做零分布时，结论方向不变（next10xgem1 24h 超额 −2.13，Rich Trader −1.91，Market Dealer 72h −5.57）。72h/168h 的正均值来自少数暴涨币，中位数都是负的；同期山寨整体在涨（零分布均值也为正），扣掉之后超额≈0。

## 5. 诊断：为什么跟不上

**喊单在涨完之后。** 入场前 24 小时，next10xgem1 喊的币平均已涨 +15.8%（中位 +11.1%，53% 涨超 10%，13% 涨超 30%）；Rich Trader +5.5%，Market Dealer +6.6%。随后 24 小时系统性回落，这就是 24h 超额为负的来源。

**战绩帖只报赢的。** next10xgem1 有 245 条回复原喊单的“profit N%”：宣称收益中位 **75%**，而从入场到宣称时点的不加杠杆最大涨幅中位只有 **14.2%**——相当于按约 5 倍杠杆乘上区间最高价。7 天样本里，后来发了战绩帖的 158 笔平均 +10.6%，没发的 155 笔平均 **−7.1%**；亏的单子不提，再加上 38% 的消息被删。Market Dealer 同理（13 条：宣称中位 27%，实际最大涨幅中位 11.3%；有战绩帖的 10 笔 7 天 +6.8%，无战绩帖的 12 笔 −9.2%）。

**按作者自己的 TP1/止损走（探索性）**：市价入场，小时线上先碰 TP1 还是止损（同根先算止损，跳空按开盘价），最长 7 天。

| 频道 | 完成笔数 | TP1 / 止损 / 到期 | 均值 | 中位数 | 周聚类 95% 区间 |
|---|---:|---|---:|---:|---|
| next10xgem1（FUTURES 计划） | 30 | 10 / 4 / 16 | −1.13% | +0.13% | [−3.64%, +1.21%] |
| Rich Trader（结构化计划） | 15 | 7 / 1 / 7 | +2.48% | +3.74% | [−2.77%, +6.65%] |
| Wise Advice | 2 | 2 / 0 / 0 | +0.30% | — | — |

next10xgem1 的 TP1 大多只比入场高 3–12%，止损却在下方 4–27%；30 笔里 16 笔 7 天内两头都没碰到。

**按格式拆分（看过结果后加的，探索性）**：next10xgem1 的 34 条“FUTURES LONG Below/TP/SL”计划在 1–72 小时都显著差于零分布（24h 超额 −2.64，区间 [−3.80, −2.02]）。Rich Trader 的 28 条结构化计划在 7 天上为 +6.66%（超额 +5.44，p=0.053，区间 [−3.87, +23.06]），但一半毛利来自 EPIC 一个币，而且是事后挑出的子组，不能当发现。Rich Trader 的“BULLISH / ready to BOOM / programmed”类观点帖 24h 超额 −5.16（26 笔，区间 [−9.39, −2.25]）。

## 6. 各频道结论

- **Market Dealer**：内容最像分析，明确喊单主要是“❗️Buying some $X here”的现货买入（另有 7 条涉及 5 个本地无数据的标的，含一只美股和几只链上 meme）。可交易样本 23 笔，低于 30 笔门槛；24h 超额 −1.51（区间跨 0），72h 超额 −4.36（区间不含 0）。三分之一消息被删、战绩帖选择性报告。**样本不足，现有迹象偏负。**
- **next10xgem1（Binance Spot & FutuRes Signals）**：典型拉盘信号频道。325 笔独立做多喊单覆盖 187 个币、26 个入场周，24h 净收益 −1.52%、超额显著为负，72h/168h 中位数 −3% 到 −4%，任何周期都没有显著的正超额（72h/168h 超额约为 0）。“PROFIT IS GUARANTEED”的自我宣称与数据相反：宣称收益靠杠杆×区间最高价，亏损单不报，38% 消息被删。**无优势，跟单在统计上是亏的。**
- **Wise Advice Signal**：本质是新闻 + 推广频道（prop firm 推荐码、跟单平台邀请、VIP 机器人、投票拉票）。180 天只有 13 条明确的加密货币喊单（6 笔可交易），大量 BTC/黄金短线喊单不写币名或是黄金。**无法研究，也不值得研究。**
- **The Rich Trader Reborn**：喊“Long #X”的小市值币为主，也有带入场/目标/止损的结构化计划。65 笔独立喊单 24h 超额 −1.89（区间不含 0），7 天约为 0；结构化计划在 7 天上偏正但不显著、集中在单个币，且是事后子组；29% 的帖子在发布 10 分钟以后还被修改。**无优势。**

## 7. 前向跟踪

**不建议对任何一个频道做“跟单”前向跟踪。** 唯一可能有研究价值的是一个由本数据生成的新假设：**next10xgem1 喊单后 24 小时反向（做空）**。它在本样本上平均 +1.1%/笔、65% 胜率，但第 5 百分位 −20.5%、最差一笔 −94.7%（喊完继续翻倍）；72h/168h 反向会被少数币暴涨 6 倍以上的尾部吞掉。它是看过数据后提出的，必须用新样本检验。下面只是草案，**未激活、未接入新闻流程、未建跟踪器**，由用户决定：

```json
{
  "protocol": "next10xgem1-fade-24h-v0",
  "status": "documented_not_running",
  "hypothesis": "kol4-v1 fresh_call long signals from @next10xgem1 underperform the same-coin null over 24h",
  "source": "public preview t.me/s/next10xgem1 (public channel; no Telethon session needed)",
  "eligibility": "kol4-v1 trade_plan/direct_call, long, ticker has a Binance USDT perpetual; first receipt <= 30 min after publication; edits after receipt never rewrite the frozen signal",
  "entry": "short at the first 5m open >= receipt + 60s; skip if later than publication + 30 min",
  "exit": "fixed 24h, no discretionary stop; 1x collateral floor recorded as liquidation",
  "cost": "20bps round trip + actual funding; 40bps stress",
  "statistic": "mean (net - same-coin pre-entry 30d random-start 24h short null), one-sided, week-cluster CI",
  "sample": "fixed n = 350 independent trades (80% power for a 1.8pp effect at sd 13.4%, one-sided 5%); about 6-7 months at 1.85 calls/day",
  "dedup": "one open position per coin",
  "pass_rule": "lower 95% week-cluster bound > 0, mean without best trade > 0, 40bps stress > 0",
  "risk_note": "right-tail squeeze risk; paper only; promotion_allowed false"
}
```

## 8. 复现

原始消息只在受限 NTFS 目录（F 盘是 exFAT，无法限制访问）：`%USERPROFILE%\.crypto_trading_system\research\telegram_new_kols_20261025\`，含 `selection.json`、四个 `*_history.json`、`PROTOCOL.md`、`events_before_prices.json`、`results/`（`results.json`、`cells.csv`、`trades.csv`）。`data/research/telegram_new_kols_20261025/LOCATION.md` 只是指针。

```powershell
$py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
& $py scripts\research_telegram_new_kols.py collect    # 已采集时跳过；会占用 reader.lock，每个频道单独加锁
& $py scripts\research_telegram_new_kols.py label      # 只在 events_before_prices.json 不存在时写入
& $py scripts\research_telegram_new_kols.py evaluate   # 只读本地 K 线，覆盖 results/
& $py -m pytest tests\test_research_telegram_new_kols.py -q
```

采集期间 Web 的 Telegram 前向任务照常运行（12:15 UTC 一轮 ok，busy_streak 0）。

## 9. 限制

- 只看到存活且是最终版本的帖子；高删帖率让 Market Dealer 和 next10xgem1 的结果偏乐观，结论“无优势”只会更稳。
- 标签是正则，不是人工逐条标注；盲审覆盖了全部被判为喊单的帖子，但仍可能漏掉写法特殊的喊单。
- 小时级入场忽略了分钟级反应；对“喊单后几分钟内冲高”的频道，真实跟单只会更差。
- 图片里的价位、趋势线没有识别；只分析文字。
- 用户自选频道本身就是选择偏差；这次结论是负的，不受这一点影响。
