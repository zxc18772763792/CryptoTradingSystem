# CryptoQuant Alert · BTC 交易所流向影子账本

2026-10-24 按用户要求（路线 A）接入。只记录，不触发开仓；`execution_authorized` 与 `promotion_allowed` 在协议和代码里都是 false。

## 做什么

@cryptoquant_alert 是自动告警频道，帖子格式固定（"🐳 2,000.64 #BTC ($162,442,109) / aggregated inflows to #Binance" 等）。离线抓到的 1,994 条里 99.3% 一条正则就能解析，全是 BTC。

接入后的数据流：

```
新闻采集器公开页轮询 t.me/s/cryptoquant_alert（约每分钟，无需 Telegram 账号）
  → 首次收件存档 data/research/telegram_observation/cryptoquant_alert/（哈希链，不产生新闻条目、不进 AI 抽取）
  → core/research/cryptoquant_alert_shadow.py（Web 运行任务 cryptoquant_alert_shadow，每 15 秒一轮）
  → 激活后首次收到的告警 → 确定性解析 → 冻结一条影子观察
  → 入场：收件及冻结后至少 60 秒的下一个 5 分钟开盘（BTCUSDT 永续，公开数据）
  → 1 / 4 / 24 / 72 小时退出同时冻结，24h 为主周期
  → 与入场前 30 天同向“每一个 5 分钟起点”的精确均值比较（超额）
```

方向由模板决定（协议写死）：交易所流入、矿工转入交易所 → 做空；"Unusual Exchange Outflows" → 做多。模板外的文字只计数、不交易。

## 已知结论与预期

- 10/20 的离线回测用同一映射：24h 平均 +0.075%/笔，区间跨零，计 40bp 成本后为负。所以这里是前向再测一次，不是在确认旧结果。
- 近期频率约 0.5–0.9 条/天，再经“24 小时内首条才计入”的独立性规则，约半年才能攒到 66 笔，空样本显示为空而不是 0%。
- 若将来被证实有用，也只意味着可以另立协议把它当 BTC 背景过滤器；不是交易授权。

## 文件

| 路径 | 内容 |
|---|---|
| `data/research/cryptoquant_alert_20261024/prospective_shadow_protocol_v1.json` | 预注册协议（激活后不可改，哈希写进每条记录） |
| `data/research/cryptoquant_alert_shadow_v1/journal/` | 哈希链账本：activation / freeze / entry / settlement / funding / null / missed / *_unavailable |
| `data/research/cryptoquant_alert_shadow_v1/runtime.json` | 运行状态（最近一轮、模板外文字计数） |
| `data/research/cryptoquant_alert_shadow_v1/evidence/` | 每次公开行情响应原文及哈希 |
| `core/research/cryptoquant_alert_shadow.py` | 解析、账本、跟踪器（实现哈希被钉死，改动后账本会报“实现变化”并停止） |
| `scripts/observe_cryptoquant_alert.py` | 命令行：status / selftest / clock / activate / tick / verify / report |
| `config/news_rules.yaml` | `telegram_alastar_channels`、`telegram_observation_archive_channels`、`telegram_archive_only_channels` |

## 日常

```bash
python scripts/observe_cryptoquant_alert.py status    # 账本、门槛、采集健康
python scripts/observe_cryptoquant_alert.py report    # 预注册统计（66 笔前仅描述性）
python scripts/observe_cryptoquant_alert.py verify    # 整条哈希链重读
```

策略页“研究跟踪器”表里有一行 `cryptoquant_alert_shadow_v1`。

## 规则要点（完整见协议）

- 入选：激活后首次实时收到、距发布不超过 10 分钟、未编辑、未被修订、资产为 BTC、采集健康（最近 4 次轮询间隔均 ≤600 秒）、币安服务器时间偏差 ≤2 秒、入场不晚于发布后 30 分钟。
- 不入选的告警也会记账并写明原因，不会被悄悄丢掉。
- 冻结晚于计划入场则记 missed，不补成交；行情取不到则重试，超过一小时记 missed。
- 门槛：主样本满 40 笔做无效性检验（均值 ≤0 则停止开新观察），满 66 笔做确认性复核（周块自助法单侧 95% 下界 >0 且 40bp 压力均值 >0），最长 24 个月。
- 重启或采集中断是覆盖中断，窗口不补；`missed` 与 `entry_too_late` 在跟踪器里单列。

## 停用

在 `config/news_rules.yaml` 去掉 `cryptoquant_alert`（三处）并重启新闻 worker 即停止采集；账本保留。不要删除 `data/research/cryptoquant_alert_shadow_v1/`，也不要手工改协议：需要换规则就新建协议和新账本。
