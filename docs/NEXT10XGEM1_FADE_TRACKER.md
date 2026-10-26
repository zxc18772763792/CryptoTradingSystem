# next10xgem1 喊单反向 · 做空 24h 纸面跟踪（next10xgem1_fade_24h_v1）

这是 [10/26 深入检验](TELEGRAM_NEXT10XGEM1_FADE_STUDY_20261026.md)之后的前向纸面检验。被检验的规则就是研究里最稳、也唯一没经过挑选的那个版本：喊单后马上做空，固定持有 24 小时。**只记账，不下单**：模块里没有任何账户或下单接口，不调用 AI，研究页上标为“仅纸面”。

- 协议（预注册，已入库）：[config/research_protocols/next10xgem1_fade_24h_v1.json](../config/research_protocols/next10xgem1_fade_24h_v1.json)
- 账本：[core/research/next10xgem1_fade_tracker.py](../core/research/next10xgem1_fade_tracker.py)
- 冻结的喊单分类规则：[core/research/kol4_call_labels.py](../core/research/kol4_call_labels.py)
- 命令行：[scripts/next10xgem1_fade_tracker.py](../scripts/next10xgem1_fade_tracker.py)
- 测试：[tests/test_next10xgem1_fade_tracker.py](../tests/test_next10xgem1_fade_tracker.py)、[tests/test_kol4_call_labels.py](../tests/test_kol4_call_labels.py)

## 规则

| 项目 | 规则 |
|---|---|
| 信号来源 | 新闻采集器轮询公开页 t.me/s/next10xgem1，**只存档**：不生成新闻，不调用 AI。账本只用每条帖子的首次收件，要求发布后 10 分钟内收到 |
| 喊单识别 | 10/25 研究封存标签时用的 kol4-v1 文字规则，原样冻结成独立模块。只取 `direct_call` / `trade_plan` 两类，且不带条件、方向为做多；以首次收到的文字为准，之后的编辑不改写 |
| 标的 | 能对上币安 USD-M 状态为 TRADING 的 USDT 永续（含 1000 倍前缀，交易对清单每 6 小时刷新一次） |
| 入场 | 收件且冻结后满 60 秒的下一个 5 分钟官方 K 线开盘价做空；比发帖晚 30 分钟以上就不进场 |
| 离场 | 入场后整 24 小时的 5 分钟开盘价。持有期内最高价到入场价 2 倍按 −100% 爆仓（1 倍保证金） |
| 第二本账 | 同样的入场，加 50% 灾难止损：最高价触及 1.5 倍入场价时按该价位离场，开盘跳空越过时按开盘价，另加 0.5% 滑点。只作描述，不参与判定 |
| 成本 | 往返 20bps；另记 80bps 压力成本。资金费按实际结算计入：只用“最近 N 条”的查询方式（带时间窗的查询会被 WAF 拦成 403），边界 60 秒内的结算只在不利时计入，覆盖不能证明时记为缺失，不当作 0 |
| 仓位 | 每笔 1,000 USDT 名义；同一个币已有仓位时新喊单跳过；同时最多 8 笔，超出就跳过 |
| 对照 | 同一个币在入场前 30 天内、每个整点起做空 24 小时（同样的爆仓和止损规则，只算价格、扣 20bps）的精确均值，至少要有 168 个起点 |
| 主统计 | 不止损账本的价格净收益减去对照均值，即每笔超额 |
| 样本与判定 | 245 笔完成时做无效性检查：均值超额 ≤0 就停止接收新喊单。490 笔时复核，四条全过才算通过：① 按周整块重抽样的单侧 95% 下界 >0；② 含资金费净收益均值 >0；③ 去掉最好一笔后均值超额 >0；④ 80bps 成本下均值 >0。最长 24 个月，不因“看起来很好”而提前结束 |
| 缺数据 | 出场后 7 天仍拿不到完整 5 分钟路径或对照数据的，记为不可用并计数，不补数 |

协议、账本模块和分类模块三者的 SHA-256 都写进每一条账本记录。改动任何一个，账本都会报“drift”并停止，只能另起新版本，不能在原账本上改规则。哈希按工作区里的字节计算。本仓库 `core.autocrlf=true`，检出时文件是 CRLF，所以这三个文件按 CRLF 保存：用 `git checkout-index` 模拟检出后，字节与工作区完全相同。不要改它们的换行符。

## 分类规则为什么单独冻结

研究脚本 `scripts/research_telegram_new_kols.py` 还会被继续修改，所以把封存标签用到的那段代码（提交 1e9f9c1）逐字复制成 `core/research/kol4_call_labels.py`。测试会核对三件事：

- 在任意文本上，副本与研究脚本的输出完全相同；
- 在 1,815 条 180 天历史帖子上，副本能逐条复现封存的标签、原因、条件标记和币种方向；
- 激活时重新跑一遍历史帖子，标签统计必须等于协议里登记的数字（310 条明确喊单、34 条交易计划，等等）。

另外，公开预览页和 10/25 Telethon 历史重叠的 10 条帖子，分类结果完全一致（其中 3 条是喊单）。

## 激活（未执行，需要用户确认）

激活就是预注册生效的那一刻：账本写入第一条记录后永不重置。频道暂时**没有**写进采集配置，因为一旦写进去，下一次任何原因的重启都会开始存档，在 F 盘上每天占用约 0.8 GB（见下文“本机存储”）。步骤：

0. 在 `config/news_rules.yaml` 里把 `next10xgem1` 加进三个列表：`telegram_alastar_channels`、`telegram_observation_archive_channels` 和 `telegram_archive_only_channels`。最后一个是“只存档”，不产生新闻。

   ```yaml
   telegram_alastar_channels: [alastartrades, cryptoquant_alert, next10xgem1]
   telegram_observation_archive_channels: [alastartrades, cryptoquant_alert, next10xgem1]
   telegram_archive_only_channels: [cryptoquant_alert, next10xgem1]
   ```

1. 托管重启，让新闻采集器读到新频道、让 web 加载跟踪任务：

   ```powershell
   .\web.bat stop -IncludeWorkers
   .\web.bat start -PaperMode
   ```

2. 等采集器对 next10xgem1 完成至少 4 次轮询，每次间隔不超过 600 秒，大约 7 分钟。可以用 `status` 查看 `health.healthy_for_new_freezes`。
3. 激活（会读取币安服务器时间核对本机时钟）：

   ```powershell
   $py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
   & $py scripts\next10xgem1_fade_tracker.py selftest     # 不联网：标签统计与协议一致
   & $py scripts\next10xgem1_fade_tracker.py activate
   ```

激活之后，web 里的 `next10xgem1_fade` 任务每 15 秒跑一轮：冻结新喊单、记录入场价、到期结算，每轮最多做一个需要联网的结算任务。

## 查看

```powershell
& $py scripts\next10xgem1_fade_tracker.py status    # 计数、采集健康、当前门槛（不联网）
& $py scripts\next10xgem1_fade_tracker.py report    # 判定规则四项的当前值（490 笔之前只作描述）
& $py scripts\next10xgem1_fade_tracker.py verify    # 从头核对哈希链
```

研究页“Telegram信号”分组下有一行“next10xgem1 喊单反向 · 做空24h（纸面）”。账本数据在 `data/research/next10xgem1_fade_24h_v1/`（不入库），频道存档在 `data/research/telegram_observation/next10xgem1/`。

## 时间与开销

- 10/25 研究里每天约 1.8 笔独立喊单，扣掉采集中断、晚到和重叠后会少一些。245 笔约需 4.5–5 个月，490 笔约需 9–10 个月。
- 代理流量：公开页压缩后每次约 16 KB，按现在约 100 秒一次的轮询，一个月约 400 MB，走按流量计费的 FlClash。币安方面每笔约 0.1 MB（入场 1 次、结算 5 分钟线 1 次、对照 1 小时线 1 次、资金费 1 次），交易对清单每 6 小时 52 KB。
- **本机存储（重要）**：频道存档每次轮询都写一个约 5 KB 的记录文件，每天约 800 个。F 盘是 exFAT，分配单元 1 MB，每个小文件实际占 1 MB，所以每个存档频道每天约占 **0.8 GB**。10/26 实测：Alastar 和 CryptoQuant 两个存档已经是每天约 1.6 GB（分别 3.8 GB / 5 天、1.5 GB / 1.8 天），F 盘剩 484 GB；加上本频道后约每天 2.4 GB，大约 6.6 个月写满，比本检验需要的时间还短。这是存档模块原有的问题，需要单独修，例如把较早的轮询记录打包。两个账本都只读最近 400 次以内的轮询记录（健康检查读最近 12 次，首次收件定位往回最多查 400 次），所以只要保留最近的记录，修复就不会影响它们。账本自己每笔约产生 5–10 个文件，一个月约 0.5 GB。

## 限制

- 公开预览页一次只显示最近 20 条帖子（约 1.3 天）。采集停一天以上会漏帖，漏掉的不补；健康门槛只能保证冻结时采集是正常的。
- 10/25 研究看的是存活帖子，前向账本看的是首次收到的帖子，包括后来被删除的，两者的样本构成不同。这正是前向检验要回答的问题之一。
- 纸面成交价是 5 分钟 K 线开盘价，没有考虑盘口深度和真实滑点。被拉盘的小币可能无法开空或临时被调整保证金，账本反映不了这些约束。
