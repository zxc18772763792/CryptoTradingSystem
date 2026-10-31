# 本地新闻结构化研究库

入口：`scripts/news_library.py`；纯本地读取和新快照写入，不调用模型、Telegram/X或交易接口。
运行环境遵守仓库 `AGENTS.md`。私有消息输出要求NTFS并限制当前用户和SYSTEM访问。

## 当前快照

主库：`C:\Users\zxc\.crypto_trading_system\research\news_event_library_20261025\v3\library.sqlite`。
研究报告、逐笔结果、可复跑笔记本及核验记录在其上一级目录。V1/V2为保留的中间快照。
来源包括原规范化库、当前新闻库的一致读取事务、已加入三频道180天历史和本地X存档；没有额外联网采集。

## 数据层和边界

| 表 | 粒度与用途 |
|---|---|
| documents | 来源消息×文本版本；原文、SHA、发表/首见/编辑/抽取完成时间 |
| occurrences | 每次来源快照记录；重复文本也保留输入路径、主键与接收语义 |
| claims | 逐句事件、肯定/传闻/条件/否认/推广/复盘、预告或已发生等原文断言 |
| claim_assets | 原句中的币种提及、逐字符证据；未知币保留为unresolved |
| claim_scopes | 主体角色、资产作用、日/周/月数据口径；不等同于已核验代币身份 |
| quantities | 十进制数值、原单位与原句；金额不自动等同于买入/转账名义额 |
| temporal_mentions | 日期线索；时区无法证明时不填UTC发生时点 |
| contract_mentions | 地址提及；不凭地址格式认定网络、代币或钱包身份 |
| reviewed_plans | 425条此前逐条复核的HUB/Zetoshi计划，严格匹配原文哈希 |
| legacy_labels | 旧模型标签与保守资产匹配状态，独立于新抽取；未匹配不自动视为错误 |
| quality_issues | 缺文本/首见/时间、迟到、未绑定资产等问题；类别可重叠 |
| symbol_map | 已有行情缓存的原生合约映射及认证状态 |
| document_fts | 原文trigram检索；research_claims视图连接上述事件和证据 |

所有新抽取都是`machine_extracted`，不是人工或外部事实核验。标题、摘要和完整文章并不等价。
同一故事的转发不构成独立共识。币名可能只是被盗货币、桥接网络或钓鱼诱饵，不能直接映射为做空对象。
`completed`表示文字陈述已发生，不证明新闻事实真实。金额可能属于股票回购、市值、收益或损失。

`decision_available_at`不早于本次抽取完成，不能将今天生成的标签回填为历史前向决定。
`retrospective_observed_at`供回顾性接收时钟模拟；`historical_proxy_at`供明确标识的历史代理。
价格研究只使用已认证UTC和官方资金费缓存。原始库、既有前向账本和交易规则均不改写。
私有群原文及引用的保留期限继续受来源政策约束；研究快照不扩大保存、共享或模型处理授权。

## 查询

```powershell
$py = 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe'
$db = 'C:\Users\zxc\.crypto_trading_system\research\news_event_library_20261025\v3\library.sqlite'
& $py scripts/news_library.py stats --db $db
& $py scripts/news_library.py search --db $db --symbol BTC --event-type etf_flow --reporting-window daily --limit 10
& $py scripts/news_library.py search --db $db --text 比特币 --phase scheduled --limit 10 --metadata-only
& $py scripts/news_library.py search --db $db --event-type security_incident --asset-role stolen_or_recovered_currency --limit 10
```

文本≥3字符用FTS；更短文本用绑定参数LIKE。默认最多100条；`--metadata-only`不输出正文证据。
直接SQL可以通过`claim_id`、`version_id`和`occurrences`追溯原文及来源记录。

## 新建或升级快照

`build`需要`--base-corpus`、`--market-manifest`、`--output-dir`；可额外读取`--news-db`、
`--channel-snapshot`和`--x-archive-dir`。`reextract`需要`--input-db`、`--market-manifest`、
`--output-dir`，复制并保留所有原文版本和来源记录，仅在新的目录重建派生字段。
已有快照拒绝覆盖；失败时保留staging文件。构建锁兼容并发拒绝，不删除其他会话锁。
构建后检查`build_manifest.json`、SHA、外键、原文跨度和原始版本保留，再登记新研究规则。
本工具没有计划任务、自动上线或实盘放行入口。
