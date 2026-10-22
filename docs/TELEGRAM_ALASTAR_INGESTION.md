# Alastar Trades 消息接入

研究结果、策略尝试及前瞻模拟候选见[本地研究总入口](ALASTAR_RESEARCH_INDEX.md)。

2026年10月21日：新增[首次观测与版本归档](ALASTAR_OBSERVATION_ARCHIVE.md)，
已通过 `defaults.telegram_alastar_observation_archive: true` 接入现有新闻采集。
完整文字与可见版本在新闻时间过滤、展示截断和数据库去重前保存到
`data/research/alastar_observation/`；旧历史不能成为前瞻样本。
新闻数据库本身仍不回写已存文章的编辑，修订证据在研究归档中保留。

来源：[Alastar Trades / @alastartrades](https://t.me/alastartrades)。
读取 Telegram 的公开预览，使用现有代理设置，无需 Telegram 账号、Bot Token 或用户 API 凭据。

`config/news_rules.yaml` 已启用 `telegram_alastar`，由现有新闻 worker 默认每 60 秒调度。
首次修改代码或来源列表后需重启新闻 worker。`NEWS_ENABLE_TELEGRAM_ALASTAR=false` 可停用；
`NEWS_INTERVAL_TELEGRAM_ALASTAR` 可调整检查间隔。其他新闻源耗时也会影响实际检查频率。

首次导入当前公开预览中的历史文章：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts\sync_alastar_telegram.py
```

此命令只保存历史原文，不把旧文章加入事件提取队列。后续最近 4 小时内的新文章通过正常
新闻采集、入库和分析流程处理。文章按消息链接去重，保留原始发布时间、频道、消息编号、
原文和币种标签，来源类型标记为第三方评论；文中仓位数值与观点是作者陈述。

本地历史查询：
<http://127.0.0.1:8000/api/news/raw/history?source=telegram_alastar&limit=50>。
新文章会进入现有资讯页；默认 24 小时视图不会显示更早的文章。

2026-10-22 补充：
- 纯图片帖（无文字说明）仍不进新闻库，但会进入首次收件研究归档：`media_only=true`、文字为空，
  排除原因`media_only_no_text`，不会进入文字候选队列，只为图片研究保留真实首次收件时刻。已有文字消息的版本哈希不变。
- `telegram_observation_archive_channels`（默认`[alastartrades]`）列出要归档的频道，须是`telegram_alastar_channels`的子集；
  Alastar 继续写`data/research/alastar_observation/`，其他频道各写`data/research/telegram_observation/<频道>/`，档案不能混用频道。
- 多频道时某个频道请求失败或页面结构变化，只记日志并跳过该频道；所有频道都失败才报源错误。
- 守护任务同时启动独立 LLM worker 时，新闻 worker 以`--pull-only`运行：新文章只写入数据库任务队列，
  由 LLM worker 处理，模型变慢也不会拖住各来源的轮询（10/21 曾因此把 Telegram 轮询拖到约7分钟）。

范围是公开预览的当前一页，不保证完整历史、断线期间全部消息、已删除文章、旧文章的修改
同步，也不包含图片内文字、视频、私密频道或 Pumpglass 付费告警。公开页访问失败会显示源错误，
频道暂未发新文章时仍记录成功检查；不会把旧文章的日期改成采集时间。
