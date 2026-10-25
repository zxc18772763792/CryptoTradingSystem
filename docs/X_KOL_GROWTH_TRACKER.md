# X KOL 关注量跟踪器（阶段 0：只记录，不交易）

目的：回答"能不能定期找出关注量增长最快的 KOL，并把他们的分析接成策略"之前，先把数据攒起来。
X 不提供历史关注量，**增长只能从第一次快照往后算**，所以阶段 0 不选人、不看帖子、不下任何单。

背景和结论见 [X_KOL_RESEARCH_RESULTS_2026-10-25.md](X_KOL_RESEARCH_RESULTS_2026-10-25.md)（现有 13 个账号没有证据）和
[X_AGENT_HANDOFF.md](X_AGENT_HANDOFF.md)（账号风险、限流、静默失败的处理）。

## 用法

用项目解释器运行 `scripts/track_x_growth.py <命令>`，每个命令都可以重复跑：

| 命令 | 做什么 | 请求成本 |
|---|---|---|
| `status` | 候选池大小、队列、最近快照日、账本里近 15 分钟请求数 | 0 |
| `seed-mentions` | 把存档里 KOL 帖子点名的账号入队（默认被点名 ≥2 次），按被点名次数从高到低解析成数字 ID，同时记第一份快照 | 每个账号 1 次 |
| `seed-search` | 用固定的 10 个搜索词各取若干页用户（约 20 个/页） | 每页 1 次 |
| `snapshot` | 给活跃候选池拍关注量快照。先按 ID 批量查；X 对这条线路回 403 时，回退到按用户名查，只查一周没更新的账号，最久的优先 | 批量：每 100 个账号 1 次；回退：每个账号 1 次，单次最多 40 个 |
| `daily` | 建议定时跑的组合：先拍快照，队列没空就继续解析被点名账号，每周跑一次搜索词；一次运行共用一个请求上限 | 通常 ≤ 60 次 |
| `report` | 近 N 天关注量变化排名，**仅描述**，未冻结任何选人规则 | 0 |

`--dry-run` 只显示计划和请求估算；`--max-requests` 是单次运行的请求上限（默认 60）。
要定期运行就只调度 `daily`（UTC 00:30，即北京时间 08:30，刚好在 Scweet 的每日预算重置之后）：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts\track_x_growth.py daily
```

**定时任务（2026-10-25 经用户同意注册）**：`CryptoTradingSystem_XGrowthDaily`，每天北京时间 08:30 运行 `scripts/run_x_growth_daily.ps1`（即 `track_x_growth.py daily`），当前用户、仅登录时运行，单次最长 3 小时，错过会在可用时补跑，不重叠。输出追加到 `logs/x_growth_daily.log`（超过 1 MB 轮转一次）。注册／更新：`scripts\ensure_x_growth_daily_task.ps1`（可重复运行）；暂停：`Disable-ScheduledTask -TaskName CryptoTradingSystem_XGrowthDaily`；删除：`schtasks /Delete /TN CryptoTradingSystem_XGrowthDaily /F`。检查计划但不发请求：`run_x_growth_daily.ps1 -TrackerArgs 'daily','--dry-run'`。

## 请求和限流

- 所有批量任务（回填、跟踪器）和新闻 worker 共用一个账号，X 在 15 分钟窗口里约 49 次请求就会返回 429。
  `core/news/x_pacing.py` 是共享的请求账本：每个任务在发请求**之前**登记，批量任务会等到
  "近 15 分钟全部任务合计 ≤ 40 次"才继续，所以不会互相叠加。新闻 worker 每次轮询后也会登记。
- Scweet 出错常返回空列表而不是报错。`core/news/x_batch.py` 的 `guarded_call` 会读 Scweet 的日志，
  看到失败行或状态库里记着 401/403/429 就报错停下，不会把它当成"没有结果"；代理断连会重试两次。
- 按用户名解析时，Scweet 把"用户不存在"只写成调试日志；跟踪器据此区分"确实不存在"（记 missing）和
  "没拿到答案"（记 retry，三次后放弃），不会把请求失败当成账号消失。

## 2026-10-25 实测

- 解析用户名（`aget_user_info(users=[…])`）和用户搜索（`asearch_users`）都通：5 个被点名账号解析成功，
  2 页搜索共返回 40 个新账号，简介判断出 26 个像个人、7 个像快讯号。
- **按 ID 批量查（`UsersByRestIds`）在这条线路上两次返回 HTTP 403**（隔了约 3 分钟），Scweet 自己的注释也说过
  "有些连接会被拦"。所以 `snapshot` 默认先试一次，遇到 403 就记 24 小时不再试，并回退到按用户名查。
  7 天的增长指标不需要每天快照，每个账号每周刷新一次就够：约 300 个账号每天约 40 次请求。
  代价：账号改名后按旧用户名查不到，会被当成"查不到"，连续三次后退出池子（少见，接受）。
- 批量任务的 Scweet 日预算按 **UTC 日**计，默认 300 次，今天已被上午的回填用到约 290 次，
  所以第一轮完整建池要等到 UTC 午夜之后；Scweet 在预算用完时会在发请求前直接拒绝，跟踪器据此提示
  "daily request budget is spent; it resets at UTC midnight"。

## 数据

SQLite：`data/research/x_kol_growth/growth.db`（gitignore 的 `data/` 下）。

- `candidates`：用户 ID、用户名、简介、账号创建时间、`kind_guess`（project / feed / individual / unknown，
  只是按简介文字的粗略提示，**不是过滤条件**）、来源（被点名／搜索词）、被点名次数、连续查不到次数（≥3 次视为消失）。
- `snapshots`：（用户 ID，UTC 日期）一条，含关注者、关注、发帖数、被列入名单数、认证状态；同一天第一份为准。
- `pending`：待解析的用户名队列和状态（new / done / missing / retry / failed）。

## 已知偏差（写下来，别忘）

- 搜索词返回的用户偏向已经有名的账号，增长最快的小号更容易出现在"被 KOL 点名"这条路径上，所以两条都要用。
- `kind_guess` 对中文和短简介常判成 unknown；项目号、快讯号会混在池里，阶段 1 必须单独处理。
- 池里的账号只是"出现在 KOL 圈子里"，不是随机样本。

## 阶段 1 之前必须先冻结（不要先看排名再定规则）

1. 排名窗口和指标（例如近 7 天相对增长），起始粉丝量上下限，账号最小年龄，增速上限（超出视为抽奖／买粉嫌疑）。
2. 如何排除项目号、快讯号、纯转发号（人工看简介，不看收益）。
3. 取前 K 个账号，只检验**排名日之后**发的帖子；用 [X_KOL_RESEARCH_PROTOCOL.md](X_KOL_RESEARCH_PROTOCOL.md)
   的事件研究（90 天窗口、按入场日成组的零分布、扣成本、合并检验），不逐个账号挑人。
4. 通过标准和样本量下限，以及"没有证据"时怎么收场。
5. 至少攒 3–4 周的快照和实时帖子再开始，并且 X 实时存档需要新闻 worker 重启后才会开始积累。

先验：此前的 Telegram、PumpGlass 和 13 个 KOL 的研究都没找到优势；按关注量选人还有"选中的是刚押对的人"和喊单盘风险，
预期结果大概率仍是"没有证据"。
