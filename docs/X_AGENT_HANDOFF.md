# X（Twitter）采集与 KOL 研究：给后续 agent 的交接

先读本文，再动任何和 X 有关的代码或数据。用户接入说明见 [X_ACCOUNTS_INGESTION.md](X_ACCOUNTS_INGESTION.md)，
KOL 研究的冻结协议见 [X_KOL_RESEARCH_PROTOCOL.md](X_KOL_RESEARCH_PROTOCOL.md)。

## 现状（2026-10-25）

- 用户的专用 X 小号的 `auth_token` 已在 `.env.local`（`X_AUTH_TOKEN`，另有 `NEWS_ENABLE_X_ACCOUNTS=true`）。
  **不要读出、打印、复述或提交它**；要换 token 让用户运行 `scripts/set_x_token.py`（隐藏输入）。
  不要替用户登录、创建账号或解密浏览器 cookie。
- 采集器 `core/news/collectors/x_accounts.py`（来源名 `x_accounts`，库 Scweet 5.8.2）。白名单 13 个账号在
  `config/news_rules.yaml` 的 `x_accounts_handles`：前 3 个（wublockchain12、williamlab、whaleinsider）进新闻流程，
  其余 10 个是**仅研究**（`x_accounts_archive_only_handles`：只存档，不产生新闻条目，不触发 LLM 抽取）。
  默认每 2 小时轮询一次（`DEFAULT_INTERVALS["x_accounts"] = 7200`）。
- 提交：`2fe1c5f`、`9082b9b`（采集器、接线、静默失败修复）；其后的存档层、回填、研究脚本、协议和本文
  见 git log，若 `git status` 里仍有这些文件说明还没提交。分支 `codex/operating-reality-bug-sweep`，未推送。
- 新闻 worker 不要为了验证而重启（AGENTS.md 第 12 节的维护检查点）；是否已在线上跑看 `.\web.bat status`。

- KOL 研究结论（2026-10-25）：13 个账号、90 天窗口、368 个事件，主检验 p=0.12，去掉最好 5% 后为负，**按冻结协议判定为"没有证据"**；详见 [X_KOL_RESEARCH_RESULTS_2026-10-25.md](X_KOL_RESEARCH_RESULTS_2026-10-25.md)。没有可接入实盘或纸面交易的信号。前向样本要等新闻 worker 重启后实时存档积累，再用同一协议重跑。

## 必须遵守的规则

1. **这是违反 X 服务条款的 cookie 方案，封号会同时废掉实时采集。** 每次真实请求都要有理由。
   先用 `scripts/probe_x_accounts.py <账号…> --limit 5` 做小规模验证，不要写循环拉全量。
2. **X 的真实限流比 Scweet 的日预算更紧**：2026-10-25 在一个 15 分钟窗口内约 49 次请求后，`UserTweets` 返回
   `429 Rate limit exceeded`。之后要等满窗口再试，连续撞 429 不会更快恢复。回填脚本因此按每 15 分钟 30 次放慢。
   Scweet 日预算默认每号每天 300 次（`x_accounts_daily_requests_limit`）；实时轮询一个账号一次约 1 次请求。
3. **Scweet 出错不抛异常，返回空列表。** 网络错误只写 WARNING；429 只写 INFO 日志，并把
   `last_error_code=429` 记进它的状态库。所以采集器的 `_fetch` 做三件事：捕获 `Scweet` 日志警告，空结果＋警告算失败；
   空结果无警告时再用 `refusal_code(client)` 查状态库的 `last_error_code`，是 401/403/429 就抛 `XRateLimited`
   并暂停（登录被拒 6 小时、限流 30 分钟）；代理 SSL 断连（`curl:`、`status=599`）最多重试两次。
   **任何直接调 Scweet 的代码都必须这样处理，不能把空列表当作"没有帖子"。**
4. token 只存在 `.env`、`.env.local`；Scweet 的会话状态 `data/x_accounts/scweet_state.db`（实时）和
   `scweet_backfill.db`（回填）含 cookie，在 gitignore 的 `data/` 下，别复制到别处。
5. `.env` / `.env.local` 只进 `settings`，不导出到 `os.environ`。读 X 配置用 `x_accounts.setting()`
   （先环境变量，后 settings）；`NEWS_INTERVAL_*` 这类开关只读进程环境。
6. 用项目解释器（AGENTS.md 第 1 节），不要用裸 `python`。提交只按路径 `git add`，不要 `git add -A`。
7. 长时间的回填要放在能撑过 10 分钟的后台任务里，并先 `--dry-run` 看请求估算。

## 文件地图

| 文件 | 作用 |
|---|---|
| `core/news/collectors/x_accounts.py` | 采集器：白名单、过滤、暂停、重试、静默失败检测、token 脱敏、研究存档钩子 |
| `core/news/x_archive.py` | 研究存档：每个账号一个 JSONL，`data/research/x_kol/<账号>.jsonl`，只追加，读取时去重 |
| `scripts/backfill_x_kol.py` | 预算内回填老帖（`handle:深度`），自带限流感知，`--dry-run` 先看估算 |
| `scripts/research_x_kol.py` | KOL 事件研究，实现冻结协议；结果 JSON 写入 `reports/x_kol_research_*/` 并带协议和脚本的 sha256 |
| `scripts/probe_x_accounts.py` | 只读探针，不写新闻库 |
| `scripts/set_x_token.py` | 把 token 从隐藏输入写进 `.env.local` |
| `config/news_rules.yaml` | `x_accounts_*` 配置：白名单、存档开关、仅研究名单 |
| `tests/test_x_accounts_collector.py`、`test_x_archive.py`、`test_research_x_kol.py`、`test_set_x_token.py` | 测试；研究脚本用合成数据检验"能找到植入的效应、不从噪声里造效应" |

## 存档语义（研究必读）

每条记录有 `created_at`（X 说的发帖时间）和 `first_seen_at`（我们第一次看到它的时间），以及
`provenance`：`live`（实时轮询，`first_seen_at` 贴近发帖）或 `backfill`（回填的老帖，`first_seen_at` 是回填那天，
**不代表当时有人能看到**）。发现样本用回填数据；协议冻结之后实时采集到的帖子是前向样本，不得回头参与调参。
存档里保留转推、回复和纯图片帖（带标记），分析时自己过滤。发帖后被删的帖子回填看不到。

## 采集语义

新闻流程只收白名单账号自己的帖子（含自回复串和引用），跳过纯转推、回复他人、纯图片帖、无时区时间戳。
币种标签只认 `$CASHTAG` 和 `XXXUSDT`。来源类型是第三方评论，进新闻库的重要度 30。
帖子是公开观点，不是交易指令；KOL 广告帖没有标注，无法剔除。
