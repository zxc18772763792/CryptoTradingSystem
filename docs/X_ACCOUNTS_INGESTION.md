# X（Twitter）白名单账号接入

来源名 `x_accounts`，采集器 [x_accounts.py](../core/news/collectors/x_accounts.py)。
**默认关闭**，需要一个专用 X 小号的登录 cookie。

## 方案与风险

X 没有免登录的公开读取通道。这里用开源库 [Scweet](https://github.com/Altimis/Scweet)（MIT，
`pip install "Scweet>=5.8.2,<6"`，已写入 requirements.txt），它带着小号的 `auth_token` cookie 调用
X 网页端未公开的 GraphQL 接口。

- 这违反 X 的服务条款，**小号可能被限流、锁定或封禁**。务必用专用小号，不要用主账号。
- 接口会无预告变化；Scweet 会在进程启动时刷新查询 ID，仍可能整体失效，需要升级库。
- `auth_token` 等同于小号的登录凭据：只放 `.env.local`（已 gitignore），不要写进仓库、文档或聊天。
  Scweet 的会话状态文件 `data/x_accounts/scweet_state.db` 也含 cookie，同样在 gitignore 的 `data/` 下。
- 需要能访问 x.com 的代理：优先 `X_PROXY`，否则用 `HTTPS_PROXY`／`HTTP_PROXY`（见 `.env`）。
  不要把 x.com 加进 `PROXY_BYPASS_HOSTS`。

## 启用步骤

1. 用专用小号在浏览器登录 x.com，开发者工具 → Application → Cookies → `https://x.com`，复制 `auth_token` 的值。
2. 在自己的终端运行 `scripts\set_x_token.py`，在隐藏提示处粘贴 token。它会把 `X_AUTH_TOKEN` 和
   `NEWS_ENABLE_X_ACCOUNTS=true` 写进 `.env.local`，保留其他行，只显示 token 长度；确认 `.env.local`
   被 gitignore 才会写。也可以手动在 `.env.local` 加这两行。

3. 在 [news_rules.yaml](../config/news_rules.yaml) 填 `x_accounts_handles`（1–20 个，不带 @ 和链接）。
   留空则来源保持禁用并在来源错误里提示。
4. 先做一次只读探针（不写新闻库，只发少量请求）：

   ```powershell
   & 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts\probe_x_accounts.py <handle> --limit 5
   ```

5. 探针看到帖子后，重启新闻 worker（`.\web.bat` 系列，守护任务托管时按 STARTUP.md）使配置生效。

## 采集什么

- 每次轮询对每个白名单账号请求最新 `x_accounts_tweets_per_account`（默认 20）条，按 `since` 过滤。
  高频发帖账号若一个周期内超过该条数，会漏掉更早的帖子。历史回填见 `scripts/backfill_x_kol.py`（写入研究存档，不进新闻库）。
- 只收账号**自己的原创帖、自回复串、引用帖**（引用的原文放在 payload `quoted_text`）。
  跳过：纯转推、回复他人、没有文字的纯图片帖、时间戳缺失或不带时区的帖子（不会编造日期）。
- 币种标签只取 `$BTC` 这类 cashtag 和 `XXXUSDT` 交易对，普通 `#话题` 不当币种。
- 来源类型标记为第三方评论；按 `x:<handle>:<tweet_id>` 去重，链接固定为 `https://x.com/<handle>/status/<id>`。
- 进入现有新闻库和 LLM 事件提取流程（重要度 30，低于 Telegram 的 35 和交易所公告）。账号多、发帖多时会增加 LLM 调用量。
- 查询：`http://127.0.0.1:8000/api/news/raw/history?source=x_accounts&limit=50`。

## 请求预算与失败行为

- Scweet 默认每个账号每天 300 次请求（`x_accounts_daily_requests_limit`）。实测一个账号一次轮询、≤20 条约 1 次请求（用户 ID 查询有缓存）。现在 13 个账号每 2 小时一轮，约 160 次/天。账号更多就拉长 `NEWS_INTERVAL_X_ACCOUNTS`（该变量读进程环境，不读 `.env.local`）。调高每日上限会增加封号风险。
- X 自己的限流更紧：10/25 在一个 15 分钟窗口里约 49 次请求后返回 429。回填请用 `scripts/backfill_x_kol.py`（每 15 分钟最多 30 次），别写循环。
- Scweet 出错时返回空列表而不是抛异常；采集器会捕获它的警告并查状态库里的 `last_error_code`，把这类空结果当失败，不会记成"该账号没有新帖"。
- 账号之间至少间隔约 6 秒加抖动；单个账号失败只记日志，其余继续。
- 遇到登录被拒／账号被锁（AuthError）暂停 6 小时，限流（RateLimitError）或额度用尽暂停 30 分钟；
  暂停期间不向 X 发请求，来源错误里会显示剩余时间。登录被拒后需要换新的 `auth_token`，再重启 worker。
- 所有账号都失败才算来源失败，不推进游标。

## 研究存档（仅研究账号）

`x_accounts_archive: true` 时，每次轮询返回的所有帖子（含转推、回复、纯图片帖，带标记）都追加写入`data/research/x_kol/<账号>.jsonl`，记录发帖时间、首次看到的时间和来源（实时／回填）。`x_accounts_archive_only_handles` 里的账号只存档，不产生新闻条目，也不触发 LLM 抽取。KOL 研究的做法和结论见 [X_KOL_RESEARCH_PROTOCOL.md](X_KOL_RESEARCH_PROTOCOL.md)，后续 agent 先读 [X_AGENT_HANDOFF.md](X_AGENT_HANDOFF.md)。
