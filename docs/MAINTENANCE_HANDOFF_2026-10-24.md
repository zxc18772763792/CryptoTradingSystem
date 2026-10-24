# 重启维护交接（2026-10-24）

本次用户要求：整理项目、维护文档，然后停止运行，由用户自行重启电脑。本文记录当次维护状态，之后的实时状态以进程、计划任务和运行文件为准。停机结果记录在本机 `runtime/reboot_checkpoint_20261024.json`。

## 接续入口

| 事项 | 入口 |
|---|---|
| 仓库操作与环境约束 | `AGENTS.md` |
| 启动、停止、故障处理 | `STARTUP.md` |
| Telegram 会话、授权边界、研究协议 | `docs/TELEGRAM_AGENT_HANDOFF.md` |
| 研究跟踪器及各策略协议索引 | `docs/RESEARCH_TRACKERS_2026-10-24.md`（有生成时点，数字不是实时值） |
| Telegram 初始研究和审计材料 | `reports/telegram_live_research_20261024/README.md` |
| 本次停机核验 | `runtime/reboot_checkpoint_20261024.json`（本地、Git 忽略） |

## 当前实现与已完成验证

- 用户最近的实现已整合：Telegram Web 运行任务、CryptoQuant 和币安中文群两个研究跟踪器；日常入口统一为 `web.bat`。
- Codex 聊天 `telegram` 心跳先暂停，随后按用户进一步要求删除（`DELETED`）。Telegram 采集仅由 Web 内的 `telegram_forward` 任务调度，后续不重建聊天心跳。
- 修复首次启动 Web、新闻采集、新闻解读、可选 PM worker 以及研究宇宙补数时缺少隐藏窗口设置的问题。worker 输出写入 `logs/`；新增日志模式纳入既有14天保留、每类至少留最新4份的清理规则。当前用户确认的问题是桌面实际弹出窗口。
- 2026-10-24 14:25–14:26（北京时间）已通过 `web.bat` 托管重启：Web、新闻 worker、新闻 LLM worker、守护程序恢复；模式为 paper，自治交易代理停止。Telegram Web runner 随后成功完成一轮采集。
- 同次功能检查：启动安全、数据维护脚本、进程归属、Telegram 跟踪器和 Web 运行任务相关测试共82项通过；PowerShell 解析通过，独立隐藏窗口及标准输出/错误日志捕获实测通过。不是全仓库测试结论。

## 本地改动与数据保留

整理前 Git HEAD 为 `620b5a5`，此前新增 Telegram 运行任务的提交为 `071dfd3`。最近的窗口修复尚未提交，涉及 `_once.ps1`、`scripts/run_research_universe_refresh.ps1`、`scripts/prune_logs.ps1`、`STARTUP.md`、`docs/TELEGRAM_AGENT_HANDOFF.md`；本次另增加此交接及入口链接。接续时重新运行 `git status --short` 和 `git diff` 确认现状。

多个 `reports/` 研究与审计目录尚未跟踪，保留原位置；未批量搬移、删除、暂存或提交。`.env*`、`keys.txt`、运行数据库、Telegram 会话和数据归档继续保留在本地忽略目录。远程仓库公开，未经用户明确要求不推送。

Telegram 冻结目录为 `data/research/telegram_live_20261024/`，五本模拟账本保留；缺样本的收益应为 `null`，不能写成0%或宣称已验证盈利。不要重采历史覆盖研究、不重置数据库、不修改策略阈值或实现指纹。断电/重启期间属于覆盖中断，恢复后按原规则等待完整窗口，不能回填成前向信号。会话已由 DPAPI 加密保存，正常重启后复用，不重新 bootstrap、不显示凭据。

## 重启前与恢复方式

本次通过托管入口停止 Web、守护程序和外部新闻 worker，并结束归属于本项目的在运行定时补数任务；核对 Telegram 子进程、8000监听及剩余项目进程。已下载数据和冻结研究保留。停止计划任务的当前运行实例，不删除或禁用其既有配置。

实际核验发现：结束 Coinglass 计划任务后，独立 Python 采集子进程仍可能存活。本次按停机前记录的 PID、创建时间、父进程和完整脚本路径重新核对后结束该子进程；以后也应核对实际进程，不只看任务的 Ready 状态。历史原始数据采用临时文件替换写入，已落盘文件保留。

现有登录守护任务仍启用，登录后延迟约2分钟自动恢复 paper 服务；补数任务按原计划运行。恢复时先查看状态，避免重复启动：

```powershell
Set-Location 'F:\9_Crypto\crypto_trading_system'
.\web.bat status
# 若服务未运行，手动恢复原 paper 组合：
.\web.bat start -PaperMode
```

检查 `http://127.0.0.1:8000/livez`、`/readyz`，以及 `runtime/telegram_forward_runner.json` 和 `data/research/telegram_live_20261024/forward_status.json`。服务仍在预热时先看日志，不连续重复拉起。旧 PID 或 runner 的 `enabled` 字段不是进程仍存活的证据，应结合文件时间和实际进程判断；不要删除锁文件来绕过占用。

## 后续待办（重启后再做）

1. 内存：维护前进程工作集合计约1.5 GB，Web约824 MB、新闻采集约238 MB、新闻解读约217 MB，另有守护及补数。工作集可能重复计入共享页，快照不是泄漏证明。后续按进程采样私有内存和工作集随时间的变化，再定位缓存、数据加载或重复任务；本次不启动剖析和全量测试。
2. 窗口：隐藏启动已应用并重启验证；后续确认登录自启、每小时补数及 supervisor 自愈时是否仍有弹窗，不批量关闭其他工程的终端。
3. Telegram 汇总：维护时发现 `forward_status.json` 持续更新，而 `forward_summary.json` 仍是早先报告时点。停机后运行只读汇总脚本刷新；后续检查 Web runner 是否应在成功 cycle 后自动生成汇总，避免页面读取过期报告。不要改变冻结交易规则。
4. 授权：三个固定来源的研究用途授权状态为用户确认，具体范围和依据待用户补充；不扩大来源和用途。

用户本次要求停下来；完成交接和停机后不继续修复、拉起服务、联网采集或运行长任务，等待用户重启后再安排。

## 后续恢复记录

2026-10-24 用户随后明确要求“重启服务”。14:52–14:53（北京时间）已通过 `web.bat stop -IncludeWorkers`、`web.bat start -PaperMode` 完成托管重启；首页 HTTP 200，`/readyz` 为 ready，Binance 连接及实时行情健康。Web、新闻采集、新闻解读和守护程序恢复。Telegram Web runner 已启动，按近期成功状态跳过重复采集；聊天心跳仍为 PAUSED。此前的停机检查点保留为历史记录，当前状态应读取新的运行文件及进程。

用户之后明确要求 Telegram 已转移到跟踪器，Codex 不再心跳。已通过应用工具删除 `telegram` 自动化（返回 `deleteStatus=deleted`）；上文 PAUSED 是此前时点的状态，当前为 DELETED。项目内跟踪器继续负责运行。
