# Startup Quick Reference

This repository now exposes one user-facing startup/control script:

```bat
.\web.bat
```

If you only remember one command, remember `.\web.bat`.

## The Commands To Remember

Daily one-click start with browser:

```bat
.\web.bat
```

Daily managed start without forcing the browser:

```bat
.\web.bat start
```

Check what is actually running:

```bat
.\web.bat status
```

Stop web and observed workers cleanly:

```bat
.\web.bat stop -IncludeWorkers
```

Show the built-in help summary:

```bat
.\web.bat help
```

## Default Managed Startup Profile

`.\web.bat` and `.\web.bat start` both use the managed default profile. It starts:

- web service
- news worker
- news LLM worker

And it also:

- keeps analytics-history collectors off unless you explicitly pass `-EnableAnalyticsHistory`
- keeps the PM worker opt-in via `-StartPmWorker`
- ignores `.env` `START_*` worker flags for managed startup decisions
- defaults to `TRADING_MODE=live` with `MARKET_WS_MODE=strategy_primary`, fail-closed live reads, and the WS quality guard enabled
- keeps the AI autonomous agent separate from the default boot path

Keep `START_NEWS_WORKER`, `START_NEWS_LLM_WORKER`, and `START_PM_WORKER` unset in local `.env` for the managed path; use the `web.bat` flags above so `status` and startup behavior stay aligned.

Important behavior while the service is already running:

- `start` does not rewire the worker mix for an already-running service
- if you need a different worker profile, run `.\web.bat stop -IncludeWorkers` first, then start again with the flags you want

## Managed Trading Mode Rule

The managed path has one guarded live default and one explicit paper override:

- `.\web.bat` and `.\web.bat start` start in guarded `live + strategy_primary` mode and allow persisted live restore
- `.\web.bat start -PaperMode` starts in `paper` mode and disables market WS authority
- `-AllowPersistedLiveMode` remains as a compatibility alias for an explicit live request
- changing between `paper` and `live` requires a clean restart: stop first, then start with the command for the mode you want

Always confirm the effective mode with `.\web.bat status` after startup.

## AI Autonomous Agent Rule

The AI autonomous agent is intentionally separate from the default startup profile.

- `.\web.bat start` does not automatically start the autonomous agent
- the agent only auto-starts on service boot if `AI_AUTONOMOUS_AGENT_AUTO_START=true` is present in the launching environment
- saving runtime config in the UI does not change this boot rule by itself

If you want the service and the agent started together from the CLI, use:

```bat
.\web.bat start -StartAutonomousAgent
```

`.\web.bat status` shows both web status and autonomous-agent state when the service is reachable.

## Common Start Variants

Open the browser too:

```bat
.\web.bat start -OpenBrowser
```

Start intentionally in live mode:

```bat
.\web.bat start -AllowPersistedLiveMode
```

Start the explicit live shadow + news engine profile:

```bat
.\web.bat live-shadow-news -ConfirmLive -ResetNewsLlmFailover
```

This profile is the unambiguous operations entry for the current live setup. It restarts web plus the news worker and news LLM worker, sets live mode, forces `MARKET_WS_MODE=shadow`, sets `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true`, and runs the live-shadow market WS precheck. `-ResetNewsLlmFailover` clears the sticky news LLM failover state so NIM is tried first again.

Start web only without the news engine:

```bat
.\web.bat start -NoNewsWorkers
```

Start without the news LLM worker:

```bat
.\web.bat start -NoNewsLlmWorker
```

Start with analytics-history collectors enabled:

```bat
.\web.bat start -EnableAnalyticsHistory
```

Start with the PM worker:

```bat
.\web.bat start -StartPmWorker
```

Start with explicit worker flags:

```bat
.\web.bat start -StartNewsWorker -StartNewsLlmWorker -StartPmWorker
```

Clean restart:

```bat
.\web.bat stop -IncludeWorkers
.\web.bat start
```

Clean live restart:

```bat
.\web.bat stop -IncludeWorkers
.\web.bat start -AllowPersistedLiveMode
```

## What `status` Should Tell You

After every startup, run:

```bat
.\web.bat status
```

Check these fields before doing anything sensitive:

- web `state`
- trading `mode`
- AI Agent `running/stopped`
- AI Agent `mode`
- AI Agent `symbol_mode`
- observed worker state for news, LLM, and PM workers

Default managed restarts come up in guarded `live + strategy_primary`. Use `.\web.bat start -PaperMode` for paper. Treat any `mode=live` status as real state before changing strategies or credentials.

## Troubleshooting

If startup looks stuck:

1. Run `.\web.bat status`
2. Check whether the web service is listening but health is not ready yet
3. Check the startup transcript at `logs\web_ps.log`
4. If needed, run `.\web.bat stop -IncludeWorkers`
5. Start again with `.\web.bat start`

If the service is already running but the worker mix is wrong:

1. Run `.\web.bat stop -IncludeWorkers`
2. Start again with the flags you actually want

If the service should not be in `live`:

1. Treat that as real state, not a display bug
2. Review the persisted runtime mode and credentials
3. Run `.\web.bat stop -IncludeWorkers`
4. Start with the default managed command: `.\web.bat start`

If the service should be in `live`:

1. Run `.\web.bat stop -IncludeWorkers`
2. Start with the explicit live command: `.\web.bat start -AllowPersistedLiveMode`
3. Run `.\web.bat status` and confirm `mode=live`

## Script Stack

The startup chain is layered like this:

- `web.bat`: the only user-facing entry; no args mean one-click startup with browser
- `scripts\web.ps1`: command router for `help`, `start`, `status`, `stop`, and `live-shadow-news`
- `scripts\start_web_ps.ps1`: transcript wrapper that writes `logs\web_ps.log`
- `_once.ps1`: low-level launcher that boots web and optional workers, waits for readiness, and can optionally start the autonomous agent through the API
- `scripts\supervise_web.ps1`: watchdog loop that restarts web/workers when they die or stop answering `/livez`
- `scripts\ensure_web_supervisor_task.ps1`: registers the `CryptoTradingSystem_WebSupervisor_<port>` scheduled task that hosts the watchdog

## Web Supervisor Lifetime Rule

The supervisor runs as the scheduled task `CryptoTradingSystem_WebSupervisor_8000`, started on demand by managed startup. This keeps it parented to the Task Scheduler service instead of the console or app that launched `web.bat`.

Why this matters: console hosts (Windows Terminal, IDE and agent terminals) wrap child processes in a kill-on-close job object. Before this rule, closing or auto-updating the host app silently killed web, the workers, and the supervisor in one sweep — with no crash log and nothing left to restart the stack (observed 2026-07-16 19:44, dashboard stuck on "状态延迟").

Operational consequences:

- the stack now survives the launching console or app closing; if web dies with the host, the task-hosted supervisor restarts it within seconds
- the task has no time triggers: after a reboot, starting the system is still an explicit operator action (`.\web.bat start`), because a managed start can restore LIVE mode
- `.\web.bat status` shows the supervisor process and the task state; `.\web.bat stop` still stops the supervisor first via the stop marker so an operator stop is never treated as a crash
- if the scheduled-task launch fails, startup falls back to the old session-bound supervisor and prints a yellow warning that it will die with the console

Market WS evidence scripts such as `scripts\market_ws_live_shadow.ps1` are not the normal operations startup path. They intentionally disable unrelated background workers, including news, to collect cleaner WS evidence. Use `.\web.bat live-shadow-news -ConfirmLive -ResetNewsLlmFailover` when you want live shadow WS and the news engine running together.

## URLs And Logs

Common local URLs:

- dashboard: `http://127.0.0.1:8000`
- news: `http://127.0.0.1:8000/news`
- docs: `http://127.0.0.1:8000/docs`
- autonomous-agent status: `http://127.0.0.1:8000/api/ai/autonomous-agent/status`

Useful local files:

- startup transcript: `logs\web_ps.log`
- runtime files: `logs/` and `runtime/`

Clean empty logs:

```powershell
.\scripts\clean_empty_logs.ps1
```
