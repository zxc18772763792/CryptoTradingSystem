# Startup Quick Reference

This repository now has one canonical control entry, one convenience alias, and one deprecated wrapper:

```bat
.\web.bat help
.\web.bat start
.\start_web_oneclick.bat
```

## Which Command To Use

| Use case | Command | Notes |
| --- | --- | --- |
| Canonical control entry | `.\web.bat ...` | Use this for `help`, `start`, `status`, and `stop`. |
| Daily one-click start | `.\start_web_oneclick.bat` | Equivalent to `.\web.bat start -OpenBrowser`. |
| Deprecated compatibility wrapper | `.\start_once.bat` | Still works, but only forwards to `start_web_oneclick.bat`. |

If you only remember one entry point, remember `.\web.bat`.

## The Commands To Remember

Daily managed start:

```bat
.\web.bat start
```

Daily one-click start with browser:

```bat
.\start_web_oneclick.bat
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

`.\web.bat start` is the managed default. It starts:

- web service
- news worker
- news LLM worker

And it also:

- keeps analytics-history collectors off unless you explicitly pass `-EnableAnalyticsHistory`
- keeps the PM worker opt-in via `-StartPmWorker`
- ignores `.env` `START_*` worker flags for managed startup decisions
- restores the persisted trading mode, so the service can still come up in `live`
- keeps the AI autonomous agent separate from the default boot path

Important behavior while the service is already running:

- `start` does not rewire the worker mix for an already-running service
- if you need a different worker profile, run `.\web.bat stop -IncludeWorkers` first, then start again with the flags you want

## AI Autonomous Agent Rule

The AI autonomous agent is intentionally separate from the default startup profile.

- `.\web.bat start` does not automatically start the autonomous agent
- the agent only auto-starts on service boot if `AI_AUTONOMOUS_AGENT_AUTO_START=true` is present in the launching environment
- saving runtime config in the UI does not change this boot rule by itself

If you want the service and the agent started together from the CLI, use:

```bat
.\web.bat start -StartAutonomousAgent
```

If you want the browser too:

```bat
.\start_web_oneclick.bat -StartAutonomousAgent
```

`.\web.bat status` shows both web status and autonomous-agent state when the service is reachable.

## Common Start Variants

Open the browser too:

```bat
.\web.bat start -OpenBrowser
```

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

If the service comes up in `live`, treat that as an explicit warning, not as a harmless default.

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

If the service comes up in `live` unexpectedly:

1. Treat that as real state, not a display bug
2. Review the persisted runtime mode and credentials
3. Do not assume default startup forces `paper`

## Script Stack

The startup chain is layered like this:

- `web.bat`: canonical user entry
- `scripts\web.ps1`: command router for `help`, `start`, `status`, and `stop`
- `scripts\start_web_ps.ps1`: transcript wrapper that writes `logs\web_ps.log`
- `_once.ps1`: low-level launcher that boots web and optional workers, waits for readiness, and can optionally start the autonomous agent through the API

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
