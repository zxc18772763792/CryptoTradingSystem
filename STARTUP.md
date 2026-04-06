# Startup Quick Reference

This project now has one control entry and one convenience alias:

```bat
.\web.bat help
.\web.bat start
.\start_web_oneclick.bat
```

- `.\web.bat ...` is the canonical control surface for help, start, status, and stop.
- `.\start_web_oneclick.bat` is the daily one-click launcher. It starts the managed default profile and opens the browser for you.

## The Commands To Remember

Daily default start:

```bat
.\web.bat start
```

Daily one-click start with browser:

```bat
.\start_web_oneclick.bat
```

Check what is really running:

```bat
.\web.bat status
```

Stop web and observed workers cleanly:

```bat
.\web.bat stop -IncludeWorkers
```

## Default Startup Behavior

`.\web.bat start` is the managed default. It starts:

- web service
- news worker
- news LLM worker

And it also:

- keeps analytics-history collectors off unless you explicitly opt in
- keeps the PM worker opt-in
- ignores `.env` `START_*` worker flags for managed startup decisions
- restores the persisted trading mode, so the service can still come up in `live`

## One-Click Script Roles

- `web.bat`
  The canonical control entry. Use this for normal operations and all scripted calls.
- `start_web_oneclick.bat`
  Convenience launcher for interactive use. It is equivalent to:

  ```bat
  .\web.bat start -OpenBrowser
  ```

- `start_once.bat`
  Deprecated wrapper kept only for compatibility. It forwards to `start_web_oneclick.bat`.

## AI Autonomous Agent Rule

The AI autonomous agent is intentionally separate from the default startup profile.

- `.\web.bat start` does not automatically start the autonomous agent.
- The agent only starts automatically on service boot if `AI_AUTONOMOUS_AGENT_AUTO_START=true` is set in the launching environment.
- Saving runtime config in the UI does not change this boot rule by itself.

If you want the service and the agent started together from the CLI, use:

```bat
.\web.bat start -StartAutonomousAgent
```

If you want the browser too:

```bat
.\start_web_oneclick.bat -StartAutonomousAgent
```

`.\web.bat status` now shows both web status and the current autonomous-agent state when the service is reachable.

## Most Common Flows

Standard managed start:

```bat
.\web.bat start
```

Start and open the browser:

```bat
.\web.bat start -OpenBrowser
```

Start web plus autonomous agent:

```bat
.\web.bat start -StartAutonomousAgent
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

If the service comes up in `live`, treat that as an explicit warning, not as a harmless default.

## Script Stack

The startup chain is layered like this:

- `web.bat`: canonical user entry
- `scripts\web.ps1`: command router for help, start, status, and stop
- `scripts\start_web_ps.ps1`: transcript wrapper that writes `logs\web_ps.log`
- `_once.ps1`: low-level launcher that boots web and optional workers, then can optionally start the autonomous agent through the API

## URLs And Logs

Common local URLs:

- dashboard: `http://127.0.0.1:8000`
- news: `http://127.0.0.1:8000/news`
- docs: `http://127.0.0.1:8000/docs`
- autonomous-agent status: `http://127.0.0.1:8000/api/ai/autonomous-agent/status`

Useful local files:

- startup transcript: `logs/web_ps.log`
- runtime files: `logs/` and `runtime/`

Clean empty logs:

```powershell
.\scripts\clean_empty_logs.ps1
```
