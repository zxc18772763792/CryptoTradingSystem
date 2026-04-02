# Startup Quick Reference

If you only remember one command in a new session, remember this:

```bat
.\web.bat start
```

This file is the single reference for day-to-day startup, status, restart, and shutdown.

## One-Click Entry

Preferred command from project root:

```bat
.\web.bat start
```

Compatibility alias:

```bat
.\start_web_oneclick.bat
```

Both commands lead to the same managed startup flow. The project should now be treated as having one canonical start family: `.\web.bat ...`.

## What Default Start Does

`.\web.bat start` is the managed default. By default it:

- starts the web service
- auto-starts the news pull worker
- auto-starts the news LLM worker
- keeps the news page in automatic background mode instead of requiring manual kickoff
- ignores `.env` auto-start worker flags such as `START_NEWS_WORKER=1` and `START_NEWS_LLM_WORKER=1`
- keeps analytics-history collectors off unless you explicitly opt in
- leaves the PM worker opt-in
- restores the persisted trading mode, so the service may still come up in `live`

## Daily Commands

Run these from project root:

```bat
.\web.bat help
.\web.bat start
.\web.bat start -OpenBrowser
.\web.bat start -NoNewsWorkers
.\web.bat start -NoNewsLlmWorker
.\web.bat start -EnableAnalyticsHistory
.\web.bat start -StartPmWorker
.\web.bat status
.\web.bat stop -IncludeWorkers
```

## Most Common Flows

Start everything recommended for daily use:

```bat
.\web.bat start
```

Start and open the browser:

```bat
.\web.bat start -OpenBrowser
```

Start web only without the news engine:

```bat
.\web.bat start -NoNewsWorkers
```

Start with the news pull worker but without the news LLM worker:

```bat
.\web.bat start -NoNewsLlmWorker
```

Start with analytics-history collectors enabled:

```bat
.\web.bat start -EnableAnalyticsHistory
```

Start with the PM worker too:

```bat
.\web.bat start -StartPmWorker
```

Check the current running state:

```bat
.\web.bat status
```

Do a clean restart:

```bat
.\web.bat stop -IncludeWorkers
.\web.bat start
```

## Script Stack

The startup chain is intentionally layered:

- `web.bat`: primary user entry point
- `start_web_oneclick.bat`: compatibility alias that forwards to `web.bat start`
- `scripts\web.ps1`: command router for help, start, status, and stop
- `scripts\start_web_ps.ps1`: startup transcript wrapper that writes `logs\web_ps.log`
- `_once.ps1`: low-level launcher that actually boots web and external workers

When editing startup behavior, treat `web.bat` plus `scripts\web.ps1` as the user-facing contract.

## Operational Rules

- Use `.\web.bat start` as the default entry point.
- Use `.\web.bat status` after startup to verify health, URLs, and trading mode.
- Use `.\web.bat stop -IncludeWorkers` before restart when you want a clean reset.
- Add `-EnableAnalyticsHistory` only when you intentionally want history collectors running.
- Add `-NoNewsWorkers` only when you intentionally want a web-only session.
- Do not rely on `.env` `START_*` worker flags to control managed startup behavior.

## Mode Warning

- `.\web.bat start` does not force `paper` mode.
- The application may restore the last persisted account mode during startup.
- Always check `.\web.bat status` after startup and look for the `mode=` or live warning before taking any trading action.

## URLs

After startup, common local pages are:

- dashboard: `http://127.0.0.1:8000`
- news: `http://127.0.0.1:8000/news`
- docs: `http://127.0.0.1:8000/docs`

## Logs

- startup transcript: `logs/web_ps.log`
- runtime files: `logs/` and `runtime/`

Clean empty logs:

```powershell
.\scripts\clean_empty_logs.ps1
```
