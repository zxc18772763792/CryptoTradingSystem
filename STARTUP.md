# Startup Quick Reference

If you only remember one command in a new session, remember this:

```bat
.\web.bat start
```

This file is the single reference for day-to-day web service control.

## Canonical Commands

Run these from project root:

```bat
.\web.bat help
.\web.bat start
.\web.bat start -NoNewsWorkers
.\web.bat start -NoNewsLlmWorker
.\web.bat start -EnableAnalyticsHistory
.\web.bat status
.\web.bat stop -IncludeWorkers
```

## Most Common Uses

Start the web service:

```bat
.\web.bat start
```

This is the recommended default start path. By default it:

- starts the web service plus the news worker and news LLM worker
- ignores `.env` auto-start worker flags such as `START_NEWS_WORKER=1`
- disables analytics-history background collectors unless you explicitly opt in
- keeps persisted trading-mode restore behavior, so the service may still come up in `live`

Start and open the browser:

```bat
.\web.bat start -OpenBrowser
```

Start with analytics-history collectors enabled:

```bat
.\web.bat start -EnableAnalyticsHistory
```

Start web only without the news engine:

```bat
.\web.bat start -NoNewsWorkers
```

Start with the pull worker but without the LLM worker:

```bat
.\web.bat start -NoNewsLlmWorker
```

Start with news workers:

```bat
.\web.bat start -StartNewsWorker -StartNewsLlmWorker
```

Start with all common workers:

```bat
.\web.bat start -StartNewsWorker -StartNewsLlmWorker -StartPmWorker
```

Check whether the service is already running:

```bat
.\web.bat status
```

Stop the web service and related workers:

```bat
.\web.bat stop -IncludeWorkers
```

## Daily Rule

- Use `.\web.bat start` as the default entry point.
- Treat `.\web.bat start` as the managed default: web + news engine, analytics-history off.
- Use `.\web.bat start -NoNewsWorkers` when you intentionally want a web-only session.
- Use `.\web.bat status` before starting if you are unsure whether something is already running.
- Use `.\web.bat status` after starting to confirm both health and trading mode.
- Use `.\web.bat stop -IncludeWorkers` before restarting if you want a clean reset.
- Add `-EnableAnalyticsHistory` only when you intentionally want the history collectors running.

## Mode Warning

- `.\web.bat start` does not currently force `paper` mode.
- The application may restore the last persisted account mode during startup.
- Always check `.\web.bat status` after startup and look for the `mode=` / live warning before taking any trading action.

## Advanced Or Legacy Entrypoints

These still exist, but they are no longer the primary commands to remember:

- `start_web_oneclick.bat`: compatibility wrapper that now forwards to `.\web.bat start`
- `start_once.bat`: deprecated compatibility wrapper
- `.\scripts\start_web_ps.ps1`: internal PowerShell startup entry
- `.\_once.ps1`: low-level launcher for direct control

## Logs

- Main startup transcript: `logs/web_ps.log`
- Runtime output files: `logs/` and `runtime/`
- Clean empty logs:

```powershell
.\scripts\clean_empty_logs.ps1
```
