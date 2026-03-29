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
.\web.bat status
.\web.bat stop -IncludeWorkers
```

## Most Common Uses

Start the web service:

```bat
.\web.bat start
```

Start and open the browser:

```bat
.\web.bat start -OpenBrowser
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
- Use `.\web.bat status` before starting if you are unsure whether something is already running.
- Use `.\web.bat stop -IncludeWorkers` before restarting if you want a clean reset.

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
