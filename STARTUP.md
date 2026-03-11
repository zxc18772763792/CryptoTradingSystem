# Startup Commands

This file is the single reference for starting the web service.

## Recommended (one-click)

Use this from project root:

```bat
start_web_oneclick.bat
```

Pass-through examples:

```bat
start_web_oneclick.bat -Port 8000 -HealthWaitSec 150
start_web_oneclick.bat -StartNewsWorker -StartNewsLlmWorker -StartPmWorker
start_web_oneclick.bat -TestDataSources
```

## PowerShell entry

```powershell
.\scripts\start_web_ps.ps1
```

Examples:

```powershell
.\scripts\start_web_ps.ps1 -Port 8000 -OpenBrowser
.\scripts\start_web_ps.ps1 -StartNewsWorker -StartNewsLlmWorker
```

## Core launcher (advanced)

```powershell
.\_once.ps1
```

Use this only when you need direct control of low-level startup behavior.

## Compatibility wrapper

`start_once.bat` is kept for compatibility and now forwards to `start_web_oneclick.bat`.

## Logs

- Main startup transcript: `logs/web_ps.log`
- Runtime output files are under `logs/` and `runtime/`
- Clean empty logs:

```powershell
.\scripts\clean_empty_logs.ps1
```
