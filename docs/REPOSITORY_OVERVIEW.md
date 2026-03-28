# Repository Overview

This document explains how the repository is organized today and which directories are intended for source control versus local runtime state.

## Top-Level Map

- `main.py`
  Entry point for `web` and `cli` modes. It builds runtime configuration, creates local directories, configures logging, and starts the FastAPI app or CLI loop.

- `config/`
  Settings, database models, exchange configuration, and registry-style metadata. Start here when a new feature needs environment variables or schema changes.

- `core/`
  The main application logic:
  - trading and execution
  - risk management
  - AI review and autonomous-agent workflows
  - news ingestion and enrichment
  - research orchestration
  - runtime state, monitoring, and ops services

- `web/`
  FastAPI application wiring, HTML templates, static assets, and user-facing API routes.

- `strategies/`
  Strategy implementations grouped by theme such as technical, quantitative, arbitrage, macro, and AI-assisted approaches.

- `prediction_markets/`
  Optional prediction-market integration and related data/worker logic.

- `scripts/`
  Operational helpers such as startup wrappers, research launches, self-checks, data downloads, and one-off utilities.

- `tests/`
  Automated coverage for web routes, ops behavior, governance flows, prediction-market logic, and strategy/runtime integration points.

- `docs/`
  Longer-form reference material including architecture notes, governance docs, integration plans, and the changelog.

## Key Entry Points

- `start_web_oneclick.bat`
  Simplest Windows-friendly way to start the web application.

- `scripts/start_web_ps.ps1`
  PowerShell launcher used by the batch wrapper and useful for passing worker flags.

- `web/main.py`
  FastAPI application startup, background task registration, route mounting, websocket wiring, and runtime snapshot publication.

- `config/settings.py`
  Central definition of environment-driven application settings.

## Runtime State That Should Stay Local

The following directories are part of local operation and should not be committed:

- `data/`
  Downloaded market data, caches, SQLite files, and research artifacts.

- `logs/`
  Startup transcripts, runtime logs, and operational traces.

- `runtime/`
  Temporary runtime outputs and service process logs.

- `output/`
  Generated outputs and local worker results.

- `.playwright-cli/`, `test-results/`, `.pytest_tmp/`
  Browser automation artifacts and temporary test state.

## Suggested Reading Order

If you are new to the codebase, this sequence works well:

1. `README.md`
2. `STARTUP.md`
3. `config/settings.py`
4. `main.py`
5. `web/main.py`
6. Relevant module under `core/`
7. Matching tests under `tests/`

## GitHub Publishing Notes

- Track source code, templates, scripts, and documentation.
- Do not publish local credentials or runtime evidence.
- Keep `.env.example` as a template, not a copy of a real workstation.
- If you need to share reproduction data, prefer sanitized fixtures under a tracked test or docs path instead of copying raw runtime files.
