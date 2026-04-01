# CryptoTradingSystem

CryptoTradingSystem is a FastAPI-based crypto trading and research workspace for paper trading, guarded live trading, AI-assisted decision review, news ingestion, and quantitative strategy iteration.

The current repository is organized for local development first: source code and documentation are tracked, while runtime data, logs, databases, caches, and real credentials stay out of Git.

## What This Repository Includes

- Web dashboard and REST API for trading, research, data download, and monitoring
- Paper and guarded live trading workflows, with explicit guard rails around live execution
- AI research and autonomous-agent workflows with configurable providers
- News collection, enrichment, and LLM-assisted triage
- Backtesting, factor research, and pairs/relative-value strategy tooling
- Optional Ops and OpenClaw integration for operational control flows
- Optional prediction-market workflows under `prediction_markets/`

## Repository Layout

- `config/`: settings, database models, exchange definitions, and registries
- `core/`: trading engine, AI modules, data pipelines, risk controls, news services, and runtime orchestration
- `web/`: FastAPI app, API routes, templates, and static assets
- `strategies/`: strategy implementations grouped by style
- `scripts/`: startup helpers, self-checks, data download, and research utilities
- `tests/`: web, ops, governance, and prediction-market coverage
- `docs/`: architecture notes, governance docs, integration plans, and changelog
- `prediction_markets/`: optional prediction-market integrations and workers

See [docs/REPOSITORY_OVERVIEW.md](docs/REPOSITORY_OVERVIEW.md) for a more detailed guide.

## Quick Start

### 1. Create a Python environment

```powershell
conda create -n crypto_trading python=3.11 -y
conda activate crypto_trading
pip install -r requirements.txt
```

### 2. Configure local secrets

Create a local `.env` from the tracked template:

```powershell
Copy-Item .env.example .env
```

Fill in only the credentials you actually use. Keep real API keys, broker secrets, and tokens in `.env` or your shell environment. Do not commit them to Git.

### 3. Start the web application

Use the canonical root command:

```bat
.\web.bat start
```

`.\web.bat start` is the recommended safe start path. By default it starts the web app only, ignores `.env` worker auto-start flags, and keeps analytics-history collectors off unless you explicitly opt in.

Useful variants:

```bat
.\web.bat start -OpenBrowser
.\web.bat start -EnableAnalyticsHistory
.\web.bat start -StartNewsWorker -StartNewsLlmWorker
.\web.bat status
.\web.bat stop -IncludeWorkers
```

Legacy wrappers still work, but `.\web.bat ...` is now the command family to remember.

After startup, always verify the runtime mode with:

```bat
.\web.bat status
```

The service may restore the persisted account mode on boot, so a safe start can still come up in `live` if that was the last saved mode.

Open:

- Dashboard: `http://127.0.0.1:8000`
- News page: `http://127.0.0.1:8000/news`
- FastAPI docs: `http://127.0.0.1:8000/docs`

### 4. Switch modes carefully

Direct CLI startup can force paper mode:

```powershell
python main.py --mode web --trading-mode paper
```

Live mode should only be used after local validation, explicit approvals, and a full credential review:

```powershell
python main.py --mode web --trading-mode live
```

## Common Commands

Run the web service:

```bat
.\web.bat start
```

Run startup helper with optional collectors or workers:

```bat
.\web.bat start -EnableAnalyticsHistory
.\web.bat start -StartNewsWorker -StartNewsLlmWorker -StartPmWorker
```

Run focused tests:

```powershell
pytest -q tests/ops tests/polymarket tests/web
```

Run the Polymarket worker once:

```powershell
python scripts/run_polymarket_worker.py
```

Run the OpenClaw/Ops self-check:

```powershell
python scripts/selfcheck_openclaw_ops.py --base-url http://127.0.0.1:8000/ops
```

## Secret Handling

- `.env`, `keys.txt`, local certificate files, and `config/*_api_key.txt` are intentionally ignored by Git
- `.env.example` must remain a placeholder-only template
- Runtime artifacts under `data/`, `logs/`, `runtime/`, and `output/` stay local and are not part of the repository history
- If a credential is ever exposed, rotate it immediately before pushing or sharing the repository

See [SECURITY.md](SECURITY.md) for the full pre-push checklist and incident response guidance.

## Documentation

- [STARTUP.md](STARTUP.md): single startup/status/stop reference for new sessions
- [SECURITY.md](SECURITY.md): secret management and safe sharing rules
- [docs/REPOSITORY_OVERVIEW.md](docs/REPOSITORY_OVERVIEW.md): directory-by-directory repository guide
- [docs/GOVERNANCE.md](docs/GOVERNANCE.md): governance model and approval flows
- [docs/INTEGRATION.md](docs/INTEGRATION.md): system integration notes
- [docs/CHANGELOG.md](docs/CHANGELOG.md): tracked repository milestones

## Git Hygiene

Before pushing to GitHub:

1. Verify `git status` is clean except for the files you intend to publish.
2. Confirm `.env`, `keys.txt`, local DB files, logs, and caches are not staged.
3. Review `git diff --cached` for accidental tokens, webhook URLs, or account identifiers.
4. Push only the project repository under `crypto_trading_system/`, not outer workspace scratch files.
