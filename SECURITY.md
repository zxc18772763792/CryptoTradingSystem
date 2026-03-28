# Security and Secret Handling

This repository is meant to be shareable without exposing real exchange credentials, API tokens, local databases, or runtime logs.

## Rules

- Keep real credentials in `.env` or environment variables, never in tracked source files
- Keep `.env.example` placeholder-only
- Do not commit `keys.txt`, `config/*_api_key.txt`, certificate/private-key files, or ad-hoc secret exports
- Treat runtime logs and databases as sensitive because they may contain account metadata, request payloads, or operational traces

## Local-Only Files and Paths

These locations are intentionally excluded from Git:

- `.env`
- `.env.*` except `.env.example`
- `keys.txt`
- `config/*_api_key.txt`
- `config/secrets/`
- `data/`
- `logs/`
- `runtime/`
- `output/`
- `.playwright-cli/`
- `.pytest_tmp/`
- `node_modules/`
- `.venv/`

## Recommended Setup

1. Copy `.env.example` to `.env`.
2. Fill only the variables you need for your local workflows.
3. Prefer paper trading until your exchange credentials, approval flow, and kill-switch behavior are verified.
4. If you use OpenAI-compatible gateways or internal proxies, set them in `.env` instead of hard-coding them into tracked files.

## Pre-Push Checklist

Run these checks before every push:

```powershell
git status --short
git diff --cached
git ls-files .env keys.txt
git grep -nI -E "(API_KEY|API_SECRET|TOKEN|PASSWORD|PASSPHRASE)" HEAD
```

Expected outcomes:

- `.env` and `keys.txt` should not appear in `git ls-files`
- `git diff --cached` should not contain real credentials
- `git grep` may return variable names or placeholders, but not non-empty secrets

## If a Secret Was Added by Mistake

1. Remove it from the working tree or replace it with a placeholder.
2. Rewrite unpublished history before pushing.
3. If it was already pushed, rotate the credential immediately.
4. Remove the secret from Git history using a history-rewrite tool before continuing to share the repository.
5. Re-check all related logs, screenshots, and copied config files.

## Notes for This Repository

- Live trading is intentionally guarded and should not be enabled casually.
- Ops tokens, exchange API keys, webhook secrets, and email credentials should all be treated as production secrets.
- Runtime artifacts are kept local on purpose; they are part of operation, not source control.
