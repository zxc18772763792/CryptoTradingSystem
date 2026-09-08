# Repository Agent Notes

Read this before running anything. The traps below have all cost real debugging time.

---

## 1. Use the project interpreter — never bare `python`

`python` on PATH is **Python 3.13 with none of this project's dependencies**. It will
fail with `No module named pytest` and mislead you into thinking the repo is broken.

The project interpreter lives **outside** the repo, one level up:

```
F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe     (Python 3.11)
```

Bash:

```bash
PY="F:/9_Crypto/.conda/miniforge3/envs/crypto_trading/python.exe"
"$PY" -m pytest tests/test_foo.py -q
```

PowerShell:

```powershell
$py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
& $py -m pytest tests\test_foo.py -q
```

If that path does not exist, run `.\scripts\setup_local_env.ps1` — do not fall back to
system Python, and do not create a new environment somewhere else.

## 2. Verify the environment before concluding a dependency is missing

Environment state has been observed to change mid-session. **Do not carry a stale
reading forward** — re-check immediately before you act on it:

```bash
"$PY" scripts/verify_env.py          # exit 1 if a REQUIRED module is missing
"$PY" scripts/verify_env.py --strict # exit 1 if any optional one is missing too
```

or, as part of environment setup, `.\scripts\setup_local_env.ps1 -SkipRequirements`.

It separates **required** modules (unguarded top-level imports on the startup path —
missing means the app cannot boot) from **optional** ones (missing means a feature is
silently degraded). Prefer it over ad-hoc `import x` probes.

## 3. The native stack is conda-managed — install natives with conda, not pip

`environment.yml` pins `numpy=1.26.*`, `pandas=2.3.*`, `pyarrow=24.0.*` from
conda-forge on purpose: mixing a pip PyArrow wheel with conda's MSVC runtime caused
**repeatable access violations** in the long-running service.

So for native packages (matplotlib, ta-lib, polars, asyncpg, scikit-learn, xgboost):

```bash
conda install -n crypto_trading -c conda-forge <pkg> "numpy=1.26.*" "pandas=2.3.*" "pyarrow=24.0.*"
```

Re-stating the three pins holds the ABI steady while the solver works. Pure-Python
packages (jinja2, web3, requests…) are fine via `pip`.

## 4. Running the app

```powershell
.\web.bat              # start web + news worker + news LLM worker, open browser
.\web.bat start        # same, no browser
.\web.bat status       # service + worker status
.\web.bat stop -IncludeWorkers
.\web.bat help
```

Defaults to **paper** mode. LIVE requires an explicit `-AllowPersistedLiveMode`.
Full detail is in [STARTUP.md](STARTUP.md); don't duplicate it here.

Do not start `uvicorn` by hand — the supervisor owns process lifecycle, and a
hand-started server bypasses it and strands pid files in `runtime/`.

## 5. Running tests

```bash
"$PY" -m pytest -q                       # full suite
"$PY" -m pytest tests/test_foo.py -q     # single file
"$PY" -m pytest -q -m "not slow and not live"
```

Notes that will save you a wrong diagnosis:

- `pytest.ini` sets `filterwarnings = error::DeprecationWarning`. A dependency that
  starts emitting a `DeprecationWarning` **at import time** therefore becomes a
  *collection* error, and pytest aborts the entire run with
  `Interrupted: N errors during collection` — **zero tests execute**. This is a config
  symptom, not a code regression. The fix is an `ignore::DeprecationWarning:<pkg>.*`
  line alongside the existing ones, not a code change.
- `timeout = 120` is a hang-catcher, not a perf gate. Heavy integration tests
  legitimately take tens of seconds. pytest-timeout `os._exit()`s the whole process on
  a single overrun, so do not lower it casually.
- Markers: `slow`, `live` (needs exchange connectivity), `integration`.

## 6. Optional dependencies degrade silently — check before trusting a signal

Missing optional deps do **not** stop the system; they quietly reduce it:

| Missing | Consequence |
|---|---|
| `xgboost` / `scikit-learn` | ML signal returns **FLAT**, ML weight → 0, aggregator keeps trading |
| `web3` | DEX connectors unavailable |
| `matplotlib` | analysis/report scripts fail at import |
| `polars` / `asyncpg` | fast dataframe / PostgreSQL paths unavailable |

The ML case announces itself with a single `WARNING` line at startup
(`core/ai/ml_signal.py`). If you are debugging "why is the model not firing",
check this table **first**.

`talib` / `pandas_ta` are **intentionally not installed and not declared** — nothing
in the repo imports them, and ta-lib's C library dependency made clean installs
painful. Do not "helpfully" add them back; see the note in `requirements.txt`.

## 7. Repository map

```
core/          engine. trading/ execution/ risk/ (money path), ai/ ml/ research/,
               data/ marketdata/ realtime/, news/, governance/ (RBAC), audit/,
               monitoring/ observability/, backtest/, exchanges/ exchange_adapters/
web/           FastAPI app. main.py = entrypoint; api/ = 252 routes; static/, templates/
strategies/    strategy implementations
scripts/       operational + analysis scripts; setup_local_env.ps1, web.ps1 live here
tests/         ~2280 tests
config/        settings + env plumbing
docs/          architecture notes
runtime/       pid/state files (gitignored) — stale pids here mean a hard kill
logs/          uvicorn_web_*.out.log / .err.log, web_supervisor.log
```

Largest modules, if you are looking for where logic lives (or what needs splitting):
`web/api/trading.py` (~9.9k lines), `web/api/data.py` (~7.2k),
`core/trading/execution_engine.py` (~6.9k).

## 8. Git — read before any push

- **The `origin` remote is a PUBLIC GitHub repository.** Treat every commit as
  world-readable and permanently archived. Never commit credentials, account
  identifiers, real balances, personal details, or anything from `.env` / `keys.txt`.
- **Do not push, force-push, or rewrite history unless the user explicitly asks in
  this session.** The local branch is far ahead of the remote by design.
- `.gitignore` already covers `.env*`, `keys.txt`, `*.pem`, `*.key`, `runtime/`,
  `logs/`, `data/`, `output/`. Do not weaken these.
- `F:\9_Crypto\_radar_refactor\` is **not** an isolated worktree and may contain
  another session's changes. In the parent repo, never `git add -A` / `git add .` —
  stage explicit paths only.
- Generated artifacts under `reports/` (`.parquet`, `.csv`, `.bak`) are currently
  untracked and should stay that way.

## 9. Secrets

Real keys live in `.env` / `.env.local` / `keys.txt` — all gitignored, none ever
committed (verified across full history). `.env.example` documents the ~166 settings
with placeholders only; when adding a setting, add it there **with a placeholder**,
never with a real value.

## 10. Playwright

Available through the bundled Codex runtime — check before reporting it missing:
`playwright@1.62.1` under
`C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\node_modules`.
There is no root `package.json` or local `node_modules`. The smoke test imports
`@playwright/test` while the bundled runtime exposes `playwright`; treat that runner
import mismatch as a project configuration issue, not an environment absence.
