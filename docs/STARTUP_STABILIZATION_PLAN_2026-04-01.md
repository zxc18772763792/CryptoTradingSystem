# Startup Stabilization Plan

Date: 2026-04-01
Workspace: `E:\9_Crypto\crypto_trading_system`
Scope: Recover the project from "startup appears hung / service listening but health endpoints time out" to a stable, repeatable startup flow.

## Background

Recent investigation narrowed the problem to a compound failure rather than a single crash:

- Startup latency was inflated by exchange connection setup.
- The current startup path automatically launches external news workers from `.env`.
- The web process also had internal news background tasks enabled by default, creating duplicate work until guarded.
- Frontend AI and dashboard pages introduced much heavier polling and WebSocket activity after `origin/main`.
- News SQLite access showed pool exhaustion symptoms under load.
- Multiple open Chrome localhost tabs/windows amplify the request volume and can keep old JS behavior alive.

The GitHub baseline at `origin/main` is older and reportedly "busy but usable", so the safest recovery path is to treat `origin/main` as the behavioral baseline and reintroduce local changes only when they are proven not to break startup health.

## Recovery Goal

We are done when all of the following are true:

1. `.\web.bat start` reaches a healthy state within a predictable window.
2. `http://127.0.0.1:8000/health` responds consistently.
3. `http://127.0.0.1:8000/api/status` responds consistently.
4. Starting the web app does not accidentally create duplicate news execution paths.
5. Opening one dashboard tab does not flood localhost with avoidable requests.
6. The default startup mode is safe enough for debugging and verification.

## Team Structure

### Track A: Startup and Process Control

Owner: startup-process lead

Focus:

- Compare `web.bat` / `scripts\web.ps1` / `_once.ps1` against `origin/main`.
- Verify how `.env` flags are converted into external worker launches.
- Fix worker stop/status detection if it is reporting incorrect state.
- Decide whether default startup should be "web only" unless explicitly opted into workers.

Primary files:

- `web.bat`
- `scripts/web.ps1`
- `_once.ps1`

Deliverables:

- A safe-start policy.
- Correct process detection for web/news/news-llm workers.
- One repeatable startup command for debugging.

### Track B: Frontend Load Shedding

Owner: frontend-load lead

Focus:

- Compare current UI initialization against `origin/main`.
- Identify everything that initializes AI research / AI agent modules before the related tab is visible.
- Remove or defer dashboard polling that is not needed for first paint.
- Make stale tabs and hidden tabs go quiet.

Primary files:

- `web/templates/index.html`
- `web/static/js/app.js`
- `web/static/js/ai_research.js`
- `web/static/js/ai_research_diagnostics.js`
- `web/static/js/dashboard_news.js`
- `web/static/js/dashboard_unstructured_news.js`

Deliverables:

- Lazy activation for AI pages.
- Reduced dashboard/news polling.
- Fewer persistent localhost connections from Chrome.

### Track C: Runtime Health and DB Pressure

Owner: runtime-health lead

Focus:

- Verify why trivial health endpoints still time out while the process is responding.
- Confirm whether the event loop is being starved by background tasks or connection churn.
- Reduce SQLite contention for news APIs and worker-status paths.
- Compare current runtime background task graph to the `origin/main` baseline.

Deliverables:

- Clear explanation for the remaining `/health` and `/api/status` timeout behavior.
- A prioritized fix list for runtime pressure.
- Validation that news DB access no longer collapses under request bursts.

### Track D: Baseline Verification

Owner: baseline-verification lead

Focus:

- Treat `origin/main` as the behavioral baseline, not just a code diff.
- Confirm which current regressions do not exist on `origin/main`.
- Re-test after each fix using one clean browser session and one controlled startup path.

Primary files:

- `web/templates/index.html`
- `web/static/js/app.js`
- `web/static/js/ai_research.js`
- `web/static/js/ai_research_diagnostics.js`
- `scripts/web.ps1`
- `_once.ps1`

Deliverables:

- A minimal list of regressions introduced after `origin/main`.
- A per-fix verification log.
- A go/no-go checklist for re-enabling workers and heavier UI surfaces.

## Active Work Board

### Track A: Current Questions

- `_once.ps1` currently reads `START_NEWS_WORKER` and `START_NEWS_LLM_WORKER`, then launches external worker processes via `Start-Process`.
- `scripts/web.ps1` status reporting needs to stay aligned with the actual managed process graph; this has already shown false "stopped" readings while workers were still alive.
- The startup path needs an explicit answer to one question: should `.\web.bat start` mean "safe web-only debug start" unless workers are explicitly requested?

Immediate next checks:

1. Compare `web.bat`, `scripts/web.ps1`, and `_once.ps1` to `origin/main`.
2. Fix stop/status detection before making startup behavior safer; otherwise verification remains ambiguous.
3. Add a deliberate startup policy: web-only by default, workers opt-in.

### Track B: Frontend Findings From Current Diff

Most suspicious regressions after `origin/main`:

1. `web/static/js/app.js` now wires AI refresh into global tab loading and global periodic refresh paths:
   - `loadAiResearchTabData()` at line 541
   - `loadAiAgentTabData()` at line 544
   - `refreshAiResearchModules()` at line 3607
   - global interval hooks at lines 5675-5700
2. `web/static/js/ai_research_diagnostics.js` adds a 30 second diagnostics loop and event-driven refreshes on `ai-research:state`, which fans out to:
   - `/news/summary`
   - `/news/pull_status`
   - `/news/worker_status`
   - `/trading/analytics/microstructure`
   - `/trading/analytics/community/overview`
3. `web/static/js/ai_research.js` keeps its own workbench refresh timers and proposal job polling, so the AI page can stack periodic refreshes on top of the shared app refresh path.
4. `web/templates/index.html` now has a much larger AI surface, including the `ai-agent` tab, while still loading AI scripts globally.

Already mitigated locally:

1. `web/static/js/dashboard_news.js` now checks dashboard visibility before polling.
2. `web/static/js/dashboard_unstructured_news.js` now disconnects or avoids keeping its extra WebSocket alive when the dashboard is hidden.
3. `web/static/js/ai_research.js` now only does initial `refreshWorkbench()` when the AI research tab is active.
4. `web/static/js/app.js` already stretches some dashboard/trading intervals when the main WebSocket is connected.

Still unfixed:

1. `web/static/js/app.js` still refreshes AI modules from the global 8 second and 10 second intervals at lines 5682-5683 and 5698-5699.
2. `web/static/js/app.js` still routes AI research and AI agent through one shared `refreshAiResearchModules()` fan-out function at line 3607.
3. `web/static/js/ai_research_diagnostics.js` still reacts to `ai-research:state` events at line 288, which can burst on top of its timer.
4. `web/static/js/ai_research.js` and `web/static/js/ai_research_diagnostics.js` still duplicate requests for overlapping market/news diagnostics.

Smallest effective next frontend changes:

1. Remove the AI branches from the two global `setInterval` blocks in `web/static/js/app.js` at lines 5675-5700.
2. Narrow `refreshAiResearchModules()` in `web/static/js/app.js` so AI research workbench refresh is not coupled to AI agent/runtime refresh.
3. Trim or debounce `ai-research:state`-triggered diagnostics refresh in `web/static/js/ai_research_diagnostics.js` at line 288.
4. Deduplicate overlapping diagnostics fetches between `web/static/js/ai_research.js` and `web/static/js/ai_research_diagnostics.js`.

### Track C: Current Questions

- `web/main.py` still schedules a large background task graph during lifespan startup and staggered follow-up tasks after startup.
- `web/api/news.py` already has short TTL caches around `/pull_status` and `/worker_status`, but those endpoints are still appearing heavily in the observed request mix.
- `core/news/storage/db.py` has already moved SQLite onto `NullPool`, so remaining health timeouts now need to be explained by event-loop pressure, background task contention, or frontend-driven request bursts.

Immediate next checks:

1. Profile what runs on the event loop after startup completes and before health endpoints respond reliably.
2. Check whether the request storm is enough to starve `/health` and `/api/status`, or whether a separate background task remains the dominant blocker.
3. Confirm whether worker-status and pull-status endpoints need a more defensive fast-path during heavy startup churn.

### Track D: Verification Protocol

1. Verify fixes with one clean browser session, not with stale localhost tabs.
2. Verify startup first with web-only mode, then reintroduce workers deliberately.
3. Keep `origin/main` as the baseline for "busy but usable"; every deviation from that behavior needs an explicit reason.

## Execution Order

### Phase 1: Stabilize the launch path

- Keep the startup path deterministic.
- Ensure only one web process and the intended workers run.
- Prefer web-only verification before adding optional workers back.

### Phase 2: Quiet the UI

- Make the default dashboard cheap enough to load.
- Ensure AI pages do not perform work when not active.
- Prevent hidden tabs from acting like active operators.

### Phase 3: Reduce backend contention

- Keep news DB calls short and tolerant of bursts.
- Avoid duplicate internal/external worker execution paths.
- Add caching or defensive fallbacks around expensive status/diagnostic endpoints where necessary.

### Phase 4: Re-verify against baseline

- Compare behavior with `origin/main`.
- Re-test startup with one clean browser session.
- Reintroduce optional workers only after health remains stable.

## Current Known Fixes Already Applied

- Exchange startup connection handling was shortened and parallelized.
- Execution engine signal queue was made event-loop safe.
- Dashboard AI/news polling was partially reduced.
- News SQLite engine now uses `NullPool` for sqlite.
- Internal web news tasks are now skipped when external `START_NEWS_WORKER` / `START_NEWS_LLM_WORKER` are enabled.

These changes still require final verification under a clean browser session and cleaner startup orchestration.

## Immediate Next Actions

1. Track A: lock down startup policy and worker detection in `web.bat`, `scripts/web.ps1`, and `_once.ps1`.
2. Track B: remove AI refresh from the global app intervals before making broader frontend changes.
3. Track C: trace why `/health` and `/api/status` still time out after the current DB and polling mitigations.
4. Track D: re-test each change with one clean browser session and a known process graph.
5. Only after all four tracks pass, re-enable optional workers or heavier AI surfaces.

## Team Kickoff

Current assignments:

- Startup-process lead: startup scripts, worker lifecycle, safe-start policy.
- Frontend-load lead: request storm and browser connection buildup.
- Runtime-health lead: health endpoint starvation and background task pressure.
- Baseline-verification lead: `origin/main` diff discipline and clean-run verification.

Working rules:

1. No broad refactors while the service is unstable.
2. Every proposal needs exact file/function references.
3. Verify one hypothesis at a time against a clean browser session.
4. Keep unrelated local changes intact.

## Acceptance Checklist

- `.\web.bat start` can reach healthy status on a clean run.
- `.\web.bat status` accurately reports worker PIDs.
- One dashboard tab does not create dozens of long-lived localhost connections.
- `QueuePool limit of size 5 overflow 10 reached` is no longer observed for news DB paths.
- Health endpoints stay responsive after startup, not just during the first few seconds.
- The difference from `origin/main` is understood, documented, and intentional.

## Notes

- Treat `origin/main` as the "known usable baseline", not necessarily the final target state.
- Avoid reverting unrelated local work while stabilizing startup.
- Prefer small, high-signal fixes with verification after each one.
