# Research Workbench — Layout & Content-Logic Fix Plan

Date: 2026-05-18
Target display: 1920×1080 (no workbench media query currently triggers above 1760px).

## Principles

- Additive and conservative. Do NOT mass-delete the 13–15 duplicated
  `.research-*` rule definitions — they govern breakpoints the user does not
  use and blind deletion risks regressions. Add one authoritative large-screen
  tier and fix concrete bugs instead.
- Preserve every string asserted by `tests/test_research_workbench_ui_assets.py`
  (button classes, `.research-module-btns .btn`, the green gradient, JS symbol
  names, regime calendar IDs).
- Bump `web/asset_versions.py` for any changed `css/`/`js/` asset (cache bust).
- Missing data must render as `—`, never a fabricated `0.00`.
- Sub-module render failure must be visible, not a silent `.catch(()=>{})`.

## P1 — Authoritative ≥1761px tier (the user's 1920 resolution)

File: `web/static/css/style.css` (append a new `@media (min-width:1761px)`
block near the other research media queries; do not edit the early dead rules).

- Constrain the workspace so the main column is not absurdly wide:
  `#research .research-workspace { grid-template-columns: minmax(300px,340px) minmax(0,1fr); }`
  and cap the whole workspace `max-width: 1680px; margin-inline:auto;` so 1920
  has balanced gutters instead of one ultra-wide column.
- Raise overview density:
  `#research .research-overview-cards { grid-template-columns: repeat(4, minmax(0,1fr)); }`
  (currently `auto-fit minmax(180px,1fr)` → few over-wide cards at 1920).
- Keep nested `research-top-grid` proportions but lift the right-rail floor so
  it is not squeezed: `grid-template-columns: minmax(0,1.5fr) minmax(360px,0.8fr)`.

Acceptance: at 1920 the main column has bounded width, overview shows 4 cards
per row, right rail ≥360px. No horizontal scrollbar.

## P2 — Content-logic fixes (`web/static/js/research_workbench.js`)

- Add a `fmtMetric(value, digits)` helper returning `—` for
  `null/undefined/NaN`, else `Number(value).toFixed(digits)`. Replace the
  misleading `(x || 0).toFixed(...)` numeric renders in the overview /
  status / regime panels with it. Do NOT touch values that are legitimately 0.
- Replace silent `.catch(()=>{})` on the per-module render path with a
  `renderModuleError(name, err)` that paints a visible degraded badge
  (`模块加载失败 · {name}`) into that panel instead of leaving stale/blank.
  Keep `setDebug` for diagnostics.
- Acceptance: forcing a module fetch to reject shows a visible failure badge;
  a missing Sharpe renders `—` not `0.00`.

## P3 — Regime calendar becomes class-based & responsive

Files: `web/templates/index.html` (line ~1187), `web/static/css/style.css`.

- Replace the inline `style="display:flex;flex-wrap:wrap;gap:6px;min-height:60px"`
  on `#regime-calendar-grid` with `class="regime-calendar-grid"` and define
  `.regime-calendar-grid` once (flex wrap + gap + min-height), plus a
  `≥1761px` rule that lets cells use a tidy fixed track.
- Acceptance: UI-asset test still passes (id unchanged); cells wrap cleanly at
  1920.

## P4 — Targeted CSS hygiene (low risk only)

File: `web/static/css/style.css`.

- Only collapse rule pairs whose bodies are byte-identical duplicates AND are
  in the same media context. Leave specificity-layered `!important` rules
  intact. Add a top-of-section comment marking the early `.research-*`
  (≈L140–234) block as superseded by the `#research`-scoped block (≈L12200+)
  so future editors do not chase dead rules.
- No behavioral change expected; this is documentation + safe exact-dup removal.

## P5 — Verify

- `node --check web/static/js/research_workbench.js`
- `python -m py_compile web/api/research.py`
- `pytest tests/test_research_workbench_ui_assets.py tests/test_research_workbench_recommendations_api.py tests/test_research_market_state.py -q`
- Bump `web/asset_versions.py` (`css/style.css`, `js/research_workbench.js`).
- Run `simplify` skill over the changed JS/CSS as a quality pass.
- Browser preview at 1920 if preview tooling is available; otherwise state that
  visual verification was static-only.

## Completion Note (2026-05-18)

Status: COMPLETE (P1–P5). Not committed (working tree also has unrelated
in-progress Codex altcoin-radar work; left untouched).

- P1 — added `@media (min-width:1761px)` authoritative tier in style.css
  (workspace capped at 1680px + centered, overview 4-up, body/top-rail floors).
- P2 — `research_workbench.js`: missing confidence preserved as `null` at
  normalization and rendered via existing `fmtNumber` → shows `—` not a
  fabricated `0.00` (2 display sites). `renderModule` catch now flips the
  module's `status` to `error` so the existing status-board chip turns red
  (visible degradation) instead of a silent `setDebug`.
- P3 — `#regime-calendar-grid` inline style replaced with
  `class="regime-calendar-grid"`; class defined once + a ≥1761px grid track.
- P4 — orientation comment on the early dead `.research-*` block (no risky
  mass-deletion of the 13–15 duplicates).
- P5 — `node --check` OK, `py_compile` OK, asset versions bumped
  (css 95→96, research_workbench.js 9→10), UI-asset + workbench API +
  market-state + phase5 tests: 21 passed. `simplify` pass removed a redundant
  defensive guard in the catch block (module is guaranteed an object there).

Verification limitation: validated via syntax checks, the UI-asset/API test
suite, and CSS-cascade analysis. The running app was NOT exercised in a real
1920×1080 browser session (would require launching the full FastAPI app with
live data); the auto-opened preview panel shows the static template only.
Recommend a quick manual eyeball at 1920 before relying on it.

False alarm investigated and cleared: the 697-line style.css diff hunk at EOF
is pre-existing uncommitted Codex "Altcoin Radar 1920 Workstation Redesign"
work, not introduced here. This task's footprint is exactly 48 CSS + ~10 JS
lines + 1 HTML attribute + version bump.
