# Altcoin Radar — Layout & Content-Logic Fix Plan

Date: 2026-05-18
Target display: 1920×1080.
Repo state note: the working tree contains in-progress Codex altcoin-radar
work (the "1920 Workstation Redesign" CSS, `web/api/altcoin.py`,
`altcoin_radar.js`, `web/templates/index.html`, etc.). This plan is written
against that in-flight state and is additive/conservative so it can be applied
on top without fighting that work.

## Principles

- Additive and conservative. Do NOT blind-delete duplicated CSS rules — verify
  cascade interaction first (see P1 caveat).
- Preserve every string asserted by `tests/test_altcoin_radar_ui_assets.py`
  (`data-tab="altcoin-radar"`, `id="altcoin-radar"`, `山寨雷达`,
  `btn-altcoin-radar-refresh`, `altcoin-radar-ranking-body`,
  `altcoin-radar-inspector-shell`). That test also asserts
  `static_asset_url("css/style.css")` / `js/altcoin_radar.js` equal the
  `ASSET_VERSIONS` entry — so bumping the registry keeps it green automatically;
  forgetting to bump does NOT fail that test but ships a stale cached asset.
- Missing/again non-numeric metrics must never render as `NaN`/`0.00`; use the
  existing guarded helpers.
- One change unit = one coherent commit; do NOT bundle the unrelated Codex WIP.

## P1 — CSS consolidation for `.altcoin-radar-workspace` (conservative)

Facts:
- Base def #1 at `style.css:8789`:
  `display:grid; grid-template-columns:minmax(280px,320px) minmax(0,1fr);
  gap:22px; align-items:start;`
- Base def #2 at `style.css:13071`:
  `grid-template-columns:minmax(300px,340px) minmax(0,1fr); gap:24px;`
  — **no `display:grid`, no `align-items`**. It only overrides two
  properties and *relies on #1 cascading* `display:grid`/`align-items`.
- ~12 media overrides (max 1500/1360/1280/1180/1080/1600/1320, min 1761) plus
  two `@supports selector(.container:has(#altcoin-radar.active))` blocks
  (`13306`, `14003`).

CAVEAT: `8789` is NOT fully dead — deleting it removes `display:grid` and the
layout collapses. Do not blind-delete.

Tasks:
1. Make `13071` self-contained: add `display:grid;` and `align-items:start;`
   to it so it no longer depends on `8789` cascading.
2. After step 1, the `8789` block's only still-needed properties
   (`display:grid`, `align-items:start`) now also live in `13071`, so `8789`
   is fully superseded. Delete the `8789` rule body and leave a one-line
   comment at its location pointing to the consolidated rule at `13071`
   (+ the `@media (min-width:1761px)` tier). Do not leave a dangling empty
   selector.
3. Audit the ~12 media overrides: only collapse pairs whose bodies are
   byte-identical AND in the same media context. Leave specificity-layered
   `!important` rules intact (same discipline as the research-workbench fix).
4. Confirm the existing `@media (min-width:1761px)` block (`12589`) sets the
   authoritative 1920 layout; if it does not target `.altcoin-radar-workspace`
   explicitly, add an authoritative entry there (workspace max-width +
   centered gutters so 1920 is balanced, matching the workbench treatment).

Acceptance: at 1920 the workspace renders identically to before this change
(visual parity), `display:grid` still applies, no horizontal scrollbar; editing
`13071` alone fully controls the base layout (no hidden dependency on `8789`).

## P2 — Eliminate `NaN` in enrichment badges (`altcoin_radar.js`)

Facts: lines 1144 / 1148 / 1150 build the event timeline detail with
`Number(e.ignition_score).toFixed(2)` / `crowding_late_score` /
`narrative_heat_score`. The outer `e.x != null` guards prevent the
null/undefined case, but a present-but-non-numeric value (string, object) still
yields literal `"NaN"`. A guarded helper `shortNumber(value, digits=2)` already
exists (returns `'--'` for non-finite).

Tasks:
- Replace the three `Number(e.<field>).toFixed(2)` with `shortNumber(e.<field>)`
  inside the existing ternary, keeping the `!= null` branch structure
  unchanged.

Acceptance: a row whose `ignition_score` is a non-numeric value shows `点火=--`,
never `点火=NaN`. No behavior change for valid numbers.

## P3 — `:has()` gate fallback for the 1920 redesign

Facts: the entire "1920 Workstation Redesign" is wrapped in
`@supports selector(.container:has(#altcoin-radar.active))` (blocks at
`13306` and `14003`). On a browser without `:has()` selector support the whole
redesign silently no-ops and the page falls back to the older cramped layout
with zero indication.

Tasks (pick the lower-risk option):
- Option A (preferred, minimal): ensure the *essential* workspace grid
  (`.altcoin-radar-workspace` columns/gap + the center/inspector rails) is
  defined OUTSIDE the `@supports` wrapper too, so the page is always usably
  laid out; the `@supports` block then only adds the polish (sticky rails,
  dvh sizing, `:has`-scoped container tweaks). The base rules from P1 already
  provide this — verify the non-`:has` path yields a coherent 2-pane layout
  and document that the redesign polish is `:has`-gated by design.
- Option B (only if A is insufficient): add an `@supports not
  selector(.container:has(...))` block with a flexbox fallback for the
  workspace.

Acceptance: with `:has()` disabled (devtools rendering emulation or a
`@supports not` check) the radar still shows a coherent two-pane layout, not a
single collapsed column.

## P4 — Asset version bump

File: `web/asset_versions.py` — `css/style.css` 101 → 102 (P1/P3),
`js/altcoin_radar.js` 18 → 19 (P2). Required for cache-bust; keeps the
UI-asset test's version-equality assertions self-consistent.

## P5 — Verify

- `node --check web/static/js/altcoin_radar.js`
- CSS brace balance unchanged (`{` count == `}` count delta 0 for the edit).
- `pytest tests/test_altcoin_radar_ui_assets.py tests/web/test_altcoin_route.py
  tests/test_altcoin_radar_universe.py tests/test_altcoin_radar_derivatives.py
  -q`
- `simplify` skill over the changed JS/CSS.
- Static visual reasoning only unless a live 1920 browser session is available;
  state the limitation explicitly (as in the workbench fix).

## Out of scope (do not touch here)

- The Codex altcoin redesign logic / `web/api/altcoin.py` scoring / universe
  separation — already committed or in-flight separately.
- The radar→research funnel (`create_research_proposal_from_radar`) — verified
  real and working; no change needed.
- The 12-lens sort model — `priority` composite already addresses the earlier
  decision-paralysis concern; not revisited here.

## Execution staging

S1 P2 (lowest risk, user-visible) → S2 P1 (CSS, careful, visual parity) →
S3 P3 (verify non-:has fallback) → S4 P4 bump → S5 P5 verify + completion note.
Each stage runs the UI-asset/route tests before proceeding. Commit the radar
fix as ONE unit, excluding unrelated Codex WIP. Do not commit unless asked.

## Completion Note (2026-05-19)

Status: COMPLETE (P1-P5). Not committed.

- P1/P3: `.altcoin-radar-workspace` has a self-contained base rule
  (`display:grid`, `align-items:start`) and the earlier duplicate location is
  replaced by an orientation comment; the `min-width:1761px` tier explicitly
  controls the 1920 workspace.
- P2: enrichment badges now render non-finite event scores via `shortNumber`,
  so bad `ignition_score` / `crowding_late_score` / `narrative_heat_score`
  values show `--`, not `NaN`.
- P4: asset versions are bumped (`css/style.css` 109,
  `js/altcoin_radar.js` 19).
- P5: static checks passed for `altcoin_radar.js`; targeted pytest verification
  is recorded in the 2026-05-19 daily bug scan memory.
