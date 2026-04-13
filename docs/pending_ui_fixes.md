# Pending UI Fixes

Last updated: 2026-04-13

Current status: no pending items.

Resolved in latest integration:

1. One-click deploy allocation is no longer hardcoded.
- Added `id="ai-oneclick-allocation"` input in `web/templates/index.html`.
- `web/static/js/ai_research.js` now reads the user-selected percentage and maps it to `allocation_pct` for:
  - `/oneclick/research-deploy`
  - `/oneclick/deploy-candidate`
- One-click summary now includes allocation.

2. Removed dead standalone handlers.
- Deleted unused `humanApprove(candidateId, target)` and `humanReject(candidateId)` from `web/static/js/ai_research.js`.
- Approval flows continue through existing `approveCandidate()` / `rejectCandidate()` paths.

3. Register/deploy modal now supports allocation override.
- Added allocation percent input (`reg-allocation-percent`) to register modal.
- `confirmRegister()` sends `allocation_pct` to `/candidates/{candidate_id}/register`.
- Backend `AICandidateRegisterRequest` now accepts optional `allocation_pct` and persists it into candidate metadata before promotion.
