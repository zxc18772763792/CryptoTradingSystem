# Pending UI Fixes (Deferred)

## Fix 1 — One-click deploy hardcoded allocation

**File**: `web/static/js/ai_research.js`
**Functions**: `runOneClickResearchDeploy` (~lines 4377 and 4476)
**Problem**: Allocation hardcoded to `0.05` (5%). User has no way to change it before deploying.
**Fix**: Add a numeric input `<input type="number" min="1" max="100" value="5" step="1"> %`
to the one-click modal so the user can adjust allocation before confirming.
The input value should be read in `confirmRegister`/`runOneClickResearchDeploy` instead of the hardcoded `0.05`.

---

## Fix 2 — Dead humanApprove / humanReject standalone functions

**File**: `web/static/js/ai_research.js`
**Functions**: `humanApprove(id)` and `humanReject(id)` (~lines 3932–3961)
**Problem**: These two functions are defined as top-level functions but are never called anywhere
in the codebase. The approval/rejection UI buttons call `approveCandidate()` and `rejectCandidate()`
(different function names). The dead functions add confusion and were likely left over from an
earlier naming convention.
**Fix**: Delete both dead functions (20 lines). Confirm `approveCandidate`/`rejectCandidate` cover
all call sites before removing.
