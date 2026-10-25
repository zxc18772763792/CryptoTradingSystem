# Signals / Telegram audit — 2026-10-23

Scope: `core/research/alastar_signal_intake.py`, its regression tests, and the public Telegram collector. Existing journal, paper/horizon protocol, immutable receipt and execution behavior were not modified. No model/exchange API calls were made; model behavior is injected by tests.

## Fixed defects

1. **Unbounded Telegram missing-ID allocation.** `_unseen_ids_before` built the entire missing-ID range before slicing to 100 values. A valid ID gap of 1 to 1,000,000 allocated **39,992,616 bytes** in the isolated pre-fix expression; allowed 15-digit message IDs could exhaust the collector process. The range endpoint is now bounded before allocation. Regression exercises real BeautifulSoup widgets with a million-ID gap, verifies the same first 100 missing IDs, and caps traced peak allocation at 1 MiB.

2. **One unrepresentable UTC publication dropped the whole page.** A timezone-aware datetime can parse successfully but overflow when converted to UTC, e.g. `9999-12-31T23:59:59-01:00`. Direct pre-fix execution raised `OverflowError` from `astimezone`, outside the existing exception guard. UTC conversion now shares the invalid-date guard, so the bad post is skipped. Both lower and upper date-boundary regressions were added to the collector tests.

3. **Interpreter reported recovery before retry, and stayed active when disabled.** Every poll previously cleared `last_error`, including polls that skipped a failed request during its 30-second cooldown, causing the worker's health callback to report success after only 5 seconds. The failure is now retained while any item is `retry_pending`, and cleared after actual resolution; successful retry also removes stale item `error_type`. `detail().runner_active` now checks the enabled flag and unresolved errors. A disabled worker clears the persisted continuous-runner flag and emits an unhealthy callback. If a failed or processing source disappears from the current review queue (e.g. the archive excludes a newly edited post), its unresolved state now becomes `removed_from_current_review_queue` and stops contributing a stale global error, while retaining all committed-book receipts. Regressions use real archive edits to remove candidates after both model failure and partial book submission; neither path requests another interpretation. Additional regressions cover failure/cooldown/recovery, enabled/error combinations, and an actual disabled worker loop.

4. **Queued automatic labels could be permanently lost on temporary lock contention.** A regression holds the real receipt-archive lock and invokes the frozen v2 inbox consumer: the consumer permanently records `rejected` / `RuntimeError`; releasing the lock and consuming again leaves the journal at activation only. The earlier intake considered its inbox write terminal and never checked that a journal freeze happened.

   The new adapter now saves the model evidence and exact label input, calls each book's **existing, unmodified `freeze(label)`** validator directly, and records immutable per-book journal receipts. A source already present in a book's immutable journal counts as confirmed success. Repeated attempts reuse the original model result, prompt hash, quote and label bytes; a partial commit or receipt-write failure is reconciled against the journal before attempting another freeze. New automatic inputs never enter the manual inbox.

   Old `label_queued` items are rechecked instead of treated as complete. Only a legacy `auto_<source-hash>.json` whose bytes exactly match the durable model interpretation can be archived into submission evidence and removed, while holding the v2/v3 consumers' pass locks. Manual files and differing automatic-looking files are preserved. This migration is covered for both already committed and previously uncommitted sources. Expired unfinished books receive no new freeze; already committed books retain their receipts. The deadline is also checked between sequential book submissions.

## Evidence / validation

- Health regression was also run against an **in-memory reconstruction of the original poll health logic**, without replacing any workspace file: it failed as expected with `None != 'RuntimeError'` on the cooldown poll.
- Targeted final validation:

  ```powershell
  & 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' -m pytest tests/test_alastar_signal_intake.py tests/test_telegram_collector.py tests/test_telegram_news_integration.py tests/test_alastar_observation.py -q
  ```

  **112 passed in 20.45s.** This includes real v2/v3 freeze acceptance, immutable revision, expiry, redaction, retry and disabled-worker behavior; removal of edited failed candidates with receipt preservation and health recovery; multi-book partial commits with a real archive lock failure; no repeated model/freeze after a committed journal record; post-commit receipt-write failure recovery; partial expiry; deadline crossing between books; and legacy migration/input preservation.

No frozen tracker files, `web/main.py`, or `web/api/` files were edited by this audit task. No changes were staged or pushed.
