# Alastar first-observation research archive

`scripts/observe_alastar_signals.py` performs one bounded public-preview read or
imports a cached JSONL archive. It writes only under the research output folder;
it does not alter the news database, call an AI provider, send Telegram messages,
schedule itself, produce an executable direction, or place trades.

Run from `F:\9_Crypto\crypto_trading_system` using the project interpreter:

```powershell
$py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
# One fresh read of https://t.me/s/alastartrades, maximum 2 MiB, no pagination.
& $py scripts/observe_alastar_signals.py

# Cached inputs are ALWAYS historical, even if collected_at looks recent.
& $py scripts/observe_alastar_signals.py --input data/research/alastar_20261020/messages.jsonl

# Explicitly verify every prior receipt and rebuild projections; no network read.
& $py scripts/observe_alastar_signals.py --verify

# Verify, then write receipt_latency.json: gaps between live receipts, the poll
# wait they imply, outages over 600 s, and publication-to-first-receipt delay of
# every message first seen live. No network read; runs are never edited.
& $py scripts/observe_alastar_signals.py --latency
```

The default output is `data/research/alastar_observation/`. `--output-dir` selects
a separate archive. Historical imports are limited to 5,000 records and 20 MiB;
`--max-records` can lower that bound. Exceeding a bound stops the import without
silently dropping records. There is deliberately no CLI override for receipt
time, source mode, or the 600-second eligibility cutoff.

The live receipt timestamp is the host's actual UTC clock immediately after the
HTTPS response arrives. The post's timezone-aware publication timestamp is stored
separately in UTC and in its supplied representation. Cached `collected_at` is
retained as source metadata and never substituted for receipt time. The machine
clock must be accurate; this is local observation evidence, not a trusted remote
timestamp or proof of original publication-time availability.

Each observed text version retains its exact original text, its SHA-256, its
publication time and visible edit/truncation flags, plus the complete supplied
JSON record. A fresh fetch containing a new message/revision also saves the
original HTML by hash; duplicate-only runs record the body hash without another
page copy when that HTML is not already saved. Fresh parsing
raises the normal collector display limit to the already bounded HTML size, so
captions longer than 20,000 characters are retained. For older truncated inputs,
the surviving text is preserved with an explicit exclusion; missing text cannot
be reconstructed. Images and videos are not interpreted or transcribed. The
public preview may omit posts, and a visible edit marker cannot reveal earlier
unseen revisions.

`runs/` contains append-only, sequential, hash-linked observation logs. Identical
versions create a duplicate observation entry but do not create a new revision or
change the first snapshot. Changed text, publication time, edit flag, or truncation
flag creates a retained revision. `state.json` and `candidate_review_queue.json`
are atomic, replaceable projections of committed runs. `state.json` also holds a
self-hashed checkpoint. Normal appends validate the checkpoint's last run hash
and apply only subsequent committed runs, so the number of prior receipts does
not increase receipt replay work. They still read/write the message projection.
A crash during projection writing is repaired on the next invocation by replaying
uncheckpointed runs. Missing/corrupt checkpoints trigger a full verified replay.
The two projection files each carry
the last run hash; consumers should compare it if reading while a writer runs.
The archive uses a nonblocking OS file lock, released when the process exits.
An invalid tail hash or newly replayed sequence/hash chain stops collection for
manual review. Earlier historical receipts are checked by explicit `--verify`,
which scans the complete chain and validates retained message hashes. This is
local integrity checking, not protection against a party rewriting the entire
archive or its checkpoint. Preserve the whole output folder as the research record.

An entry can enter the human review queue only when all of these hold:

- Its first observation came from a fresh public-preview request, not an import.
- Its first receipt was between publication and 600 seconds after publication.
- Publication included an explicit timezone and was not in the future.
- The observed post was unedited and its text was not truncated.
- No revision of that message has subsequently been observed.

A later live fetch cannot promote an initially historical or late message into
prospective evidence. Later edits retain the original first snapshot and its
original eligibility field, while removing the message from the current review
queue. Even a reversion to the initial text stays excluded from that queue.
The queue has `human_review_required` status and `execution_authorized: false`;
eligibility establishes timely text capture, not a valid trade or profitable
strategy. A researcher must freeze interpretation and execution rules before
joining future market outcomes. This script installs no recurring monitor.

The original 26/40-view research proposed the next full hourly open after
`max(publication, actual first receipt, label_frozen_at) + 60 seconds`.
Keep that as the original research convention. The newer multi-cycle study has
a separately documented, inactive forward-paper candidate: next5m open at or
after `max(publication+15min, actual first receipt+60sec, label_frozen_at+60sec)`.
See [the research index](ALASTAR_RESEARCH_INDEX.md) and
`data/research/alastar_response_20261021/prospective_paper_protocol_v1.json`.
Neither convention permits backfilling an already-past entry after late
interpretation. The 10-minute archive cutoff only establishes timely capture;
it does not establish timely interpretation or execution. The archive does not
itself freeze labels or record prospective paper fills.

Freeze the review decision with the receipt/version evidence available then.
A later revision may remove a message from the current review queue, but must
not retroactively delete a frozen research observation, admitted paper trade or
loss. Preserve both the original decision and the subsequent revision.

The importable implementation is `core.news.alastar_observation.observe_records`.

The existing `telegram_alastar` news collector enables this archive with
`defaults.telegram_alastar_observation_archive: true` in `config/news_rules.yaml`.
It records a fresh public response, full text, and any observed revisions before
news display truncation, age filtering, and database URL de-duplication. It uses
the existing news-worker polling loop; it installs no additional scheduler.
Archive write errors are logged and marked in the news payload without dropping
ordinary news ingestion. Historical imports remain permanently excluded from
prospective review eligibility. An empty queue does not mean observation failed.
Its keyword arguments are `mode` (default `historical_import`), `observed_at`
(default actual UTC now), `evidence`, and `raw_preview`. Internal live collectors
may supply already parsed records and the actual receipt timestamp with
`mode="live_public_preview"` and the received HTML bytes. Live mode requires raw
preview bytes; historical CLI input has no override to claim live status. The
function returns counts and the committed run path. Always capture before any
publication-age filter when recording a full preview. The companion
`verify_archive(output)` checks all prior runs without a network request or a new
observation. Observation receipts accumulate over time; monitor disk usage and
retain the complete chain when backing up this research archive.

```powershell
& $py -m pytest tests/test_alastar_observation.py -q
```
