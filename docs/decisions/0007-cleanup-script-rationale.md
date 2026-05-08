# ADR 0007: One-off cleanup script for pseudo-IDs, duplicates, and column drift

Date: 2026-05-08
Status: Accepted

## Context

Three independent data-quality issues accumulated in `Instagram_Events_Master`
during the period before the May 7-8 incident investigation:

**1. Composite-key duplicates in `All_Events`.**
Audit on 2026-05-08 showed 16,831 total rows, 16,328 unique
`(POST ID, EVENT NAME, DATE)` keys, 288 duplicate keys, **503 excess rows**.
Top duplicate appeared 14× — suggests one specific post got re-extracted
across many weekly runs via interactions between the static-URL feed,
`history_max_age_days` cutoff, retry tags, and the race-condition path
addressed in [ADR 0006](0006-dedup-race-condition.md).

**2. Pseudo-IDs in `Processed_Log`.**
3,189 rows have `Post ID` starting with `post_` instead of a real
Instagram post ID. Two distinct populations:

- **2,965 from the Feb 5 2026 migration script** — imported from the
  pre-Sheet checkpoint pickle. Empty Account / Result / Post Date.
- **224 from Auto-Bot weekly runs** (Apr 23 – May 8) — `no_events_found`
  outcomes where Apify returned post objects with both `id` and
  `shortCode` missing, triggering the `f'post_{post_num}'` fallback in
  `process_post()`.

Real Instagram post IDs are 19-digit integers. `post_NNN` strings can
**never** structurally collide with real IDs. So these rows block zero
real posts in dedup — they're inert. But they consume rows and cause
audit ambiguity.

**3. Column drift in `Processed_Log`.**
Two distinct row schemas have been written to the same tab:

- **Schema A (Auto-Bot, current code path):** matches canonical header
  `Post ID, Account, Date Processed, Source, Notes, Result, Post Date`.
- **Schema B (Migration Script):** drifted one column to the left —
  what should be `Date Processed` is in the `Account` column,
  what should be `Source` is in `Date Processed`, etc.

Detection by content fingerprint (presence of "Migration Script" and
"Imported from checkpoint" string literals), not by row position, so
future undocumented schema variants land in an "unknown" bucket rather
than getting silently broken.

## Decision

Add `cleanup_pseudo_ids_and_duplicates.py` — a one-off script that runs
three passes against the live Sheet, with **dry-run as the default mode**
and explicit `--apply` flag required to make any writes.

### Pass 1 — `All_Events` events-dedup
Group by `(POST ID, EVENT NAME, DATE)`. For each duplicate key, keep
the first occurrence (lowest sheet row), delete the rest. Expected
delete count: ~503 rows.

### Pass 2 — `Processed_Log` pseudo-ID delete
Delete every row where `Post ID` starts with `post_`. Expected delete
count: ~3,189 rows. Safe because pseudo-IDs cannot match any real
Instagram post.

### Pass 3 — `Processed_Log` column-shift
For real-ID rows classified as Schema B by content fingerprint, rewrite
the row to match canonical Schema A:
- `Post ID` → unchanged
- `Account` → empty (we don't have it; the migration source didn't store it)
- `Date Processed` ← whatever was in old col 1 (the actual date)
- `Source` ← `"Migration Script"` (canonicalised)
- `Notes` ← `"Imported from checkpoint"` (canonicalised)
- `Result` → empty (no extraction was actually done — this is just
  history dedup data, not an extraction outcome)
- `Post Date` → empty

Header is also rewritten to canonical (`Date Processed` replaces the
non-canonical `Migration Date` label).

### Order and safety guarantees

- **Backup before any writes.** `All_Events_Backup_<ts>` and
  `Processed_Log_Backup_<ts>` tabs are created before passes 1-3 run.
  Recoverable as long as the backup tabs are not also deleted.
- **Dry run is the default.** `--apply` is required to do anything
  destructive. Dry run reports counts and a sample of what would change.
- **Rate-limited writes.** Deletes batched at 100/batch with 1.5s sleep
  between batches, well under Google Sheets' API quotas.
- **Failed individual writes don't abort the pass.** Errors are printed
  and the pass continues with the next row. The verification report at
  the end shows actual vs. expected counts.

## Consequences

- **All_Events** drops by ~503 rows. Composite-key duplicates eliminated.
  Future duplicates from race-conditions / source-list duplicates are
  prevented by [ADR 0006](0006-dedup-race-condition.md).
- **Processed_Log** drops by ~3,189 rows. Pseudo-IDs gone, dedup history
  for real posts unchanged. Migration-source rows that remain have
  canonical column alignment.
- **The 224 Auto-Bot pseudo-ID posts will re-process on next run** because
  they're no longer in dedup. They were tagged `no_events_found` previously
  so re-processing won't create duplicate event rows in `All_Events` (no
  prior event rows existed). If Gemini handles them differently this time
  and finds events, those are net-new coverage, not pollution.
- **Schema A becomes the only schema** writing to `Processed_Log` going
  forward. The pipeline's own `save_data()` already writes Schema A;
  `migrate_history.py` will need a separate update if it runs again
  (currently part of the parallel "Project" workflow). Tracked as a
  follow-up — not blocking.
- **Backup tabs persist indefinitely** unless manually removed. They're
  dated, so accumulation is visible.

## What this script does NOT do

- **Does not fix the source of the duplicates.** That's [ADR 0005](0005-config-mode-scoping.md)
  (Mode-2 cutoff scoping) and [ADR 0006](0006-dedup-race-condition.md)
  (atomic check-and-claim + source-list dedup). Run those first.
- **Does not modify the canonical pipeline code.** It's a one-off cleanup
  utility, not a behavior change. Safe to run multiple times — additional
  runs find zero work to do once the first cleanup completes.
- **Does not touch `Reliable_Accounts`, `Accounts`, `Reprocess_Backup`,
  or other tabs.**
- **Does not delete the existing backup tabs from prior runs.**

## Recommended order of operations

1. Merge ADR 0005 + ADR 0006 PRs first (stops new bleed).
2. Wait for one weekly run to confirm those PRs are stable in production.
3. Run `python cleanup_pseudo_ids_and_duplicates.py` (dry run) and
   review the counts and sample previews.
4. Run `python cleanup_pseudo_ids_and_duplicates.py --apply` to execute.
5. Verify the backup tabs exist before deleting anything else.
6. The cleanup is now complete; the script can be left in the repo for
   future use if similar accumulation occurs.

## Future agents — read this before similar cleanups

- This is a **one-off** script, not a recurring maintenance task. Don't
  add it to the cron workflow.
- The fingerprint-based row classification is the only reliable detector
  given that the Sheet has historically had multiple schemas. Don't
  switch to position-based detection.
- The delete-and-shift operations are batched explicitly because Google
  Sheets API has a per-minute write quota that gets exceeded if you
  batch-write 3,000+ row deletes without pacing.
