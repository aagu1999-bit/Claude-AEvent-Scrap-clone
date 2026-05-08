# ADR 0008: Skip Apify shell records + surface affected accounts

Date: 2026-05-08
Status: Accepted

## Context

When Apify scrapes an Instagram account that returns no posts (the account
is private, suspended, deleted, has zero recent posts, or is a typo'd
handle), Apify still emits a record into the dataset. That record has the
shape:

```
{
  "inputUrl": "https://www.instagram.com/dj_peters42",
  "url":      "https://www.instagram.com/dj_peters42",
  "id":       <missing>,
  "shortCode": <missing>,
  "ownerUsername": "",
  "caption": "",
  "type": ""
}
```

These are **shell records** — they're not posts. They're metadata about a
failed account-level scrape.

The original `process_post()` did:

```python
pid = post.get('id') or post.get('shortCode') or f'post_{post_num}'
```

When neither `id` nor `shortCode` was present, it fabricated a synthetic
ID like `post_142`. The post then proceeded through OCR (no image to OCR),
Gemini (no caption to analyze), produced no events, and got tagged
`no_events_found` with the pseudo-ID written to `Processed_Log`.

Two problems with that:

1. **`Processed_Log` accumulates pseudo-IDs forever** — by 2026-05-08 there
   were 224 such rows, all `no_events_found` with `post_NNN` IDs that
   could never collide with real Instagram post IDs (real ones are 19-digit
   integers). They block nothing in dedup but pollute the audit surface.

2. **The actual signal — "this account is producing nothing" — is silent.**
   Each weekly run, ~13% of the static dataset's records are shells. The
   pipeline burns Vision+Gemini quota on them, gets nothing, and reports
   nothing useful. The operator never finds out which of their ~1,000+
   active accounts are dead.

Empirical evidence (verified by reading the static dataset directly on
2026-05-08): the dataset contains 4,014 entries; 133 are shell records
across 133 distinct accounts. That's ~3.3% by count, but those accounts
appear every weekly run, so cumulative pollution is much larger.

## Decision

Two layered changes in `process_post()`:

### Layer 1 — skip at entry

```python
pid = post.get('id') or post.get('shortCode')
if not pid:
    self._note_apify_shell_record(post, post_num)
    return None
```

If neither real ID is present, we treat it as a shell record. We do NOT:
- fabricate a pseudo-ID
- write to `Processed_Log`
- run OCR or call Gemini

### Layer 2 — track + surface affected accounts

The new `_note_apify_shell_record()` helper:

- Increments `stats['apify_shell_records']`
- Best-effort extracts the affected account name from `inputUrl` (the
  shell record's `url` is the profile URL, e.g.
  `https://www.instagram.com/dj_peters42`)
- Accumulates into `self._shell_accounts: dict[str, int]`

`create_final_report()` prints a "dead accounts" block when shells were
seen, with the top 20 by frequency. The full per-account map is included
in the run's `anomalies_<ts>.json` summary under
`apify_shell_accounts`.

## Consequences

- **Pseudo-ID generation is eliminated forward.** Combined with the
  one-off cleanup script (ADR 0007), `Processed_Log` no longer contains
  `post_NNN` rows from any source.
- **Dead-handle visibility is gained.** Each run now reports the
  accounts that produced zero posts, sorted by frequency. The operator
  can review these and decide whether to remove or correct them in the
  `Accounts` tab.
- **Per-run extraction quota is reduced slightly** — we no longer run
  OCR/Gemini on shell records that would never produce events. ~133
  fewer Gemini calls per weekly run with the current dataset.
- **No data loss risk** — shell records by definition have no caption
  and no carousel images. There is nothing in them to extract; skipping
  them removes only wasted work.

## What this does NOT do

- **Does not auto-remove dead accounts** from the `Accounts` tab. The
  operator decides. We surface; they act. This is intentional — some
  accounts may be temporarily private and will recover.
- **Does not reach back through `account_hygiene.py`** to mark these
  accounts. That's a separate hygiene concern; this ADR keeps the
  in-pipeline surface clean and leaves the periodic hygiene check as
  its own responsibility.
- **Does not classify shell records as `gemini_error` or `ocr_failed`.**
  They get a brand new bucket — `apify_shell_records` — because they're
  structurally different from extraction failures. Lumping them in would
  obscure both signals.

## Verification plan

- After the next pipeline run, the run's stats JSON should contain
  `apify_shell_records` with a non-zero count.
- The run log should print the "Apify returned empty for..." block at
  the end with a list of accounts.
- `Processed_Log` should grow only by genuine post entries; no new
  `post_NNN` rows.
- Dry-running the cleanup script (`cleanup_pseudo_ids_and_duplicates.py`)
  after the next run should report 0 pseudo-IDs to delete.

## Future agents — read this before changing the fallback

- **Don't restore the `f'post_{post_num}'` fallback.** It generates IDs
  that look like data but aren't, polluting `Processed_Log` with rows
  that block no real dedup checks.
- **Don't dedup shell records by their `inputUrl` either.** Two different
  weekly runs may emit shells for the same dead account; that's a signal
  to surface, not a duplicate to suppress.
