# ADR 0006: Atomic check-and-claim in `process_post()` + source-list dedup

Date: 2026-05-08
Status: Accepted

## Context

The May 8 reprocess run produced **47 duplicate composite keys** in
`All_Events` (`(POST ID, EVENT NAME, DATE)`), totaling **55 excess rows**.
All duplicates carried the same `processed_timestamp` (`2026-05-08
01:51:16`), meaning they were written in a single `append_rows()` call —
so the duplication originated **upstream of the Sheet write**, in the
in-memory `self.results` list.

Tracing through `process_post()` revealed two parallel mechanisms:

### Mechanism 1 — split lock blocks for dedup check vs. dedup add

The original `process_post()` had this shape:

```python
def process_post(self, post, post_num, total):
    pid = post.get('id') or post.get('shortCode') or f'post_{post_num}'

    with self.lock:                              # ← lock A acquired
        if pid in self.processed_posts:
            self.stats['skipped_history'] += 1
            return None
                                                 # ← lock A released here

    # ... 10–30 seconds of OCR + Gemini work happens here, no lock held ...

    with self.lock:                              # ← lock B acquired (much later)
        self.processed_posts.add(pid)            # finally claimed
        self.post_results[pid] = {...}
        self.results.extend(processed_events)
```

The dedup check (lock A) and the dedup add (lock B) are separated by
seconds of network/model work where the lock is released. Two threads
that pull the same `pid` simultaneously can both pass the lock-A check
before either reaches lock B. Both then write events to `self.results`
and produce duplicate rows.

### Mechanism 2 — duplicate post IDs in the source list

`reprocess_weekends.py` calls `pipeline.processed_posts = set()` to clear
the dedup history before processing, then submits a list of `fresh_posts`
fetched from Apify. If Apify returns the same post object twice in a
single dataset (which it does occasionally, especially after manual
filtering or when posts are cross-tagged), the duplicate entries enter
`run_pipeline()`'s `ThreadPoolExecutor`. Even with a perfect lock, both
copies of the same `pid` would race through the entry check.

With `max_workers: 2` and per-post processing time of seconds, the race
window is wide enough that **3-way duplication** can occur if a post
appears 3× in the source — observed in the May 8 run:

| Post ID | Event | Duplication factor |
|---|---|---|
| `3877464873669843235` | NURSES WEEK COCKTAIL FOR A CAUSE | ×3 across 7 dates |
| `3881124183330420217` | HANDMADE MARKET | ×2 across 17 weekly dates |
| `3867434041209926811` | R&B ONLY LIVE | ×2 across 5 dates, ×3 on one |

## Decision

Two complementary fixes, both shipped together because either alone
leaves a path open.

### Fix A — atomic check-and-claim in `process_post()`

```python
with self.lock:
    if pid in self.processed_posts:
        self.stats['skipped_history'] += 1
        return None
    self.processed_posts.add(pid)    # claim immediately, in same lock block
```

The post is now in the dedup set the moment the check passes. A second
thread with the same `pid` will see it claimed and short-circuit at the
`if pid in self.processed_posts` branch — incrementing `skipped_history`
rather than processing the post a second time.

The downstream `_record_post_outcome()` and other code paths still call
`self.processed_posts.add(pid)`, but `set.add` is idempotent — re-adding
an already-present element is a no-op.

### Fix B — source-list dedup in `reprocess_weekends.py`

Before `pipeline.run_pipeline(fresh_posts)`:

```python
seen_pids = set()
deduped = []
for p in fresh_posts:
    pid = p.get('id') or p.get('shortCode')
    if pid and pid in seen_pids:
        continue
    if pid:
        seen_pids.add(pid)
    deduped.append(p)
fresh_posts = deduped
```

Removes duplicate post objects from the input list before any threads see
them. Belt to Fix A's suspenders — this catches the case where the
same `pid` appears twice in the source and Fix A would still race
under the worst-case timing.

## Consequences

- **No more duplicate rows from same-pid races.** Both fixes apply on
  every pipeline run going forward (Mode 1 and Mode 2; reprocess and
  regular). The 55 May 8 duplicates were the visible symptom; this
  closes the source.
- **`skipped_history` counter will increment in the duplicate-source
  case.** When the source list has the same `pid` listed 3 times, the
  first copy is processed and the next 2 are skipped via `skipped_history`.
  This will show up as a small bump in that stat for affected runs.
- **Existing duplicate rows in `All_Events` are not removed by this PR.**
  A separate one-off cleanup script (PR-C, forthcoming) will collapse
  composite-key duplicates to single rows. Tracked in CHANGELOG.

## Edge case to be aware of: claim-without-outcome

The atomic claim happens at the entry check. If `process_post()` then
encounters an unexpected exception that escapes the existing inner
`try`/`except` blocks (the OCR helper, the Gemini call, the carousel
URL parsing), the post is now in `processed_posts` but no entry was
written to `post_results`. On the next run, that `pid` would be skipped
as "already processed" — silent loss.

In practice this gap is narrow because:

- `extract_ocr_text()` has its own `try`/`except Exception` returning `""`
- The Gemini call is wrapped in a top-level `try`/`except Exception`
  that records a `gemini_error` outcome
- The carousel URL collector has its own exception handling

But if a future change introduces an uncaught exception path between
the entry claim and any outcome write, posts can be silently dropped.

The clean fix for that is wrapping the post-entry portion of
`process_post()` in a `try` / `finally` that ensures either an outcome
is recorded or the claim is rolled back via `processed_posts.discard(pid)`.
That refactor is deferred — current exception coverage is adequate.

## Verification plan

After this PR ships, the next reprocess run should show:

- `skipped_history` ≥ 1 if the source list has any duplicate post IDs
  (Fix B handles most of these before threads start; Fix A catches the
  rest via the entry claim)
- Composite-key duplicates from new runs: 0
- `Worker error: ...` lines remain absent from run logs for posts that
  hit the early-return claim path

The agent or operator should run the same composite-key audit query
after the next Mode-2 run to confirm zero new duplicates.
