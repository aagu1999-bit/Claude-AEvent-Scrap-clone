# ADR 0010: Incremental Processed_Log writes

Date: 2026-05-08
Status: Accepted

## Context

`save_data()` previously wrote the entire run's `post_results` to the
`Processed_Log` Sheet tab in **one batch at the end of the run**, after
all post processing was complete. We verified this directly:
`save_data()` calls `self.log_worksheet.append_rows(log_rows)` exactly
once at line 1101 (pre-this-ADR), with `log_rows` constructed from the
full `post_results` dict.

**The failure mode this exposes:** if a run is killed before
`save_data()` is reached, **100% of that run's dedup state is lost.**

Concrete evidence from the May 7 incident: three Event Pipeline runs
(18:24:58, 18:25:41, 18:26:44) each processed roughly 395 of 4,441
posts before being killed. Each run ran OCR via Vision API, called
Gemini for extraction, accumulated `post_results` entries in memory,
and then died — none of that dedup work made it to `Processed_Log`. On
the next run, those ~395 posts (per-run) are treated as fresh and
re-processed from scratch. Wasted Gemini quota, wasted Vision quota,
wasted runtime.

## Decision

`Processed_Log` writes happen **incrementally during the run**, not
only at end-of-run.

### Mechanism

- `__init__` initializes `self._flushed_pids: set`,
  `self._flush_lock: threading.Lock`, and `self.PL_FLUSH_THRESHOLD =
  50`.
- New method `_flush_processed_log(force=False)`:
  - Snapshots the subset of `self.post_results` whose pids aren't yet
    in `self._flushed_pids` (i.e., pending entries).
  - If `not force` and pending count is below `PL_FLUSH_THRESHOLD`,
    returns immediately. Most calls are this no-op.
  - Otherwise, calls `self.log_worksheet.append_rows(...)` with the
    pending entries, then updates `self._flushed_pids` to mark them
    written.
- `run_pipeline` calls `self._flush_processed_log(force=False)` after
  each future completes (in the parallel path) or after each
  `process_post()` returns (in the sequential path). The threshold
  check inside makes most calls cheap.
- `save_data()` calls `self._flush_processed_log(force=True)` at the
  end so any tail-end entries (below threshold) get written.

### Thread safety

- `self._flush_lock` serializes flush attempts. Only one thread is
  inside a flush at a time, so we don't double-write.
- `self.lock` (the existing main lock) guards both the snapshot read
  of `self.post_results` and the post-write update of
  `self._flushed_pids`. The Sheets API call itself happens between
  the snapshot and the flushed-pid update, so worker threads can keep
  adding to `post_results` during the network call without blocking
  on the flush. They're picked up in the next flush.

### Failure handling

- If `append_rows()` raises (network blip, Sheets quota), the failure
  is printed but the pipeline continues. The pids that didn't get
  flushed stay out of `self._flushed_pids`, so the next flush call
  attempts them again.
- The `force=True` final flush in `save_data()` also covers any
  entries that previously failed to flush.

## Consequences

- **Killed-run dedup state survives.** A run that completes 1,000
  posts before being killed will have ~950+ entries in `Processed_Log`
  (last partial batch may not have flushed yet). The next run skips
  those. Previously: 0 survived, 1,000 redone.
- **More frequent Sheets API writes during the run.** With
  `PL_FLUSH_THRESHOLD = 50`, a 4,000-post run does ~80 flush API calls
  instead of 1. Sheets allows ~60 writes/min/user, well above what 80
  spread-out flushes consume.
- **Slight per-flush overhead** (~1-2 seconds for the API call). With
  a typical run already running for 30+ minutes, the cumulative
  overhead is minor (~2-3 minutes for ~80 flushes).
- **`Processed_Log` rows now appear during the run, not only at end.**
  Operators watching the Sheet during a run will see entries growing
  in real time. This is a visible behavior change.
- **No data integrity risk.** The flush writes the same content the
  end-of-run write was producing. Idempotent: running multiple flushes
  doesn't create duplicates because `_flushed_pids` tracks what's been
  written.

## What this does NOT do

- **Does not address `All_Events` writes.** `All_Events` still gets
  written once at the end of `save_data()`. That's a much bigger
  payload (events, not just IDs) and the rationale for incremental
  writes is weaker — All_Events writes happen after PL writes in
  `save_data()`, so a kill between PL and All_Events still loses the
  events. Future work could mirror this pattern for All_Events; for
  now the value is much smaller because PL is what drives dedup
  decisions on the next run.
- **Does not change the eventual end-of-run behavior.** A run that
  completes normally writes the same total rows to `Processed_Log` as
  before. The only difference is timing.
- **Does not prevent kills.** Just preserves work when they happen.

## Verification

Unit tests (in-script, with stubbed gspread) verify:
- Below threshold + `force=False` → no API call
- Above threshold + `force=False` → exactly one API call with all pending entries
- Re-running with no new entries → no API call (idempotent)
- New entries added after a flush + `force=True` → API call with only the new entries

A real-world test happens automatically on the next pipeline run: the
log will show `✓ Flushed N entries to Processed_Log` lines during the
run rather than only at the end.

## Future agents — read this before changing the flush threshold

- **Don't lower `PL_FLUSH_THRESHOLD` to 1.** That's a flush per post,
  which means an API call per post — the same rate-limit problem we
  fixed in the cleanup script. The threshold of 50 is calibrated to
  stay under Sheets quota and amortize per-call overhead across many
  posts.
- **Don't remove the `_flushed_pids` tracking.** Without it, every
  flush would re-write all post_results entries, creating duplicate
  rows in Processed_Log.
- **Don't move the flush call inside `process_post()`.** Calling it
  from `run_pipeline` (one place) is cleaner than scattering calls
  inside the multi-branch `process_post()`.
