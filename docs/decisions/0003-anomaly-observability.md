# ADR 0003: Per-run anomaly summary + raw Apify dump

Date: 2026-05-07
Status: Accepted

## Context

The pipeline was failing silently in places that mattered. When a post landed
in `no_events_found` or `gemini_error` state, that fact was logged to the
`Processed_Log` Sheet but the *reason* — the caption, the OCR text, the
Gemini response that led to the verdict — was thrown away. Forensics on a
missed event (e.g. the March 30 Interlude post) required re-scraping or
reasoning from absence.

Apify's response — including carousel image URLs, post date, location tags,
all child posts — was loaded into memory, processed, and then discarded.
This made post-hoc questions like "did Apify return this post on April 16?"
unanswerable without burning Apify quota on a re-scrape.

The pipeline's `print()` output was visible only in Replit's live shell
buffer, which rotates. Once a run finished, you couldn't read what it had
printed.

## Decision

Three additive changes, all written to `outputs/`. None affects the
extraction logic; all are pure data persistence at points where data was
previously discarded.

1. **Per-run log file** — `outputs/run_<run_id>.log`. `stdout` and `stderr`
   are wrapped with a `_TeeOutput` class at pipeline `__init__` so every
   `print()` is mirrored to disk. Console behaviour is unchanged.

2. **Anomaly summary** — `outputs/anomalies_<run_id>.json`. Every post
   outcome now flows through a single helper, `_record_post_outcome()`,
   which captures `caption_preview` (≤400 chars), `ocr_preview` (≤400
   chars), `gemini_raw` (≤600 chars), and a free-form `error` / reason
   string for non-success outcomes. At process exit (`atexit`), the
   accumulated per-post outcomes are written to JSON along with per-account
   scrape→extract counts. Success-path posts are NOT bloated with preview
   text — the helper filters that to anomalies only.

3. **Raw Apify dump** — `outputs/apify_raw_<timestamp>.json`. After the
   Apify dataset response is downloaded but before extraction begins, the
   raw payload is written to disk verbatim along with run metadata
   (`run_id`, `dataset_id`, `usernames_requested`, `posts_per_profile_cap`,
   `newer_than`).

A bonus regression check compares per-account event counts against the
most recent prior `Events_*.csv` and prints a warning block for accounts
that previously produced events but produced zero this run.

## Consequences

- **Forensics drops from hour-scale to seconds.** "Why was this post
  missed?" becomes a `grep` against `outputs/anomalies_*.json` and
  `outputs/apify_raw_*.json` rather than re-scraping or reading code.
- **Storage growth is bounded and small.** Anomaly JSONs run ~500KB-1MB
  per weekly run; raw Apify dumps run ~1-2MB. ~150MB/year combined. Replit
  has plenty of headroom. If we ever need to prune, last-N retention is
  trivial.
- **No behaviour change.** All 7 anomaly hook sites in `process_post` now
  call `_record_post_outcome()` instead of mutating `processed_posts` and
  `post_results` directly. The semantics are identical; the helper just
  also captures debug previews. Pipeline can be rolled back with a single
  `git revert` and would be functionally identical to the prior state.
- **`atexit` ensures the summary writes on crashes and Ctrl-C.** Partial
  data is better than no data when diagnosing a failure mode.

## What this does NOT solve

- The bare-`except` problem in OCR and elsewhere. Many exceptions still get
  swallowed silently. Future work, deferred — this ADR is the cheap
  observability layer first, not a refactor.
- Structured logging with severity levels. The log file is a literal copy
  of stdout. Switching to `logging` module with levels is future work.
- Active alerting. Currently the user must read the anomaly file and
  regression warnings; nothing pages them. Could be wired up to a Slack
  webhook later if useful.

## Future agents — read this before you "clean up" silent failures

These three artifacts (`run_*.log`, `anomalies_*.json`, `apify_raw_*.json`)
are **the** observability surface for the pipeline as of this ADR. Don't
remove them. Don't move them out of `outputs/`. Don't strip the
`_record_post_outcome` capture fields under the assumption "no one reads
this." The user does, when an event is missed.
