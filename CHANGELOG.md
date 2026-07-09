# Changelog

Notable changes to the Instagram event extraction pipeline. Newest first.
Each entry is dated and links to a decision record where the *why* is non-obvious.

## 2026-07-09 (screening throughput: adaptive rate-limit recovery + lazy image downloads)

Addresses the reported regression where a 6,000-post run took ~6 hours with
10 workers via the UI, while 5 workers from the shell historically finished
in ~2 hours. Three compounding causes, all in `main.py`:

### Fixed
- **`rate_limit_delay` now decays back to its configured base after
  successful API calls.** Previously it only ratcheted UP (×1.5 per 429,
  capped at 5.0s) and never recovered — one 429 burst early in a long run
  left every subsequent Vision call (per carousel slide!) and per-post
  Gemini call sleeping 5s for the rest of the run. At 6,000 posts that is
  hours of dead sleep. More workers made it *worse*: 10 workers hit the
  quota harder, pinning the delay at max sooner — the counterintuitive
  "10 workers slower than 5" effect. Successful calls now walk the delay
  back (×0.9 per success, floor = configured `rate_limit_delay`).
- **Gemini 429s are retried in place instead of escalating the tier
  ladder.** `_call_gemini_tier` used to swallow a 429 and return `None`,
  which the ladder read as "zero events" → escalate to the next tier —
  burning up to 4 Gemini calls per post into an already-exhausted quota
  and amplifying the storm. Rate-limit errors now retry the SAME tier up
  to 2 more times with the escalated adaptive delay.
- **Carousel images are downloaded lazily in the tier ladder.**
  `_process_with_tier_ladder` eagerly downloaded every slide for every
  post before Tier 1 ran; most posts resolve caption-only and never
  needed them. Downloads now happen only when an image tier (2 or 4)
  actually runs; Tier 2/4 still share one download via the existing
  per-post cache.

## 2026-05-08 (incremental Processed_Log writes)

### Changed
- **`Processed_Log` is now written incrementally during the run**, not only
  at end-of-run. Previously, if a run was killed before `save_data()` ran
  (e.g., the May 7 18:24-26 kills), 100% of that run's dedup work was lost
  and re-processed on the next run. Now the pipeline flushes
  `post_results` entries every 50 new posts via a new
  `_flush_processed_log()` helper. A killed run now preserves
  ~950+ of every 1,000 processed posts' dedup state. The final
  `save_data()` does a `force=True` flush to capture the remainder. See
  [docs/decisions/0010-incremental-processed-log-writes.md](docs/decisions/0010-incremental-processed-log-writes.md).

## 2026-05-08 (lessons doc)

### Added
- **`docs/decisions/0009-pipeline-reliability-lessons.md`** — fourteen
  transferable lessons from the May 7-8 incident investigation. Future
  agents working on this pipeline (or similar data-extraction systems)
  should read this before starting any non-trivial investigation.
  Specifics of May 7-8 are evidence; this doc is the takeaway.

## 2026-05-08 (skip Apify shell records)

### Fixed
- **No more new pseudo-ID rows in `Processed_Log`.** When Apify returns a
  "shell" record (account that produced no posts — private, suspended,
  deleted, typo'd, or zero recent posts), `process_post()` previously
  fabricated a synthetic ID like `post_142` and processed the empty record
  anyway. Now those records are detected at entry and skipped before any
  OCR/Gemini work happens, with the affected account surfaced in the run
  report.

### Added
- **Dead-handle visibility in the per-run report.** When shell records are
  detected, the final report block lists the top 20 affected accounts by
  frequency so the operator can review their `Accounts` tab. Full
  per-account map is in `anomalies_<ts>.json` under `apify_shell_accounts`.
- New stat key `apify_shell_records` counts shell entries per run.

See [docs/decisions/0008-skip-apify-shell-records.md](docs/decisions/0008-skip-apify-shell-records.md)
for the empirical evidence (133 of 4,014 dataset entries on 2026-05-08
were shells) and the rationale.

## 2026-05-08 (race-condition fix)

### Fixed
- **Duplicate `(POST ID, EVENT NAME, DATE)` rows in `All_Events`** caused by
  a race condition in `process_post()`. The dedup check and the dedup add
  were in two separate lock blocks separated by 10–30s of OCR/Gemini work,
  so concurrent threads with the same post ID could both pass the check.
  The May 8 reprocess produced 47 duplicate composite keys (55 excess rows)
  via this path. Fix: atomic check-and-claim — `processed_posts.add(pid)`
  now happens inside the same lock block as the existence check, so the
  second thread sees the claimed pid and skips. See
  [docs/decisions/0006-dedup-race-condition.md](docs/decisions/0006-dedup-race-condition.md).
- **Duplicate post IDs in `reprocess_weekends.py` source list** are now
  removed before the pipeline starts. Belt to the atomic-claim's suspenders
  — handles the case where the same post appears twice in the input list.

### TODO (separate PR-C)
- One-off cleanup script for the existing duplicate rows in `All_Events`.
  Backed up to a `Cleanup_Backup` tab before deletion.
## 2026-05-08

### Added
- **`docs/config.md`** — full reference for every setting in `config.json`,
  grouped by Mode 1 / Mode 2 / Universal scope. Future agents should read
  this before editing config defaults.

### Changed
- **`history_max_age_days` is now Mode-1 only.** Previously applied
  universally on every pipeline start. In Mode 2 (static `instagram_data_url`),
  this caused posts to silently re-enter processing each week as they aged
  past the cutoff, generating duplicate rows in `All_Events`. The cutoff
  now applies only when `apify_enabled: true`. See
  [docs/decisions/0005-config-mode-scoping.md](docs/decisions/0005-config-mode-scoping.md).
- **Scheduler timezone is now explicit (`America/New_York`).** Previously
  `start_scheduler()` used `datetime.now()` with no timezone, picking up the
  Replit container's undeclared local TZ. To change, edit `SCHEDULE_TZ` in
  `main.py` `start_scheduler()`.

## 2026-05-07 (later)

### Added
- **Per-run log file** at `outputs/run_<timestamp>.log` — stdout/stderr is now
  teed to disk so post-mortem forensics can read what the run actually
  printed without needing access to Replit's live shell buffer.
- **Anomaly summary** at `outputs/anomalies_<timestamp>.json` — for every
  post that did NOT produce events (no_events_found / gemini_error /
  ocr_failed), captures: post URL, account, post date, caption preview,
  OCR preview, Gemini raw response, and a `reason` field naming the exact
  failure mode. Plus per-account scrape→extract counts. Written via
  `atexit` so it survives crashes and Ctrl-C.
- **Regression alert** — at end of run, compares per-account event counts
  against the most recent prior `Events_*.csv` and prints a `⚠ POSSIBLE
  REGRESSIONS` block listing accounts that produced ≥2 events last run
  but zero this run. Cheap signal for typos / suspended accounts /
  extraction regressions.
- **Raw Apify dump** at `outputs/apify_raw_<timestamp>.json` — saves the
  Apify API response verbatim so future "why was this post missed?"
  questions can be answered with `grep` instead of a re-scrape. ~1-2MB
  per run. See [docs/decisions/0003-anomaly-observability.md](docs/decisions/0003-anomaly-observability.md).

## 2026-05-07

### Added
- `recurring_accounts.py` now syncs reliable accounts into the live `Accounts`
  tab so they actually get scraped on subsequent runs. Previously, an account
  could appear in `reliable_accounts.csv` with 30+ historical events and never
  be in the active list. See
  [docs/decisions/0001-reliable-to-active-promotion.md](docs/decisions/0001-reliable-to-active-promotion.md).
- Typo detector — flags `Accounts` tab handles that look like near-misspellings
  of a known reliable handle (Levenshtein distance ≤ 2). Catches silent fetch
  failures of the `interludeseries` → `interludseries` variety.
- `recurring_accounts.py --dry-run`, `--no-promote`, `--threshold N`,
  `--recency-days N` flags.

### Changed
- `apify_newer_than_days` default raised from 7 → 14 to match how runs are
  performed manually. See
  [docs/decisions/0002-apify-lookback-window.md](docs/decisions/0002-apify-lookback-window.md).
