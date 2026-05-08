# Changelog

Notable changes to the Instagram event extraction pipeline. Newest first.
Each entry is dated and links to a decision record where the *why* is non-obvious.

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
