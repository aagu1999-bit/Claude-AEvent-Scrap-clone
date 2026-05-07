# Changelog

Notable changes to the Instagram event extraction pipeline. Newest first.
Each entry is dated and links to a decision record where the *why* is non-obvious.

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
