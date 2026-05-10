# Tooling Catalog

This page lists every script in the repo, what it does, and when to use it.
**Start here when you're not sure which tool to run.**

---

## Production pipeline

### `main.py`
Production pipeline: Apify scrape → OCR → Gemini → Sheets. Runs weekly
via Replit cron. Use this for normal end-to-end runs.

**Use when:** You want a fresh scrape of all configured accounts.
**Don't use when:** You only need to re-extract a known list of posts
(use `events_from_ids.py`) or you only need to upload an existing CSV
(use `events_from_ids.py --from-events`).

---

## Recovery & re-extraction

### `events_from_ids.py`  *(replaces recover_events.py and upload_recovery.py)*
Two modes in one tool. See `docs/decisions/0011-events-from-ids.md` for design.

**Mode 1 — extract from post-ID list (`--from-ids <csv>`):**
Re-extracts events for a known list of post IDs by looking them up in
provided Apify dataset(s) and running OCR + Gemini through a 3-tier
ladder (Flash-Lite text → Flash-Lite image → Pro image).

Use when:
- Recovering orphaned posts (Processed_Log says `events_found` but
  All_Events has no rows)
- Manually re-extracting a post the team flagged as wrong
- A/B testing a prompt or model change on a fixed post set
- Pro fallback retry on suspect Flash-Lite extractions

**Mode 2 — upload existing CSV (`--from-events <csv>`):**
Uploads a pre-extracted Events_*.csv to All_Events with dedup. No
re-extraction. Used when `save_data()` wrote the CSV but the Sheets
push step failed.

Use when:
- Log shows `⚠ Sheets Upload Error` after a run, but the CSV exists in `outputs/`

**Don't use when:**
- You need a fresh Apify scrape (use `main.py`)
- You need weekend-specific re-scraping (use `reprocess_weekends.py`)

### `reprocess_weekends.py`
Re-scrapes weekend posts (Fri/Sat/Sun) for the next 3 calendar weeks
via fresh Apify scrape, then runs the full pipeline.

**Use when:** You want to refresh weekend data specifically.
**Don't use when:** You're trying to recover a specific list of posts
(use `events_from_ids.py --from-ids`).

---

## Analytics (called by main.py + standalone)

### `recurring_accounts.py`
Mines historical event CSVs to identify accounts whose posts contain
recurring-event language ("Fridays", "Weekly", "Every Saturday", etc.).
Writes results to `Reliable_Accounts` tab and promotes top accounts to
the live `Accounts` tab so they get scraped on subsequent runs.

**Use when:** Analyzing which accounts reliably produce events.
**Auto-runs:** As part of `save_data()` at end of every pipeline run.

### `account_hygiene.py`
Audits Accounts tab handles to detect dead/typo'd profiles.

**Use when:** Cleaning up the active account list. Run periodically.

### `audit.py`
End-of-run silent-failure detector. Identifies handles in the Accounts
tab that returned ZERO posts from Apify, classifies them
(PRODUCTIVE_BEFORE / NEVER_PRODUCED / IG_404), writes to `Run_Audit` and
`Silent_Failures` tabs.

**Auto-runs:** Called by `main.py` at end of every pipeline run.
**Don't run manually** unless debugging the audit logic itself.

### `ig_lookup.py`
Helper module for Instagram handle lookups. Used by other scripts
(notably `audit.py`). Not a standalone tool.

---

## Migration / one-off

### `migrate_history.py`
Historical data migration utility. Likely one-off; status TBD.

---

## Deleted tools (do not look for these)

- ~~`recover_events.py`~~ — absorbed into `events_from_ids.py --from-events`
- ~~`upload_recovery.py`~~ — unsafe earlier version of `recover_events.py`,
  deleted along with its successor

---

## Quick decision tree

```
"I want to..."

├─ Run a normal weekly extraction
│  └─ main.py
│
├─ Recover orphaned posts (events_found but no rows in All_Events)
│  └─ events_from_ids.py --from-ids <orphan_queue.csv>
│
├─ Re-extract a single post / small set after team flagged a wrong extraction
│  └─ events_from_ids.py --from-ids <small_list.csv>
│
├─ Upload a CSV to All_Events (Sheets push failed during a run)
│  └─ events_from_ids.py --from-events <events.csv>
│
├─ Refresh weekend data with a fresh Apify scrape
│  └─ reprocess_weekends.py
│
├─ Audit which accounts went silent
│  └─ Read the Silent_Failures tab (auto-populated by audit.py)
│
└─ Find recurring-event accounts
   └─ Read the Reliable_Accounts tab (auto-populated by recurring_accounts.py)
```
