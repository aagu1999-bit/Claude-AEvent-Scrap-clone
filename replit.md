# Claude-AEvent-Scrap - Instagram Event Extraction Pipeline

## Overview
Python-based pipeline that extracts event information from Instagram posts using Google Gemini AI for natural language processing and Google Cloud Vision API for OCR text extraction from images. Results sync to Google Sheets and save locally as CSV/Excel.

## Project Status
- **Current State**: Fully configured and running on schedule
- **Language**: Python 3.11
- **Dependencies**: Managed via uv (pyproject.toml)
- **Workflow**: Event Pipeline (console-based, scheduled)
- **AI Model**: gemini-2.5-flash-lite (paid tier)

## Features
- Parallel processing with configurable workers (up to 20)
- Multi-event extraction from single posts (calendars, weekly lineups)
- Image OCR using Google Cloud Vision API
- Permanent Instagram URL generation
- CSV, Excel output + Google Sheets sync
- Checkpoint/resume support for large batches
- Thread-safe rate limiting and error handling
- Google Sheets history tracking (Processed_Log tab) to skip duplicates
- Scheduled weekly runs via config.json
- Verbose logging: OCR status, event details, calendar detection, stats
- Reliable Accounts detection: scores accounts on recurring event signals, writes Reliable_Accounts tab to Google Sheets after each run
- Smart history: Processed_Log tags each post with result (events_found/no_events_found/ocr_failed/gemini_error); OCR/Gemini failures are retried on next run; posts older than `history_max_age_days` always skipped
- **Live Apify scrape**: Weekly pipeline triggers a fresh `apify/instagram-post-scraper` run at startup using the account list from the "Accounts" tab in the Google Sheet (falls back to `accounts.json` if the tab is missing/empty, falls back to `instagram_data_url` if Apify is disabled or fails)
- **Sheet-managed accounts**: Add/remove Instagram accounts directly in the "Accounts" tab of the Google Sheet — no file editing needed. On first run, the tab is auto-created and pre-loaded from `accounts.json`

## Project Structure
```
/
├── main.py                                          # Main pipeline (scheduler + processing)
├── instagram_event_pipeline.py                      # Original pipeline class (reference)
├── original.py                                      # Original entry point (reference)
├── config.json                                      # Schedule, model, workers, Apify config
├── accounts.json                                    # Instagram usernames to scrape (901 accounts)
├── migrate_history.py                               # One-time history migration script
├── reprocess_weekends.py                            # Re-scrape weekend posts with fresh data
├── recurring_accounts.py                            # Reliable Accounts detection + Sheets sync
├── apt-mark-468506-u9-ec44cabc7335 copy.json       # Google service account
├── pyproject.toml                                   # Python dependencies
├── outputs/                                         # Generated output files
└── .gitignore                                       # Git ignore rules
```

## Configuration

### config.json
```json
{
  "schedule_day": "Thursday",
  "schedule_time": "20:16",
  "apify_enabled": true,
  "apify_posts_per_profile": 9,
  "apify_newer_than_days": 7,
  "instagram_data_url": "https://api.apify.com/...",
  "sheet_name": "Instagram_Events_Master",
  "gemini_model": "gemini-2.5-flash-lite",
  "max_workers": 3,
  "rate_limit_delay": 1
}
```

### Accounts tab (Google Sheet) — primary source
The "Accounts" tab in the `Instagram_Events_Master` Google Sheet is the primary list of Instagram usernames (no `@`) to scrape each week. Add or remove rows directly in the sheet.

- **Header row**: `Username`
- On the very first run, the tab is auto-created and pre-loaded from `accounts.json` (901 usernames)

### accounts.json — fallback only
Still present as a backward-compatible fallback if the sheet tab is unreachable. No longer needs to be edited directly.

### Required Secrets
| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | Yes | Google Gemini API key (paid tier) |
| `APIFY_API_KEY` | Yes | Apify API token for live scraping |

### Optional Environment Overrides
| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULE_DAY` | Thursday | Day of week to run |
| `SCHEDULE_TIME` | 14:00 | Time to run (HH:MM) |
| `DATA_URL` | config.json | Override instagram_data_url |
| `MAX_WORKERS` | 10 | Parallel workers (1-20) |
| `RATE_LIMIT_DELAY` | 0.5 | Delay between API calls (seconds) |

## Running the Pipeline

### Scheduled (default)
The Event Pipeline workflow runs `main.py` which waits for the configured day/time, then processes all posts automatically.

### Force Run
Option 1 — In config.json, set `"run_now": true`, then restart the workflow. It will run immediately and automatically reset back to `false` when done.

Option 2 — From the Shell:
```bash
python main.py --now
```

## Output Files
Pipeline generates:
- `Events_YYYYMMDD_HHMMSS.csv` - Event data in CSV format
- `Events_YYYYMMDD_HHMMSS.xlsx` - Event data in Excel format
- `stats_YYYYMMDD_HHMMSS.json` - Processing statistics
- Google Sheets: All_Events tab + Processed_Log tab + Reliable_Accounts tab
- `outputs/reliable_accounts.csv` - Recurring/reliable accounts watchlist

## Recent Changes
- **2026-04-27**: Accounts list now managed from Google Sheet "Accounts" tab
  - New `load_usernames_from_accounts_sheet()` function reads usernames from the "Accounts" tab
  - On first run: tab is auto-created and pre-loaded with all 901 usernames from `accounts.json`
  - `fetch_posts_via_apify()` now tries the Sheet first, falls back to `accounts.json` if tab missing/empty
  - `accounts.json` kept as backward-compatible fallback only
- **2026-04-27**: Live Apify scrape for weekly pipeline
  - Weekly pipeline now triggers a fresh `apify/instagram-post-scraper` run at startup
  - `accounts.json` added with 901 pre-loaded Instagram usernames
  - New config fields: `apify_enabled`, `apify_posts_per_profile`, `apify_newer_than_days`
  - Falls back to `instagram_data_url` if Apify is disabled, fails, or returns empty
  - Confirmed field names from real run: `username` (array), `resultsLimit`, `onlyPostsNewerThan`
- **2026-03-12**: Added run_now toggle to config.json for triggering immediate runs without Shell commands
- **2026-02-10**: URL fix + processed timestamp
  - Fixed uppercase conversion breaking Instagram links (URLs now keep original casing)
  - Added 'processed_timestamp' column at end of output data
- **2026-02-05**: Performance + logging overhaul
  - Increased parallel workers to 10 (from 3)
  - Restored full verbose logging from original pipeline
  - Thread-safe rate_limit_delay updates for high worker counts
  - Added "already processed" skip logging
  - Comprehensive final report with stats/percentages
  - Updated gemini model to gemini-2.0-flash-lite (stable)
  - History migration: 14,533 post IDs uploaded to Google Sheets
- **2025-11-28**: Imported from GitHub and configured for Replit environment

## User Preferences
- Console-based workflow (no web frontend)
- API keys stored in Secrets
- Paid Gemini API tier (supports high worker count)
- Prefers fast processing with detailed logging
- Config changes via config.json
