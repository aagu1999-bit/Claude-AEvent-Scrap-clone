#!/usr/bin/env python3
"""
reprocess_weekends.py

Re-scrapes and re-processes weekend posts (Fri/Sat/Sun) for the next 3 calendar weeks
with full carousel support. Removes stale rows from All_Events and Processed_Log,
then runs fresh Apify scrapes and the full pipeline (OCR + Gemini + Sheets upload).

Usage:
    python reprocess_weekends.py           # preview + confirm prompt
    python reprocess_weekends.py --confirm # skip confirmation prompt, run immediately
"""

import os
import sys
import time
import argparse
import re
import requests
from datetime import datetime, date, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
APIFY_BASE_URL = "https://api.apify.com/v2"
APIFY_ACTOR = "apify~instagram-scraper"


# ─────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────

def get_next_3_weekend_dates():
    """Return sorted list of date objects for every Fri/Sat/Sun in the next 3 calendar weeks."""
    today = date.today()
    weekend_days = []
    weeks_seen = set()
    for offset in range(30):
        d = today + timedelta(days=offset)
        if d.weekday() in (4, 5, 6):
            iso_week = d.isocalendar()[1]
            weeks_seen.add(iso_week)
            if len(weeks_seen) <= 3:
                weekend_days.append(d)
    return weekend_days


# ─────────────────────────────────────────────
# Sheets connection
# ─────────────────────────────────────────────

def connect_sheets():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}")
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    try:
        spreadsheet = client.open(SHEET_NAME)
        print(f"✓ Connected to Google Sheet: {SHEET_NAME}")
        return client, spreadsheet
    except gspread.SpreadsheetNotFound:
        print(f"❌ Google Sheet not found: {SHEET_NAME}")
        sys.exit(1)


# ─────────────────────────────────────────────
# Read All_Events: find target post IDs, then
# collect ALL rows belonging to those posts
# ─────────────────────────────────────────────

def _col_index(header, *names):
    """Return first matching column index (case-insensitive) or None."""
    for name in names:
        for i, h in enumerate(header):
            if h.strip().upper() == name.upper():
                return i
    return None


def find_rows_to_reprocess(spreadsheet, weekend_dates):
    """
    Phase 1: find all rows in All_Events whose DATE falls on a target weekend.
    Phase 2: expand to ALL rows in All_Events that share those post IDs,
             ensuring no stale non-weekend rows from the same post are left behind.

    Returns:
        header         — list of column names
        all_data_rows  — all data rows (list of lists, no header)
        rows_to_delete — list of dicts describing every row that will be deleted
                         (includes both weekend-dated AND sibling rows from same posts)
        post_id_to_url — mapping of post_id -> instagram_post_url for re-scraping
        post_id_to_handle — mapping of post_id -> instagram handle
    """
    try:
        ws = spreadsheet.worksheet("All_Events")
    except gspread.WorksheetNotFound:
        print("❌ Worksheet 'All_Events' not found in the spreadsheet.")
        sys.exit(1)

    all_values = ws.get_all_values()
    if not all_values:
        print("⚠ All_Events is empty.")
        return [], [], [], {}, {}

    header = all_values[0]
    all_data_rows = all_values[1:]

    date_col     = _col_index(header, "DATE", "EVENT DATE")
    post_id_col  = _col_index(header, "POST ID")
    post_url_col = _col_index(header, "INSTAGRAM POST URL")
    handle_col   = _col_index(header, "INSTAGRAM HANDLE", "HANDLE")
    event_col    = _col_index(header, "EVENT NAME")

    if date_col is None:
        print(f"❌ Could not find DATE column in All_Events header: {header}")
        sys.exit(1)
    if post_id_col is None:
        print(f"❌ Could not find POST ID column in All_Events header: {header}")
        sys.exit(1)

    weekend_date_strs = {str(d) for d in weekend_dates}

    # Phase 1: collect post IDs that have at least one weekend-dated row
    _DATE_FMTS = ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%B %d, %Y')

    def _parse_date(s):
        for fmt in _DATE_FMTS:
            try:
                return str(datetime.strptime(s.strip(), fmt).date())
            except ValueError:
                continue
        return None

    weekend_post_ids = set()
    for row in all_data_rows:
        raw_date = row[date_col].strip() if len(row) > date_col else ""
        if not raw_date:
            continue
        parsed_date = _parse_date(raw_date)
        if parsed_date is None:
            continue
        if parsed_date in weekend_date_strs:
            pid = row[post_id_col].strip() if len(row) > post_id_col else ""
            if pid:
                weekend_post_ids.add(pid)

    if not weekend_post_ids:
        return header, all_data_rows, [], {}, {}

    # Phase 2: expand — collect ALL rows (any date) whose post ID is in weekend_post_ids
    rows_to_delete = []
    post_id_to_url = {}
    post_id_to_handle = {}

    for idx, row in enumerate(all_data_rows):
        if not row:
            continue
        pid = row[post_id_col].strip() if len(row) > post_id_col else ""
        if pid not in weekend_post_ids:
            continue

        raw_date = row[date_col].strip() if len(row) > date_col else ""
        post_url = (
            row[post_url_col].strip()
            if post_url_col is not None and len(row) > post_url_col
            else ""
        )
        handle = (
            row[handle_col].strip()
            if handle_col is not None and len(row) > handle_col
            else ""
        )
        event_name = (
            row[event_col].strip()
            if event_col is not None and len(row) > event_col
            else ""
        )
        sheet_row = idx + 2  # 1-based sheet row (header is row 1)

        rows_to_delete.append({
            "row_idx":    idx,
            "sheet_row":  sheet_row,
            "post_id":    pid,
            "post_url":   post_url,
            "handle":     handle,
            "date":       raw_date,
            "event_name": event_name,
            "row_data":   row,
        })

        if post_url and pid not in post_id_to_url:
            post_id_to_url[pid] = post_url
        if handle and pid not in post_id_to_handle:
            post_id_to_handle[pid] = handle

    return header, all_data_rows, rows_to_delete, post_id_to_url, post_id_to_handle


# ─────────────────────────────────────────────
# Backup
# ─────────────────────────────────────────────

def backup_rows(spreadsheet, header, rows_to_delete):
    """Write all rows-to-delete to Reprocess_Backup tab (fresh each run)."""
    try:
        ws = spreadsheet.worksheet("Reprocess_Backup")
        ws.clear()
        print("  ↳ Cleared existing Reprocess_Backup tab")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet("Reprocess_Backup", rows=5000, cols=30)
        print("  ↳ Created new Reprocess_Backup tab")

    rows_to_write = [header] + [r["row_data"] for r in rows_to_delete]
    ws.update(rows_to_write, value_input_option="RAW")
    print(f"✓ Backed up {len(rows_to_delete)} rows to Reprocess_Backup")


# ─────────────────────────────────────────────
# Delete from All_Events (post-ID based)
# ─────────────────────────────────────────────

def delete_from_all_events(spreadsheet, rows_to_delete):
    """Delete rows from All_Events using batched batchUpdate (avoids per-row quota limits)."""
    ws = spreadsheet.worksheet("All_Events")
    sheet_rows = sorted({r["sheet_row"] for r in rows_to_delete}, reverse=True)

    # Group consecutive row numbers into ranges to minimise request count.
    # Sorted descending so that earlier deletions don't shift later indices.
    range_requests = []
    i = 0
    while i < len(sheet_rows):
        high = sheet_rows[i]
        low = high
        while i + 1 < len(sheet_rows) and sheet_rows[i] - sheet_rows[i + 1] == 1:
            i += 1
            low = sheet_rows[i]
        # Sheets API uses 0-based startIndex, exclusive endIndex
        range_requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": low - 1,
                    "endIndex": high,
                }
            }
        })
        i += 1

    # Execute in batches of 500 (well within the 10 MB request size limit).
    # Each batchUpdate call counts as ONE write request regardless of how many
    # operations it contains, so this stays far below the 60-writes/min quota.
    batch_size = 500
    for start in range(0, len(range_requests), batch_size):
        batch = range_requests[start : start + batch_size]
        spreadsheet.batch_update({"requests": batch})
        if start + batch_size < len(range_requests):
            time.sleep(1)

    print(f"✓ Deleted {len(sheet_rows)} rows from All_Events ({len(range_requests)} range requests)")


# ─────────────────────────────────────────────
# Unlock from Processed_Log
# ─────────────────────────────────────────────

def unlock_from_processed_log(spreadsheet, post_ids_to_remove):
    """Remove matching post IDs from Processed_Log so pipeline re-processes them."""
    try:
        ws = spreadsheet.worksheet("Processed_Log")
    except gspread.WorksheetNotFound:
        print("⚠ Processed_Log not found — skipping unlock step")
        return 0

    all_values = ws.get_all_values()
    if not all_values:
        return 0

    header = all_values[0]
    data_rows = all_values[1:]

    pid_col = _col_index(header, "POST ID") or 0
    ids_to_remove = {pid.strip() for pid in post_ids_to_remove if pid}

    rows_to_delete = []
    for idx, row in enumerate(data_rows):
        pid = row[pid_col].strip() if len(row) > pid_col else ""
        if pid in ids_to_remove:
            rows_to_delete.append(idx)

    # Build batched deleteDimension requests (1 API call per 500 ranges)
    sheet_rows_desc = sorted({idx + 2 for idx in rows_to_delete}, reverse=True)
    range_requests = []
    i = 0
    while i < len(sheet_rows_desc):
        high = sheet_rows_desc[i]
        low = high
        while i + 1 < len(sheet_rows_desc) and sheet_rows_desc[i] - sheet_rows_desc[i + 1] == 1:
            i += 1
            low = sheet_rows_desc[i]
        range_requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": low - 1,
                    "endIndex": high,
                }
            }
        })
        i += 1

    batch_size = 500
    for start in range(0, len(range_requests), batch_size):
        batch = range_requests[start : start + batch_size]
        spreadsheet.batch_update({"requests": batch})
        if start + batch_size < len(range_requests):
            time.sleep(1)

    print(f"✓ Unlocked {len(rows_to_delete)} Processed_Log entries for {len(ids_to_remove)} post ID(s)")
    return len(rows_to_delete)


# ─────────────────────────────────────────────
# Apify re-scrape
# ─────────────────────────────────────────────

def apify_rescrape(post_urls, apify_token):
    """
    Submits a batch Apify run for the given post URLs.
    Waits for completion and returns the raw dataset items list.
    """
    ig_url_re = re.compile(
        r"^(https://)?(www\.)?instagram\.com/[A-Za-z0-9._-]+(/.*)?$"
    )
    all_urls = list(dict.fromkeys(u for u in post_urls if u))
    unique_urls = [u for u in all_urls if ig_url_re.match(u.strip())]
    skipped = len(all_urls) - len(unique_urls)
    if skipped:
        print(f"  ⚠ Skipped {skipped} URL(s) that didn't match Instagram URL pattern")
    if not unique_urls:
        print("⚠ No valid post URLs to scrape.")
        return []

    print(f"\n🚀 Submitting Apify run for {len(unique_urls)} post URL(s)...")

    run_url = f"{APIFY_BASE_URL}/acts/{APIFY_ACTOR}/runs"
    payload = {
        "directUrls": unique_urls,
        "resultsType": "posts",
        "resultsLimit": 200,
    }
    resp = requests.post(
        run_url,
        params={"token": apify_token},
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ↳ Apify error {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    run_data = resp.json()

    run_id = run_data["data"]["id"]
    dataset_id = run_data["data"]["defaultDatasetId"]
    print(f"  ↳ Run ID:     {run_id}")
    print(f"  ↳ Dataset ID: {dataset_id}")
    print(f"  ↳ Waiting for Apify run to finish...")

    poll_url = f"{APIFY_BASE_URL}/actor-runs/{run_id}"
    while True:
        time.sleep(10)
        poll_resp = requests.get(poll_url, params={"token": apify_token}, timeout=30)
        poll_resp.raise_for_status()
        status = poll_resp.json()["data"]["status"]
        print(f"    Status: {status}")
        if status in ("SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"):
            break

    if status != "SUCCEEDED":
        print(f"❌ Apify run ended with status: {status}")
        return []

    dataset_url = f"{APIFY_BASE_URL}/datasets/{dataset_id}/items"
    dataset_resp = requests.get(
        dataset_url,
        params={"token": apify_token, "format": "json", "clean": "false"},
        timeout=60,
    )
    dataset_resp.raise_for_status()
    items = dataset_resp.json()
    print(f"✓ Downloaded {len(items)} item(s) from Apify dataset")
    return items


# ─────────────────────────────────────────────
# Run pipeline on fresh data
# ─────────────────────────────────────────────

def run_pipeline_on_fresh_data(fresh_posts):
    """
    Instantiates InstagramEventPipeline, clears its processed_posts set so all
    fresh posts are treated as new, then runs OCR + Gemini + Sheets upload.
    """
    from main import InstagramEventPipeline

    pipeline = InstagramEventPipeline()
    pipeline.setup_sheets()

    if not pipeline.sheets_client or not pipeline.main_sheet:
        print("⚠ Could not connect pipeline to Sheets — results will be saved locally only")

    # Clear history so the re-scraped posts are processed fresh, not skipped
    pipeline.processed_posts = set()

    results, post_log = pipeline.run_pipeline(fresh_posts)
    pipeline.save_data(results, post_log)

    return results, post_log


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Re-process next 3 weekends' posts with full carousel support."
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt and run immediately.",
    )
    args = parser.parse_args()

    apify_token = os.environ.get("APIFY_API_KEY", "").strip()
    if not apify_token:
        print("❌ APIFY_API_KEY secret is not set. Please add it in Secrets.")
        sys.exit(1)

    print("\n" + "=" * 65)
    print(" REPROCESS WEEKENDS — Full Carousel Support")
    print("=" * 65)
    print(f"  Run date: {date.today()}")

    weekend_dates = get_next_3_weekend_dates()
    print(f"\n📅 Target weekend dates ({len(weekend_dates)} days across 3 weekends):")
    for d in weekend_dates:
        print(f"   • {d}  ({d.strftime('%A')})")

    print("\n🔗 Connecting to Google Sheets...")
    _, spreadsheet = connect_sheets()

    print("\n🔎 Scanning All_Events for weekend posts...")
    header, _, rows_to_delete, post_id_to_url, post_id_to_handle = find_rows_to_reprocess(
        spreadsheet, weekend_dates
    )

    # ── Recovery mode: rows already deleted in a prior interrupted run ────
    # If All_Events has no matching rows but Reprocess_Backup exists with data,
    # the deletion step completed previously but Steps 3-5 did not. Resume from
    # the backup so we still unlock Processed_Log and re-run the pipeline.
    resuming = False
    if not rows_to_delete:
        try:
            bk_ws = spreadsheet.worksheet("Reprocess_Backup")
            bk_values = bk_ws.get_all_values()
        except gspread.WorksheetNotFound:
            bk_values = []

        if len(bk_values) > 1:  # header + at least one data row
            bk_header = bk_values[0]
            bk_data   = bk_values[1:]
            pid_col   = _col_index(bk_header, "POST ID") or 0
            url_col   = _col_index(bk_header, "POST URL") or _col_index(bk_header, "instagram_post_url") or 1
            handle_col = _col_index(bk_header, "Account") or _col_index(bk_header, "account") or None

            for row in bk_data:
                pid  = row[pid_col].strip()  if len(row) > pid_col  else ""
                url  = row[url_col].strip()  if len(row) > url_col  else ""
                hdl  = (row[handle_col].strip() if handle_col is not None and len(row) > handle_col else "")
                if pid and pid not in post_id_to_url:
                    post_id_to_url[pid] = url
                if hdl and pid not in post_id_to_handle:
                    post_id_to_handle[pid] = hdl

            if post_id_to_url:
                print(f"\n♻️  RESUME MODE — Reprocess_Backup has {len(bk_data)} row(s) from a prior run.")
                print(f"   Steps 1 & 2 already completed. Continuing from Step 3 (unlock + re-scrape).")
                resuming = True
            else:
                print("\n✅ No weekend events found and no backup to resume from. Nothing to do.")
                sys.exit(0)
        else:
            print("\n✅ No weekend events found in All_Events for the target date range. Nothing to do.")
            sys.exit(0)

    unique_post_ids  = list(dict.fromkeys(r["post_id"]  for r in rows_to_delete if r["post_id"])) if not resuming else list(post_id_to_url.keys())
    unique_post_urls = list(dict.fromkeys(post_id_to_url.get(pid, "") for pid in unique_post_ids if post_id_to_url.get(pid)))
    unique_accounts  = list(dict.fromkeys(post_id_to_handle.get(pid, "") for pid in unique_post_ids if post_id_to_handle.get(pid)))
    posts_without_url = [pid for pid in unique_post_ids if not post_id_to_url.get(pid)]

    if not resuming:
        # ── Detailed preview ──────────────────────────────────────────────────
        print(f"\n{'─'*65}")
        print(f"📋 PREVIEW — exact rows that will be deleted from All_Events")
        print(f"{'─'*65}")
        print(f"{'Sheet Row':<12} {'Post ID':<25} {'Date':<12} {'Event Name':<35} Post URL")
        print(f"{'─'*65}")
        for r in rows_to_delete:
            event_display = (r["event_name"][:33] + "..") if len(r["event_name"]) > 35 else r["event_name"]
            pid_display   = (r["post_id"][:23]   + "..") if len(r["post_id"])   > 25 else r["post_id"]
            print(f"  row {r['sheet_row']:<7} {pid_display:<25} {r['date']:<12} {event_display:<35} {r['post_url']}")
        print(f"{'─'*65}")
    print(f"\n  Summary:")
    if not resuming:
        print(f"   • Total rows to delete from All_Events: {len(rows_to_delete)}")
    print(f"   • Unique post IDs:                      {len(unique_post_ids)}")
    print(f"   • Unique post URLs to re-scrape:        {len(unique_post_urls)}")
    print(f"   • Accounts affected:                    {len(unique_accounts)}")
    if unique_accounts:
        print(f"   • Accounts: {', '.join('@' + a for a in unique_accounts[:20])}")

    if posts_without_url:
        print(f"\n⚠ WARNING: {len(posts_without_url)} post ID(s) have no instagram_post_url and")
        print(f"  cannot be re-scraped. Their rows will still be deleted and backed up,")
        print(f"  but they will NOT appear in the fresh Apify results:")
        for pid in posts_without_url[:10]:
            print(f"    - {pid}")
        if len(posts_without_url) > 10:
            print(f"    ... and {len(posts_without_url) - 10} more")

    # ── Confirm ───────────────────────────────────────────────────────────
    if not args.confirm and not resuming:
        print("\n⚠ This will DELETE the rows above from All_Events and Processed_Log,")
        print("  then re-scrape each post fresh from Apify and re-run the full pipeline.")
        print("  Originals are backed up to Reprocess_Backup before anything is deleted.")
        try:
            answer = input("\nProceed? [yes/no]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(0)
        if answer not in ("yes", "y"):
            print("Aborted.")
            sys.exit(0)

    if not resuming:
        # ── Step 1: Backup ────────────────────────────────────────────────────
        print("\n💾 Step 1 — Backing up affected rows to Reprocess_Backup...")
        backup_rows(spreadsheet, header, rows_to_delete)

        # ── Step 2: Delete from All_Events ───────────────────────────────────
        print("\n🗑  Step 2 — Deleting rows from All_Events (post-ID based)...")
        delete_from_all_events(spreadsheet, rows_to_delete)

    # ── Step 3: Unlock Processed_Log ─────────────────────────────────────
    print("\n🔓 Step 3 — Unlocking post IDs in Processed_Log...")
    unlocked_rows = unlock_from_processed_log(spreadsheet, unique_post_ids)

    # ── Step 4: Apify re-scrape ───────────────────────────────────────────
    print("\n📡 Step 4 — Re-scraping via Apify...")
    fresh_posts = apify_rescrape(unique_post_urls, apify_token)

    if not fresh_posts:
        print("\n⚠ Apify returned no posts. Stopping before pipeline step.")
        print("  Rows have been deleted and backed up. Restore from Reprocess_Backup if needed.")
        sys.exit(1)

    # ── Step 5: Pipeline ─────────────────────────────────────────────────
    print(f"\n⚙️  Step 5 — Running pipeline on {len(fresh_posts)} fresh post(s)...")
    results, post_log = run_pipeline_on_fresh_data(fresh_posts)

    events_found = len(results)
    posts_with_events = sum(
        1 for info in post_log.values() if info.get("result") == "events_found"
    )

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(" REPROCESS COMPLETE — Summary")
    print("=" * 65)
    print(f"  • Weekend date range:            {weekend_dates[0]} → {weekend_dates[-1]}")
    print(f"  • Rows removed from All_Events:  {len(rows_to_delete)}")
    print(f"  • Unique post IDs unlocked:      {len(unique_post_ids)} ({unlocked_rows} log entries removed)")
    print(f"  • Unique posts re-scraped:       {len(unique_post_urls)}")
    print(f"  • Fresh Apify results:           {len(fresh_posts)} post(s)")
    print(f"  • New events extracted:          {events_found}")
    print(f"  • Posts that yielded events:     {posts_with_events}")
    print(f"  • Accounts affected:             {len(unique_accounts)}")
    print(f"  • Backup preserved in:           Reprocess_Backup tab")
    print("=" * 65)


if __name__ == "__main__":
    main()
