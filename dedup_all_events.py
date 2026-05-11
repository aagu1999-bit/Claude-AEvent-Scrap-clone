#!/usr/bin/env python3
"""
dedup_all_events.py — remove exact-duplicate rows from All_Events.

PURPOSE
───────
During today's smoke testing of events_from_ids.py, the tool was run
multiple times on the same posts before dedup was added. Result: some
post_ids appear with 2-3 identical event rows (same name, same date,
same venue). We want to drop the duplicates and keep one canonical row.

This tool also handles general drift cleanup — even without test
pollution, a sheet can accumulate duplicates over time from manual
re-runs or recovery operations.

DEDUP KEY
─────────
Composite key: (POST ID, EVENT NAME, DATE)
  - Same key = exact duplicate event from the same source post
  - Different key = legitimately different events (different times/details
    extracted, multiple events per post, etc.)

For each duplicate group: keeps the FIRST occurrence (lowest row number),
deletes the rest. This preserves the original extraction row and removes
later re-runs.

USAGE
─────
  # Preview what would be deleted (default — no writes)
  python dedup_all_events.py

  # Apply deletions to the sheet
  python dedup_all_events.py --apply

  # Limit to recent rows (e.g., today's smoke-test pollution only)
  python dedup_all_events.py --after-row 18400 --apply

  # Limit to a date range (PROCESSED TIMESTAMP column)
  python dedup_all_events.py --after-date 2026-05-09 --apply

SAFETY
──────
- Dry-run by default. Nothing changes unless --apply is passed.
- Writes a CSV audit trail to outputs/dedup_all_events_<ts>.csv listing
  every row that would be deleted (or was deleted with --apply).
- Uses batch row deletion in REVERSE order (highest row first) so row
  numbers don't shift mid-batch.
- Sheets API has a soft cap on batch_update size; we batch in groups
  of 500 row deletions per call.
"""

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

DELETE_BATCH_SIZE = 100   # rows per batch delete request
BATCH_DELAY_SEC = 1.5


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def main():
    p = argparse.ArgumentParser(
        description="Remove exact-duplicate rows from All_Events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--apply", action="store_true",
                   help="Actually delete duplicates (default: dry-run preview)")
    p.add_argument("--after-row", type=int, default=None,
                   help="Only consider rows >= this row number (e.g., --after-row 18400 "
                        "to skip everything before today's test pollution)")
    p.add_argument("--after-date", default=None,
                   help="Only consider rows with PROCESSED TIMESTAMP >= this date "
                        "(YYYY-MM-DD format)")
    args = p.parse_args()

    print(f"━━━ dedup_all_events.py ━━━")
    print(f"  Apply mode:        {args.apply}")
    print(f"  After row:         {args.after_row or '(all)'}")
    print(f"  After date:        {args.after_date or '(all)'}")
    print()

    print(f"  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)

    header = ws.row_values(1)
    try:
        post_id_idx = header.index("POST ID")
        name_idx = header.index("EVENT NAME")
        date_idx = header.index("DATE")
        ts_idx = header.index("PROCESSED TIMESTAMP") if "PROCESSED TIMESTAMP" in header else None
    except ValueError as e:
        print(f"❌ Header missing required column: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Reading All_Events rows...")
    all_data = ws.get_all_values()
    print(f"  {len(all_data)-1} data rows")

    # Build composite-key index. row_num is 2-indexed (row 1 = header).
    groups = defaultdict(list)  # (post_id, name, date) -> [row_num, ...]
    for i, row in enumerate(all_data[1:], start=2):
        if args.after_row and i < args.after_row:
            continue
        if args.after_date and ts_idx is not None:
            ts = (row[ts_idx] if len(row) > ts_idx else '').strip()
            # PROCESSED TIMESTAMP format is "YYYY-MM-DD HH:MM:SS"
            if ts and ts[:10] < args.after_date:
                continue
        if len(row) <= max(post_id_idx, name_idx, date_idx):
            continue
        post_id = (row[post_id_idx] or '').strip()
        name = (row[name_idx] or '').strip().upper()
        date = (row[date_idx] or '').strip()
        if not post_id:
            continue
        groups[(post_id, name, date)].append(i)

    # Find duplicates: any group with >1 row
    duplicates = []  # list of (row_num, post_id, name, date, kept_row_num)
    dup_count_by_key = []
    for key, rows in groups.items():
        if len(rows) <= 1:
            continue
        keep = rows[0]  # keep first occurrence
        for r in rows[1:]:
            duplicates.append((r, key[0], key[1], key[2], keep))
        dup_count_by_key.append((key, len(rows)))

    print(f"\n━━━ FINDINGS ━━━")
    print(f"  Duplicate groups (same post_id + name + date with >1 row): {len(dup_count_by_key)}")
    print(f"  Total rows that would be deleted: {len(duplicates)}")

    if dup_count_by_key:
        # Sort by occurrence count descending
        dup_count_by_key.sort(key=lambda x: -x[1])
        print(f"\n  Top 20 most-duplicated event keys:")
        print(f"  {'post_id':<22} {'event_name':<40} {'date':<12} {'count':<5}")
        for (post_id, name, date), count in dup_count_by_key[:20]:
            print(f"  {post_id:<22} {name[:39]:<40} {date:<12} {count}")

    if not duplicates:
        print("\n✓ No duplicates found.")
        return

    # Write audit CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"dedup_all_events_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['row_to_delete', 'post_id', 'event_name', 'date', 'row_kept'])
        for d in duplicates:
            w.writerow(d)
    print(f"\n  ✓ Audit CSV: {csv_path}")

    if not args.apply:
        print(f"\n  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    # Apply deletions in REVERSE order (highest row first) so earlier
    # row numbers stay valid as we delete.
    print(f"\n━━━ DELETING {len(duplicates)} ROWS ━━━")
    rows_to_delete = sorted([d[0] for d in duplicates], reverse=True)

    # Group consecutive runs of rows so we can delete them as a single
    # delete_rows call (more efficient than per-row deletes).
    # Build "runs" — consecutive descending row numbers form one delete range.
    # delete_rows takes (start_row, end_row) inclusive in 1-indexed.
    runs = []
    if rows_to_delete:
        run_end = rows_to_delete[0]
        run_start = run_end
        for r in rows_to_delete[1:]:
            if r == run_start - 1:
                run_start = r
            else:
                runs.append((run_start, run_end))
                run_end = r
                run_start = r
        runs.append((run_start, run_end))

    total = len(runs)
    for i, (start_row, end_row) in enumerate(runs, 1):
        try:
            ws.delete_rows(start_row, end_row)
            n_deleted = end_row - start_row + 1
            print(f"  ✓ run {i}/{total}: deleted rows {start_row}-{end_row}  ({n_deleted} rows)")
        except Exception as e:
            print(f"  ✗ delete failed for rows {start_row}-{end_row}: {str(e)[:120]}")
            print(f"  Stopping. Earlier deletes are persisted.")
            sys.exit(1)
        if i < total:
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n✓ Deleted {len(duplicates)} duplicate rows in {len(runs)} delete operation(s).")
    print(f"  Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
