#!/usr/bin/env python3
"""
reset_quality_formatting.py — re-apply All_Events cell highlighting based on
each row's CURRENT QUALITY_FLAGS content and the CURRENT FLAG_TO_COLUMNS mapping.

PROBLEM IT SOLVES
─────────────────
Formatting is applied at write time and never refreshed. Once a row is in
the sheet, its yellow-cell map is frozen. If you later:
  - Change FLAG_TO_COLUMNS in extraction_core (e.g., move NO_GROUNDING from
    QUALITY_FLAGS to DESCRIPTION, as we did in PR #16)
  - Edit a row's QUALITY_FLAGS cell manually
  - Delete rows and shift others up
...the existing rows' yellow cells stop matching their flag content. You see
"DATE is yellow but QUALITY_FLAGS only says NO_GROUNDING" — a real
discrepancy spotted on row 18910.

This tool fixes it by treating QUALITY_FLAGS cell content as the source of
truth and re-applying yellow cells per current FLAG_TO_COLUMNS.

WHAT IT DOES NOT DO
───────────────────
- Does NOT re-evaluate sanity checks. The QUALITY_FLAGS cell content is
  taken as-is. If you want to re-RUN sanity checks (e.g., recompute
  REGION_AUTOFIXED after lookup edits), use `audit_regions.py` or
  re-extract via events_from_ids.py.
- Does NOT touch rows where QUALITY_FLAGS is empty (no flags = no
  highlighting needed = nothing to do).

USAGE
─────
  # Preview what would change (default — no writes)
  python reset_quality_formatting.py

  # Apply
  python reset_quality_formatting.py --apply

  # Limit to recent rows (e.g., today's work only)
  python reset_quality_formatting.py --after-row 18400 --apply

SAFETY
──────
- Dry-run by default. Counts flagged rows and lists what colors would change.
- Two-phase per batch: (1) clear background on all 25 columns of each
  affected row, (2) apply yellow to specific cells per flags. Done as
  batch_format requests, ~100 rows per API call.
"""

import argparse
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from extraction_core import FLAG_TO_COLUMNS, FLAG_BG_COLOR

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"
BATCH_SIZE = 100
BATCH_DELAY_SEC = 1.5
WHITE_BG = {"red": 1.0, "green": 1.0, "blue": 1.0}


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def idx_to_col_letter(idx_0based):
    if idx_0based < 26:
        return chr(ord('A') + idx_0based)
    return chr(ord('A') + (idx_0based // 26) - 1) + chr(ord('A') + (idx_0based % 26))


def main():
    p = argparse.ArgumentParser(
        description="Re-apply per-cell highlighting on All_Events using current FLAG_TO_COLUMNS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--apply", action="store_true", help="Actually apply (default: dry-run)")
    p.add_argument("--after-row", type=int, default=None,
                   help="Only consider rows >= this row number")
    args = p.parse_args()

    print(f"━━━ reset_quality_formatting.py ━━━")
    print(f"  Apply mode:  {args.apply}")
    print(f"  After row:   {args.after_row or '(all)'}")
    print()

    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    header = ws.row_values(1)
    if 'QUALITY_FLAGS' not in header:
        print(f"❌ QUALITY_FLAGS column not in header — nothing to do", file=sys.stderr)
        sys.exit(1)

    flags_col_idx = header.index('QUALITY_FLAGS')
    last_col_letter = idx_to_col_letter(len(header) - 1)

    # Map header column names → A1 letters
    col_letter_by_name = {name: idx_to_col_letter(i) for i, name in enumerate(header)}

    print(f"  Reading All_Events rows...")
    all_data = ws.get_all_values()
    print(f"  {len(all_data)-1} data rows")

    # For each row, determine which cells SHOULD be yellow per current FLAG_TO_COLUMNS
    targets = []  # (row_num, cells_to_color_set, flag_list)
    flag_counter = Counter()
    for i, row in enumerate(all_data[1:], start=2):
        if args.after_row and i < args.after_row:
            continue
        if len(row) <= flags_col_idx:
            continue
        flags_str = (row[flags_col_idx] or '').strip()
        if not flags_str:
            continue

        cells = set()
        flags = [f.strip() for f in flags_str.split(',') if f.strip()]
        for f in flags:
            flag_counter[f] += 1
            cols = FLAG_TO_COLUMNS.get(f, [])
            cells.update(cols)
        if cells:
            targets.append((i, cells, flags))

    print(f"\n━━━ FINDINGS ━━━")
    print(f"  Rows with flag content: {len(targets)}")
    print(f"\n  Flag distribution:")
    for flag, n in flag_counter.most_common():
        print(f"    {flag:<28} {n:>5}")

    if not targets:
        print("\n✓ No flagged rows. Nothing to reformat.")
        return

    if not args.apply:
        print(f"\n  [dry-run] Would clear formatting + re-apply on {len(targets)} rows.")
        print(f"  Re-run with --apply to commit.")
        return

    # PHASE 1: clear background on every row that's about to be touched
    print(f"\n━━━ PHASE 1: clearing formatting on {len(targets)} rows ━━━")
    clear_requests = [
        {'range': f'A{row_num}:{last_col_letter}{row_num}',
         'format': {'backgroundColor': WHITE_BG}}
        for row_num, _, _ in targets
    ]
    total = (len(clear_requests) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(0, len(clear_requests), BATCH_SIZE):
        batch = clear_requests[batch_idx:batch_idx + BATCH_SIZE]
        try:
            ws.batch_format(batch)
            n = batch_idx // BATCH_SIZE + 1
            print(f"  ✓ clear batch {n}/{total} ({len(batch)} rows)")
        except Exception as e:
            print(f"  ✗ clear batch failed: {str(e)[:120]}")
            sys.exit(1)
        if batch_idx + BATCH_SIZE < len(clear_requests):
            time.sleep(BATCH_DELAY_SEC)

    # PHASE 2: re-apply yellow per FLAG_TO_COLUMNS
    print(f"\n━━━ PHASE 2: applying yellow to flagged cells ━━━")
    yellow_requests = []
    for row_num, cells, _ in targets:
        for col_label in cells:
            letter = col_letter_by_name.get(col_label)
            if not letter:
                continue
            yellow_requests.append({
                'range': f'{letter}{row_num}',
                'format': {'backgroundColor': FLAG_BG_COLOR},
            })
    print(f"  {len(yellow_requests)} cells to color")
    total = (len(yellow_requests) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(0, len(yellow_requests), BATCH_SIZE):
        batch = yellow_requests[batch_idx:batch_idx + BATCH_SIZE]
        try:
            ws.batch_format(batch)
            n = batch_idx // BATCH_SIZE + 1
            print(f"  ✓ apply batch {n}/{total} ({len(batch)} cells)")
        except Exception as e:
            print(f"  ✗ apply batch failed: {str(e)[:120]}")
            sys.exit(1)
        if batch_idx + BATCH_SIZE < len(yellow_requests):
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n✓ Done. All flagged rows now match current FLAG_TO_COLUMNS.")


if __name__ == "__main__":
    main()
