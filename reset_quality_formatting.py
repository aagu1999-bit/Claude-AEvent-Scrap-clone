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
- Dry-run by default. Reports row range that Phase 1 will wipe + cell
  count Phase 2 will yellow.
- Two-phase:
  - PHASE 1: universal background wipe of ALL data rows in the
    considered range. ONE bulk batch_format request. This clears drift
    from rows with blank QUALITY_FLAGS that have stray yellow cells
    (the failure mode of the pre-PR-D version).
  - PHASE 2: re-apply yellow ONLY to cells dictated by each row's
    current QUALITY_FLAGS + current FLAG_TO_COLUMNS mapping. Batched
    ~100 cells per API call.
"""

import os
import argparse
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from extraction_core import FLAG_TO_COLUMNS, FLAG_BG_COLOR

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
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
    # Accept either header form. 2026-06-04: main.py rewrites row 1 to use
    # SPACE-separated column names ('QUALITY FLAGS'), but sheets that
    # haven't been touched by the new main.py still have the underscore
    # form. Take whichever exists.
    if 'QUALITY FLAGS' in header:
        flags_col_idx = header.index('QUALITY FLAGS')
    elif 'QUALITY_FLAGS' in header:
        flags_col_idx = header.index('QUALITY_FLAGS')
    else:
        print(f"❌ QUALITY FLAGS / QUALITY_FLAGS column not in header — nothing to do", file=sys.stderr)
        sys.exit(1)

    last_col_letter = idx_to_col_letter(len(header) - 1)

    # Map header column names → A1 letters. Add both forms of the QUALITY
    # FLAGS key so downstream FLAG_TO_COLUMNS lookups (which may use either
    # 'QUALITY FLAGS' or 'QUALITY_FLAGS' depending on the flag's vintage)
    # all resolve to the right cell letter.
    col_letter_by_name = {name: idx_to_col_letter(i) for i, name in enumerate(header)}
    qf_letter = idx_to_col_letter(flags_col_idx)
    col_letter_by_name.setdefault('QUALITY FLAGS', qf_letter)
    col_letter_by_name.setdefault('QUALITY_FLAGS', qf_letter)

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

    # ─────────────────────────────────────────────────────────────
    # PR D — UNIVERSAL Phase 1 wipe.
    # The OLD Phase 1 cleared background only on rows with flag content.
    # That left "blank QUALITY_FLAGS but stray yellow cells" rows
    # completely untouched — the exact drift visible in the 2026-05
    # screenshots (NEWSLETTER, CITY, SECTION OF NJ rows yellowed under
    # an older FLAG_TO_COLUMNS, then orphaned when the mapping changed).
    #
    # New Phase 1 wipes ALL data rows in the considered range as a
    # single bulk batch_format request — one API call instead of N.
    # Phase 2 is unchanged: it paints yellow ONLY where the cell's
    # current QUALITY_FLAGS content + current FLAG_TO_COLUMNS dictate.
    # ─────────────────────────────────────────────────────────────
    phase1_start = max(args.after_row or 2, 2)   # row 1 is header
    phase1_end = len(all_data)
    if phase1_end < phase1_start:
        print(f"\n✓ No data rows in the considered range. Nothing to do.")
        return

    if not args.apply:
        print(f"\n  [dry-run] Would:")
        print(f"    Phase 1: clear formatting on rows {phase1_start}..{phase1_end} "
              f"({phase1_end - phase1_start + 1} rows)")
        n_cells = sum(len(c) for _, c, _ in targets)
        print(f"    Phase 2: apply yellow to {n_cells} cells across "
              f"{len(targets)} flagged rows")
        print(f"  Re-run with --apply to commit.")
        return

    # PHASE 1: universal wipe of the considered row range.
    # One bulk request — cheaper than N per-row requests AND covers
    # rows the old code skipped (blank QUALITY_FLAGS with stray yellow).
    print(f"\n━━━ PHASE 1: clearing formatting on rows {phase1_start}..{phase1_end} ━━━")
    wipe_range = f'A{phase1_start}:{last_col_letter}{phase1_end}'
    try:
        ws.batch_format([{
            'range': wipe_range,
            'format': {'backgroundColor': WHITE_BG},
        }])
        print(f"  ✓ Cleared {phase1_end - phase1_start + 1} rows in one call ({wipe_range})")
    except Exception as e:
        print(f"  ✗ Bulk clear failed: {str(e)[:120]}")
        sys.exit(1)

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
