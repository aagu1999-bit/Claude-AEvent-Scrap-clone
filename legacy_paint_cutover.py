#!/usr/bin/env python3
"""
legacy_paint_cutover.py — one-time migration to mark all current yellow
quality-flag highlights in All_Events as "legacy" (soft peach red).

WHY
───
Pre-cutover run logs lack the thread-safe per-line attribution that
worker_log.py adds (see feat/worker-log-prefixing). Threaded output
interleaves in old logs, so existing yellow highlights can't be reliably
verified retrospectively — repair_flags_from_log.py can do ~85-95% on old
logs but the rest is unrecoverable without expensive re-extraction.

This script makes that uncertainty visible: every cell currently painted
with FLAG_BG_COLOR (light yellow) becomes a soft peach red. After cutover:
  - RED   = legacy paint, applied under pre-cutover code/logs. May or may
            not still be correct. Treat as "look here, but verify."
  - YELLOW = paint applied by main.py / events_from_ids.py post-cutover.
             Thread-safe logging means it's verifiable. Trust it.

The QUALITY_FLAGS column text is NOT touched — only background paint.
The existing flag text stays exactly as-is on every row.

DOES NOT
────────
- Touch QUALITY_FLAGS column text
- Add, remove, or modify any flag
- Touch non-yellow cells (other backgrounds preserved)
- Re-extract anything

CRITICAL FOLLOW-UP
──────────────────
After this runs, DO NOT run `reset_quality_formatting.py` on the full
sheet — it will wipe all backgrounds (Phase 1) and repaint yellow per
FLAG_TO_COLUMNS, undoing this cutover for any row whose QUALITY_FLAGS
column still has a matching flag. Scope reset to post-cutover rows only:
  python reset_quality_formatting.py --after-row <CUTOVER_ROW> --apply

USAGE
─────
  # Dry-run — scan and report what would change
  python legacy_paint_cutover.py

  # Apply
  python legacy_paint_cutover.py --apply

  # Limit scope (e.g., only rows from a known buggy window)
  python legacy_paint_cutover.py --start-row 5000 --end-row 18000 --apply

OUTPUT
──────
- Console: cell count, breakdown by column
- CSV audit: outputs/legacy_paint_cutover_<ts>.csv with every cell address
"""

import os
import argparse
import csv
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

# Current "flagged" yellow (mirrors extraction_core.FLAG_BG_COLOR).
YELLOW = {"red": 1.0, "green": 0.97, "blue": 0.78}

# New "legacy" soft peach — visible but calm; signals "verify, don't trust."
LEGACY_RED = {"red": 1.0, "green": 0.88, "blue": 0.88}

COLOR_TOLERANCE = 0.02         # Sheets API can round-trip with tiny float drift
CHUNK_ROWS = 5000              # rows per fetch_sheet_metadata call
BATCH_FORMAT_SIZE = 500        # cells per batch_format request
BATCH_DELAY_SEC = 1.0


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def color_match(c, target, tol=COLOR_TOLERANCE):
    """Compare two color dicts with float tolerance. Missing channels
    default to 1.0 (white), since Sheets omits the default value."""
    if c is None:
        return False
    return (
        abs(c.get('red',   1.0) - target['red'])   < tol
        and abs(c.get('green', 1.0) - target['green']) < tol
        and abs(c.get('blue',  1.0) - target['blue'])  < tol
    )


def col_letter(idx):
    """0-indexed column number → A1 letter(s)."""
    if idx < 26:
        return chr(ord('A') + idx)
    return chr(ord('A') + (idx // 26) - 1) + chr(ord('A') + (idx % 26))


def fetch_yellow_cells(spreadsheet, start_row, end_row, num_cols):
    """Fetch backgroundColor metadata for a chunk; return [A1 addresses]
    of cells whose background matches YELLOW."""
    last_letter = col_letter(num_cols - 1)
    range_str = f"{ALL_EVENTS_TAB}!A{start_row}:{last_letter}{end_row}"

    metadata = spreadsheet.fetch_sheet_metadata({
        'ranges': [range_str],
        'fields': 'sheets.data.rowData.values.userEnteredFormat.backgroundColor',
        'includeGridData': True,
    })

    yellow_addrs = []
    for sheet in metadata.get('sheets', []):
        for block in sheet.get('data', []):
            row_data = block.get('rowData', [])
            for row_offset, row in enumerate(row_data):
                actual_row = start_row + row_offset
                for col_offset, cell in enumerate(row.get('values', [])):
                    bg = cell.get('userEnteredFormat', {}).get('backgroundColor')
                    if color_match(bg, YELLOW):
                        yellow_addrs.append(f"{col_letter(col_offset)}{actual_row}")
    return yellow_addrs


def main():
    p = argparse.ArgumentParser(
        description="Recolor legacy yellow flag highlights in All_Events to soft peach red.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--apply", action="store_true",
                   help="Commit the recoloring (default: dry-run preview).")
    p.add_argument("--start-row", type=int, default=2,
                   help="First row to scan (default: 2, skips header).")
    p.add_argument("--end-row", type=int, default=None,
                   help="Last row to scan (default: total row count).")
    args = p.parse_args()

    print("━━━ legacy_paint_cutover.py ━━━")
    print(f"  Apply mode: {args.apply}  (dry-run is default)")

    print("\n  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)

    total_rows = ws.row_count
    end_row = args.end_row or total_rows
    num_cols = ws.col_count

    print(f"  Scanning {ALL_EVENTS_TAB} rows {args.start_row}–{end_row} × {num_cols} cols")
    print(f"  Target yellow: rgb({YELLOW['red']}, {YELLOW['green']}, {YELLOW['blue']})")
    print(f"  Will recolor to: rgb({LEGACY_RED['red']}, {LEGACY_RED['green']}, {LEGACY_RED['blue']})")
    print()

    all_yellow = []
    current = args.start_row
    while current <= end_row:
        chunk_end = min(current + CHUNK_ROWS - 1, end_row)
        print(f"    Scanning rows {current}–{chunk_end}...", end='', flush=True)
        chunk_yellow = fetch_yellow_cells(sh, current, chunk_end, num_cols)
        all_yellow.extend(chunk_yellow)
        print(f" found {len(chunk_yellow)}")
        current = chunk_end + 1
        time.sleep(0.5)  # be gentle on quota between chunks

    print(f"\n  Total yellow cells found: {len(all_yellow)}")

    if not all_yellow:
        print("\n✓ Nothing to recolor.")
        return

    # Breakdown by column
    col_counts = Counter(addr.rstrip('0123456789') for addr in all_yellow)
    header = ws.row_values(1)
    print("\n  Breakdown by column:")
    for letter, count in sorted(col_counts.items()):
        # convert letter back to index for header lookup
        idx = ord(letter) - ord('A') if len(letter) == 1 else \
              (ord(letter[0]) - ord('A') + 1) * 26 + (ord(letter[1]) - ord('A'))
        col_name = header[idx] if idx < len(header) else '?'
        print(f"    {letter} ({col_name}): {count}")

    # Audit CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"legacy_paint_cutover_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['cell_address', 'column_letter', 'row'])
        for addr in all_yellow:
            letter = addr.rstrip('0123456789')
            row_num = addr[len(letter):]
            w.writerow([addr, letter, row_num])
    print(f"\n  ✓ Audit CSV: {csv_path}")

    if not args.apply:
        print(f"\n  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    # Apply the recoloring in batches
    print(f"\n━━━ Recoloring {len(all_yellow)} cells → soft peach red ━━━")
    format_requests = [
        {'range': addr, 'format': {'backgroundColor': LEGACY_RED}}
        for addr in all_yellow
    ]

    for start in range(0, len(format_requests), BATCH_FORMAT_SIZE):
        batch = format_requests[start:start + BATCH_FORMAT_SIZE]
        ws.batch_format(batch)
        done = min(start + BATCH_FORMAT_SIZE, len(format_requests))
        print(f"  {done}/{len(format_requests)} cells recolored")
        if start + BATCH_FORMAT_SIZE < len(format_requests):
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n✓ Done. {len(all_yellow)} cells recolored to legacy peach.")
    print(f"  Audit trail: {csv_path}")
    print()
    print("⚠ REMINDER: Do NOT run reset_quality_formatting.py on the full sheet.")
    print("  It will wipe these reds and repaint yellow per current column text.")
    print("  Scope reset to post-cutover rows only:")
    print("    python reset_quality_formatting.py --after-row <CUTOVER_ROW> --apply")


if __name__ == "__main__":
    main()
